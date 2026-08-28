package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	privaterunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/google/uuid"
)

type runForkActivationLineage struct {
	ForkRunID         string
	ForkStatus        string
	ForkBundleHash    string
	SourceRunID       string
	SourceBundleHash  string
	ForkEventID       string
	ForkEventName     string
	ForkEventTime     time.Time
	ForkEventRevision int64
	SourceRunStatus   string
	EntityIDs         []string
	FlowInstances     []string
	SourceFlows       []string
}

type RunForkActivationLineage = runForkActivationLineage

func (s *RunForkPostgresOwner) ActivateRunFork(ctx context.Context, req runfork.RunForkActivateRequest) (runfork.RunForkActivation, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkActivation{}, fmt.Errorf("postgres store is required")
	}
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return runfork.RunForkActivation{}, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkActivation{}, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.RunForkActivation{}, err
	}
	handoff, err := reserveRunLifecycleCandidateHandoff(ctx)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	defer handoff.Rollback()

	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return runfork.RunForkActivation{}, fmt.Errorf("begin fork activation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	lineage, err := loadRunForkActivationLineage(ctx, s.RunLifecyclePostgresOwner, tx, forkRunID)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	if err := lockRunForkSourceRevisionFrontier(ctx, tx, &lineage); err != nil {
		return runfork.RunForkActivation{}, err
	}
	result := runfork.RunForkActivation{
		SourceRunID:             lineage.SourceRunID,
		ForkRunID:               lineage.ForkRunID,
		ForkRunStatus:           lineage.ForkStatus,
		SourceRunStatus:         lineage.SourceRunStatus,
		ForkPoint:               runfork.RunForkPoint{Input: lineage.ForkEventID, EventID: lineage.ForkEventID, EventName: lineage.ForkEventName, Timestamp: lineage.ForkEventTime, Revision: lineage.ForkEventRevision},
		ReplayResumeBlocked:     true,
		MaterializedEntityCount: len(lineage.EntityIDs),
	}
	if lineage.ForkStatus != runfork.RunForkMaterializedStatus {
		result.RepeatedActivationFailed = lineage.ForkStatus == runfork.RunForkActivatedStatus
		return result, fmt.Errorf("fork activation requires materialized fork status %q; got %q", runfork.RunForkMaterializedStatus, lineage.ForkStatus)
	}
	sourceState, sourceStateErr := runtimerunlifecycle.ParseState(lineage.SourceRunStatus)
	if sourceStateErr != nil || !sourceState.Active() {
		return result, fmt.Errorf("fork activation requires source run status running or paused before freeze; got %q", lineage.SourceRunStatus)
	}
	if len(lineage.EntityIDs) == 0 {
		return result, fmt.Errorf("fork activation requires materialized fork entity_state rows")
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, lineage.ForkRunID)
	if err == nil {
		result.SelectedContractBinding = &binding
	} else if err != sql.ErrNoRows {
		return result, fmt.Errorf("load selected contract binding: %w", err)
	}

	plan, err := s.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: lineage.SourceRunID, At: lineage.ForkEventID})
	if err != nil {
		return result, err
	}
	result.ReplayResumeAdmission = plan.ReplayResumeAdmission
	if !plan.ExecutionReady {
		result.UnsupportedBlockers = plan.UnsupportedBlockers
		return result, fmt.Errorf("fork activation requires execution-ready materialized fork; blockers: %s", runForkBlockerCodes(plan.UnsupportedBlockers))
	}
	if err := ensureRunForkSourceNotAdvanced(ctx, tx, lineage); err != nil {
		result.SourceAdvancedAfterFork = true
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
	}
	if err := ensureRunForkActivationNoForkReplayState(ctx, tx, postgresDeliveryAdapter, lineage.ForkRunID); err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
	}

	historicalReplayExecution, err := requireRunForkHistoricalReplayExecution(ctx, req.HistoricalReplayExecutionAdmitter, lineage, plan)
	if err != nil {
		return result, err
	}
	if historicalReplayExecution.DeliveryEventReplayReady {
		if err := validateRunForkDeliveryEventReplayWorkAgainstPlan(plan.PendingWork, historicalReplayExecution.DeliveryEventReplayWork); err != nil {
			return result, err
		}
		if err := runfork.ValidateFanOutPendingReplayExecution(plan, historicalReplayExecution.DeliveryEventReplayWork); err != nil {
			return result, err
		}
	}

	now := time.Now().UTC()
	effects := privaterunforkrevision.NewEffects()
	replayResult := runfork.RunForkDeliveryEventReplayResult{
		Owner:       runfork.RunForkDeliveryEventReplayOwner,
		SourceRunID: lineage.SourceRunID,
		ForkRunID:   lineage.ForkRunID,
	}
	if historicalReplayExecution.DeliveryEventReplayReady {
		replayResult, err = applyRunForkDeliveryEventReplay(ctx, tx, story, effects, s.deliveryEventReplayAdapter(), lineage, historicalReplayExecution, now)
		if err != nil {
			return result, err
		}
	}
	if err := bindRunForkFanOutPendingReplays(ctx, tx, effects, lineage.ForkRunID, plan, now); err != nil {
		return result, err
	}
	if err := s.applyRunForkSourceFreeze(ctx, tx, story, effects, lineage, now, req.ConfirmSourceFreeze, handoff); err != nil {
		return result, err
	}
	if err := effects.Add(lineage.ForkRunID,
		privaterunforkrevision.FamilyEvents,
		privaterunforkrevision.FamilyEventDeliveries,
		privaterunforkrevision.FamilyCommittedReplayScopes,
		privaterunforkrevision.FamilyEventReceipts,
		privaterunforkrevision.FamilyReplyContexts,
	); err != nil {
		return result, err
	}
	if err := commitRunForkAuthorActivityTransaction(ctx, tx, story, effects); err != nil {
		return result, fmt.Errorf("commit fork activation: %w", err)
	}
	committed = true
	if err := handoff.Commit(); err != nil {
		return result, err
	}
	result.ForkRunStatus = runfork.RunForkActivatedStatus
	result.SourceRunStatus = runfork.RunForkSourceFrozenStatus
	result.Activated = true
	result.SourceFrozen = true
	if replayResult.ReplayedEventCount > 0 || replayResult.ReplayedDeliveryCount > 0 {
		historicalReplayExecution.DeliveryEventReplay = &replayResult
		result.HistoricalReplayExecution = &historicalReplayExecution
		result.DeliveryEventReplay = &replayResult
	}
	return result, nil
}

func (s *RunForkSQLiteOwner) ActivateRunFork(ctx context.Context, req runfork.RunForkActivateRequest) (result runfork.RunForkActivation, err error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkActivation{}, fmt.Errorf("sqlite store is required")
	}
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return runfork.RunForkActivation{}, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkActivation{}, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.RunForkActivation{}, err
	}
	handoff, err := reserveRunLifecycleCandidateHandoff(ctx)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	defer handoff.Rollback()
	var historicalReplayExecution runfork.RunForkHistoricalReplayExecution
	var replayResult runfork.RunForkDeliveryEventReplayResult
	err = s.runRuntimeMutation(ctx, "sqlite run fork activation", func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		lineage, err := loadSQLiteRunForkActivationLineage(txctx, s.RunLifecycleSQLiteOwner, tx, forkRunID)
		if err != nil {
			return err
		}
		if err := lockSQLiteRunForkSourceRevisionFrontier(txctx, tx, &lineage); err != nil {
			return err
		}
		result = runfork.RunForkActivation{
			SourceRunID: lineage.SourceRunID, ForkRunID: lineage.ForkRunID, ForkRunStatus: lineage.ForkStatus,
			SourceRunStatus:     lineage.SourceRunStatus,
			ForkPoint:           runfork.RunForkPoint{Input: lineage.ForkEventID, EventID: lineage.ForkEventID, EventName: lineage.ForkEventName, Timestamp: lineage.ForkEventTime, Revision: lineage.ForkEventRevision},
			ReplayResumeBlocked: true, MaterializedEntityCount: len(lineage.EntityIDs),
		}
		if lineage.ForkStatus != runfork.RunForkMaterializedStatus {
			result.RepeatedActivationFailed = lineage.ForkStatus == runfork.RunForkActivatedStatus
			return fmt.Errorf("fork activation requires materialized fork status %q; got %q", runfork.RunForkMaterializedStatus, lineage.ForkStatus)
		}
		sourceState, sourceStateErr := runtimerunlifecycle.ParseState(lineage.SourceRunStatus)
		if sourceStateErr != nil || !sourceState.Active() {
			return fmt.Errorf("fork activation requires source run status running or paused before freeze; got %q", lineage.SourceRunStatus)
		}
		if len(lineage.EntityIDs) == 0 {
			return fmt.Errorf("fork activation requires materialized fork entity_state rows")
		}
		binding, err := loadRunForkSelectedContractBinding(txctx, tx, lineage.ForkRunID)
		if err == nil {
			result.SelectedContractBinding = &binding
		} else if err != sql.ErrNoRows {
			return fmt.Errorf("load selected contract binding: %w", err)
		}
		plan, err := planRunForkSnapshot(txctx, tx, runfork.RunForkPlanRequest{SourceRunID: lineage.SourceRunID, At: lineage.ForkEventID}, privaterunforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint)
		if err != nil {
			return err
		}
		result.ReplayResumeAdmission = plan.ReplayResumeAdmission
		if !plan.ExecutionReady {
			result.UnsupportedBlockers = plan.UnsupportedBlockers
			return fmt.Errorf("fork activation requires execution-ready materialized fork; blockers: %s", runForkBlockerCodes(plan.UnsupportedBlockers))
		}
		if err := ensureRunForkSourceNotAdvanced(txctx, tx, lineage); err != nil {
			result.SourceAdvancedAfterFork = true
			if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
				result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
				result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
			}
			return err
		}
		if err := ensureRunForkActivationNoForkReplayState(txctx, tx, sqliteDeliveryAdapter, lineage.ForkRunID); err != nil {
			if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
				result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
				result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
			}
			return err
		}
		historicalReplayExecution, err = requireRunForkHistoricalReplayExecution(txctx, req.HistoricalReplayExecutionAdmitter, lineage, plan)
		if err != nil {
			return err
		}
		if historicalReplayExecution.DeliveryEventReplayReady {
			if err := validateRunForkDeliveryEventReplayWorkAgainstPlan(plan.PendingWork, historicalReplayExecution.DeliveryEventReplayWork); err != nil {
				return err
			}
			if err := runfork.ValidateFanOutPendingReplayExecution(plan, historicalReplayExecution.DeliveryEventReplayWork); err != nil {
				return err
			}
		}
		now := s.now()
		effects := privaterunforkrevision.NewEffects()
		replayResult = runfork.RunForkDeliveryEventReplayResult{Owner: runfork.RunForkDeliveryEventReplayOwner, SourceRunID: lineage.SourceRunID, ForkRunID: lineage.ForkRunID}
		if historicalReplayExecution.DeliveryEventReplayReady {
			replayResult, err = applyRunForkDeliveryEventReplay(txctx, tx, story, effects, s.deliveryEventReplayAdapter(), lineage, historicalReplayExecution, now)
			if err != nil {
				return err
			}
		}
		if err := bindRunForkFanOutPendingReplays(txctx, tx, effects, lineage.ForkRunID, plan, now); err != nil {
			return err
		}
		if err := s.applyRunForkSourceFreeze(txctx, tx, story, effects, lineage, now, req.ConfirmSourceFreeze, handoff); err != nil {
			return err
		}
		if err := effects.Add(lineage.ForkRunID,
			privaterunforkrevision.FamilyEvents, privaterunforkrevision.FamilyEventDeliveries,
			privaterunforkrevision.FamilyCommittedReplayScopes, privaterunforkrevision.FamilyEventReceipts,
			privaterunforkrevision.FamilyReplyContexts,
		); err != nil {
			return err
		}
		if err := story.Finalize(txctx); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.FinalizeSQLite(txctx, tx, effects); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if err := handoff.Commit(); err != nil {
		return result, err
	}
	result.ForkRunStatus = runfork.RunForkActivatedStatus
	result.SourceRunStatus = runfork.RunForkSourceFrozenStatus
	result.Activated = true
	result.SourceFrozen = true
	if replayResult.ReplayedEventCount > 0 || replayResult.ReplayedDeliveryCount > 0 {
		historicalReplayExecution.DeliveryEventReplay = &replayResult
		result.HistoricalReplayExecution = &historicalReplayExecution
		result.DeliveryEventReplay = &replayResult
	}
	return result, nil
}

func recordRunForkActivationAuthorActivity(ctx context.Context, story runtimeauthoractivity.Mutation, lineage runForkActivationLineage, now time.Time) error {
	occurrenceScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, lineage.ForkBundleHash)
	if err != nil {
		return fmt.Errorf("record run fork activation target scope: %w", err)
	}
	identity := lineage.ForkRunID + ":fork_started"
	return story.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: "fork_started",
		SourceOwner: "runs", SourceIdentity: identity, DedupKey: "run-transition:" + identity,
		OccurredAt: now.UTC(), RunID: lineage.ForkRunID, Scope: occurrenceScope,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "run", SubjectID: lineage.ForkRunID, ParentRunID: lineage.SourceRunID,
			TriggerEventType: lineage.ForkEventName,
		},
	})
}

func commitRunForkAuthorActivityTransaction(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *privaterunforkrevision.Effects) error {
	if err := story.Finalize(ctx); err != nil {
		return err
	}
	if _, err := privaterunforkrevision.FinalizePostgres(ctx, tx, effects); err != nil {
		return err
	}
	return tx.Commit()
}

func CommitRunForkAuthorActivityTransaction(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *privaterunforkrevision.Effects) error {
	return commitRunForkAuthorActivityTransaction(ctx, tx, story, effects)
}

func requireRunForkHistoricalReplayExecution(
	ctx context.Context,
	admitter runfork.RunForkHistoricalReplayExecutionAdmitter,
	lineage runForkActivationLineage,
	plan runfork.RunForkPlan,
) (runfork.RunForkHistoricalReplayExecution, error) {
	if !plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		return runfork.RunForkHistoricalReplayExecution{}, nil
	}
	if admitter == nil {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("%s admission required before delivery_event_replay_ready mutation", runfork.RunForkHistoricalReplayExecutionOwner)
	}
	execution, err := admitter.AdmitRunForkHistoricalReplayExecution(ctx, runfork.RunForkHistoricalReplayExecutionRequest{
		ForkRunID:             lineage.ForkRunID,
		SourceRunID:           lineage.SourceRunID,
		ForkEventID:           lineage.ForkEventID,
		ReplayResumeAdmission: plan.ReplayResumeAdmission,
		PendingWork:           plan.PendingWork,
	})
	if err != nil {
		return runfork.RunForkHistoricalReplayExecution{}, err
	}
	if strings.TrimSpace(execution.Owner) != runfork.RunForkHistoricalReplayExecutionOwner {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation requires %s; got %q", runfork.RunForkHistoricalReplayExecutionOwner, execution.Owner)
	}
	if strings.TrimSpace(execution.AdmissionOwner) != runfork.RunForkHistoricalReplayExecutionAdmissionOwner {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation requires %s; got %q", runfork.RunForkHistoricalReplayExecutionAdmissionOwner, execution.AdmissionOwner)
	}
	if strings.TrimSpace(execution.ForkRunID) != lineage.ForkRunID ||
		strings.TrimSpace(execution.SourceRunID) != lineage.SourceRunID ||
		strings.TrimSpace(execution.ForkEventID) != lineage.ForkEventID {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation historical replay execution identity mismatch")
	}
	if !execution.DeliveryEventReplayReady ||
		execution.EventDeliveriesAdmission.Fact != runfork.RunForkHistoricalReplayFactEventDeliveries ||
		execution.EventDeliveriesAdmission.Admission != runfork.RunForkHistoricalReplayAdmissionExecutableForkWork {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation requires event_deliveries executable fork work admission")
	}
	if len(execution.DeliveryEventReplayWork) == 0 {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation requires owner-authorized delivery_event_replay_ready work")
	}
	return execution, nil
}

func loadRunForkActivationLineage(ctx context.Context, lifecycle *privaterunlifecycle.RunLifecyclePostgresOwner, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
	snapshot, err := lifecycle.LoadSnapshotTx(ctx, tx, forkRunID, true)
	if err != nil {
		if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
			return runForkActivationLineage{}, fmt.Errorf("fork run %s not found", forkRunID)
		}
		return runForkActivationLineage{}, fmt.Errorf("load fork activation lifecycle: %w", err)
	}
	if snapshot.Origin.Kind() != runtimerunlifecycle.OriginForkMaterialization {
		return runForkActivationLineage{}, fmt.Errorf(
			"fork activation requires fork materialization origin; run %s has %s",
			forkRunID,
			snapshot.Origin.Kind(),
		)
	}
	lineage := runForkActivationLineage{
		ForkRunID:      snapshot.RunID,
		ForkStatus:     string(snapshot.State),
		ForkBundleHash: snapshot.BundleHash,
		SourceRunID:    snapshot.Origin.SourceRunID(),
		ForkEventID:    snapshot.Origin.SourceEventID(),
	}
	var forkEventTime sql.NullTime
	err = tx.QueryRowContext(ctx, `
		SELECT
			COALESCE(s.status, ''),
			COALESCE(s.bundle_hash, ''),
			COALESCE(e.event_name, ''),
			e.created_at
		FROM runs s
		LEFT JOIN events e ON e.run_id = s.run_id AND e.event_id = $2::uuid
		WHERE s.run_id = $1::uuid
		FOR UPDATE OF s
	`, lineage.SourceRunID, lineage.ForkEventID).Scan(
		&lineage.SourceRunStatus,
		&lineage.SourceBundleHash,
		&lineage.ForkEventName,
		&forkEventTime,
	)
	if err == sql.ErrNoRows {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run")
	}
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load fork activation lineage: %w", err)
	}
	if lineage.SourceRunStatus == "" || !forkEventTime.Valid {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run and fork point event")
	}
	lineage.ForkEventTime = forkEventTime.Time
	rows, err := tx.QueryContext(ctx, `
		SELECT entity_id::text, COALESCE(flow_instance, '')
		FROM entity_state
		WHERE run_id = $1::uuid
		ORDER BY entity_id
	`, lineage.ForkRunID)
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load fork materialized state facts: %w", err)
	}
	defer rows.Close()
	flowSet := map[string]struct{}{}
	sourceFlowSet := map[string]struct{}{}
	for rows.Next() {
		var entityID, flowInstance string
		if err := rows.Scan(&entityID, &flowInstance); err != nil {
			return runForkActivationLineage{}, fmt.Errorf("scan fork materialized state facts: %w", err)
		}
		if entityID = strings.TrimSpace(entityID); entityID != "" {
			lineage.EntityIDs = append(lineage.EntityIDs, entityID)
		}
		if flowInstance = strings.TrimSpace(flowInstance); flowInstance != "" {
			if _, ok := flowSet[flowInstance]; !ok {
				flowSet[flowInstance] = struct{}{}
				lineage.FlowInstances = append(lineage.FlowInstances, flowInstance)
			}
			if sourceFlow := runtimeflowidentity.SemanticScope(flowInstance); sourceFlow != "" {
				sourceFlowSet[sourceFlow] = struct{}{}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return runForkActivationLineage{}, fmt.Errorf("read fork materialized state facts: %w", err)
	}
	lineage.SourceFlows = stringSetValues(sourceFlowSet)
	lineage.ForkBundleHash = strings.TrimSpace(lineage.ForkBundleHash)
	lineage.SourceBundleHash = strings.TrimSpace(lineage.SourceBundleHash)
	return lineage, nil
}

func loadSQLiteRunForkActivationLineage(ctx context.Context, lifecycle *privaterunlifecycle.RunLifecycleSQLiteOwner, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
	snapshot, err := lifecycle.LoadSnapshotTx(ctx, tx, forkRunID)
	if err != nil {
		if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
			return runForkActivationLineage{}, fmt.Errorf("fork run %s not found", forkRunID)
		}
		return runForkActivationLineage{}, fmt.Errorf("load sqlite fork activation lifecycle: %w", err)
	}
	if snapshot.Origin.Kind() != runtimerunlifecycle.OriginForkMaterialization {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires fork materialization origin; run %s has %s", forkRunID, snapshot.Origin.Kind())
	}
	lineage := runForkActivationLineage{
		ForkRunID: snapshot.RunID, ForkStatus: string(snapshot.State), ForkBundleHash: snapshot.BundleHash,
		SourceRunID: snapshot.Origin.SourceRunID(), ForkEventID: snapshot.Origin.SourceEventID(),
	}
	var forkEventTimeRaw any
	err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(s.status, ''), COALESCE(s.bundle_hash, ''), COALESCE(e.event_name, ''), e.created_at
		FROM runs s
		LEFT JOIN events e ON e.run_id = s.run_id AND e.event_id = $2
		WHERE s.run_id = $1
	`, lineage.SourceRunID, lineage.ForkEventID).Scan(
		&lineage.SourceRunStatus, &lineage.SourceBundleHash, &lineage.ForkEventName, &forkEventTimeRaw,
	)
	if err == sql.ErrNoRows {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run")
	}
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load sqlite fork activation lineage: %w", err)
	}
	forkEventTime, present, err := sqliteTimeValue(forkEventTimeRaw)
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("decode sqlite fork activation event time: %w", err)
	}
	if lineage.SourceRunStatus == "" || !present {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run and fork point event")
	}
	lineage.ForkEventTime = forkEventTime
	rows, err := tx.QueryContext(ctx, `
		SELECT CAST(entity_id AS TEXT), COALESCE(flow_instance, '')
		FROM entity_state
		WHERE run_id = $1
		ORDER BY entity_id
	`, lineage.ForkRunID)
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load sqlite fork materialized state facts: %w", err)
	}
	defer rows.Close()
	flowSet := map[string]struct{}{}
	sourceFlowSet := map[string]struct{}{}
	for rows.Next() {
		var entityID, flowInstance string
		if err := rows.Scan(&entityID, &flowInstance); err != nil {
			return runForkActivationLineage{}, fmt.Errorf("scan sqlite fork materialized state facts: %w", err)
		}
		if entityID = strings.TrimSpace(entityID); entityID != "" {
			lineage.EntityIDs = append(lineage.EntityIDs, entityID)
		}
		if flowInstance = strings.TrimSpace(flowInstance); flowInstance != "" {
			if _, ok := flowSet[flowInstance]; !ok {
				flowSet[flowInstance] = struct{}{}
				lineage.FlowInstances = append(lineage.FlowInstances, flowInstance)
			}
			if sourceFlow := runtimeflowidentity.SemanticScope(flowInstance); sourceFlow != "" {
				sourceFlowSet[sourceFlow] = struct{}{}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return runForkActivationLineage{}, fmt.Errorf("read sqlite fork materialized state facts: %w", err)
	}
	lineage.SourceFlows = stringSetValues(sourceFlowSet)
	lineage.ForkBundleHash = strings.TrimSpace(lineage.ForkBundleHash)
	lineage.SourceBundleHash = strings.TrimSpace(lineage.SourceBundleHash)
	return lineage, nil
}

func lockRunForkSourceRevisionFrontier(ctx context.Context, tx *sql.Tx, lineage *runForkActivationLineage) error {
	if lineage == nil {
		return fmt.Errorf("fork activation requires lineage")
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT MIN(revision)
		FROM run_fork_fact_revisions
		WHERE run_id = $1::uuid
		  AND family = 'events'
		  AND fact_key = $2
		  AND present
	`, lineage.SourceRunID, lineage.ForkEventID).Scan(&lineage.ForkEventRevision); err != nil {
		return fmt.Errorf("resolve fork activation event revision: %w", err)
	}
	if lineage.ForkEventRevision <= 0 {
		return fmt.Errorf("fork activation source event is not revisioned; recreate the store and retry")
	}
	var currentRevision int64
	if err := tx.QueryRowContext(ctx, `
		SELECT last_revision
		FROM run_fork_revision_heads
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, lineage.SourceRunID).Scan(&currentRevision); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("fork activation source revision frontier is missing; recreate the store and retry")
		}
		return fmt.Errorf("lock fork activation source revision frontier: %w", err)
	}
	if currentRevision < lineage.ForkEventRevision {
		return fmt.Errorf("fork activation source revision frontier is corrupt; recreate the store and retry")
	}
	return nil
}

func lockSQLiteRunForkSourceRevisionFrontier(ctx context.Context, tx *sql.Tx, lineage *runForkActivationLineage) error {
	if lineage == nil {
		return fmt.Errorf("fork activation requires lineage")
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT MIN(revision)
		FROM run_fork_fact_revisions
		WHERE run_id = $1 AND family = 'events' AND fact_key = $2 AND present
	`, lineage.SourceRunID, lineage.ForkEventID).Scan(&lineage.ForkEventRevision); err != nil {
		return fmt.Errorf("resolve sqlite fork activation event revision: %w", err)
	}
	if lineage.ForkEventRevision <= 0 {
		return fmt.Errorf("fork activation source event is not revisioned; recreate the store and retry")
	}
	var currentRevision int64
	if err := tx.QueryRowContext(ctx, `
		SELECT last_revision FROM run_fork_revision_heads WHERE run_id = $1
	`, lineage.SourceRunID).Scan(&currentRevision); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("fork activation source revision frontier is missing; recreate the store and retry")
		}
		return fmt.Errorf("load sqlite fork activation source revision frontier: %w", err)
	}
	if currentRevision < lineage.ForkEventRevision {
		return fmt.Errorf("fork activation source revision frontier is corrupt; recreate the store and retry")
	}
	return nil
}

func collectRunForkSourceAdvancedFacts(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT family
		FROM run_fork_fact_revisions
		WHERE run_id = $1
		  AND revision > $2
		ORDER BY family
	`, lineage.SourceRunID, lineage.ForkEventRevision)
	if err != nil {
		return nil, fmt.Errorf("read source revisions after fork point: %w", err)
	}
	defer rows.Close()
	facts := []string{}
	for rows.Next() {
		var family string
		if err := rows.Scan(&family); err != nil {
			return nil, fmt.Errorf("scan source revision after fork point: %w", err)
		}
		code, ok := runForkSourceAdvancedCode(family)
		if !ok {
			return nil, fmt.Errorf("unsupported revisioned source family %q; recreate the store and retry", family)
		}
		facts = append(facts, code)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source revisions after fork point: %w", err)
	}
	return uniqueNonEmptyStrings(facts), nil
}

func ensureRunForkSourceNotAdvanced(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage) error {
	facts, err := collectRunForkSourceAdvancedFacts(ctx, tx, lineage)
	if err != nil {
		return err
	}
	if len(facts) == 0 {
		return nil
	}
	return runForkReplayResumeError(facts[0], runfork.RunForkReplayResumeFactSourceAdvanced, fmt.Sprintf("fork activation blocked: %s", facts[0]))
}

func runForkSourceAdvancedCode(family string) (string, bool) {
	switch strings.TrimSpace(family) {
	case "events":
		return "source_events_advanced_after_fork_point", true
	case "entity_mutations":
		return "source_mutations_advanced_after_fork_point", true
	case "entity_metadata":
		return "source_current_state_advanced_after_fork_point", true
	case "event_deliveries":
		return "source_deliveries_advanced_after_fork_point", true
	case "committed_replay_scopes":
		return "source_committed_replay_scope_advanced_after_fork_point", true
	case "event_receipts":
		return "source_receipts_advanced_after_fork_point", true
	case "dead_letters":
		return "source_dead_letters_advanced_after_fork_point", true
	case "timers":
		return "source_timers_advanced_after_fork_point", true
	case "agent_sessions":
		return "source_sessions_advanced_after_fork_point", true
	case "agent_turns":
		return "source_turns_advanced_after_fork_point", true
	case "agent_conversation_audits":
		return "source_conversation_audits_advanced_after_fork_point", true
	case "reply_contexts":
		return "source_reply_contexts_advanced_after_fork_point", true
	default:
		return "", false
	}
}

func ensureRunForkActivationNoForkReplayState(ctx context.Context, tx *sql.Tx, deliveries *storedelivery.Adapter, forkRunID string) error {
	if deliveries == nil {
		return fmt.Errorf("fork delivery inspection owner is required")
	}
	hasDeliveries, err := deliveries.RunHasDeliveryObligations(ctx, tx, forkRunID)
	if err != nil {
		return fmt.Errorf("check fork_deliveries_already_exist: %w", err)
	}
	if hasDeliveries {
		return runForkReplayResumeError("fork_deliveries_already_exist", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_deliveries_already_exist")
	}
	checks := []struct {
		code  string
		query string
	}{
		{"fork_events_already_exist", `SELECT EXISTS (SELECT 1 FROM events WHERE run_id = $1)`},
		{"fork_sessions_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_sessions WHERE run_id = $1)`},
		{"fork_conversation_audits_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_conversation_audits WHERE run_id = $1)`},
		{"fork_turns_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_turns WHERE run_id = $1)`},
	}
	for _, check := range checks {
		var exists bool
		if err := tx.QueryRowContext(ctx, check.query, forkRunID).Scan(&exists); err != nil {
			return fmt.Errorf("check %s: %w", check.code, err)
		}
		if exists {
			return runForkReplayResumeError(check.code, runfork.RunForkReplayResumeFactForkReplayState, fmt.Sprintf("fork activation blocked: %s", check.code))
		}
	}
	return nil
}
