package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func (s *RunForkSQLiteOwner) requireRunForkSelectedContractExecutionAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkSQLiteOwner) LoadRunForkSelectedContractSourceEventModes(ctx context.Context, sourceRunID string, sourceEventIDs []string) (modes []executionmode.Mode, err error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite store is required")
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return nil, fmt.Errorf("source run_id is required")
	}
	ids := uniqueNonEmptyStrings(sourceEventIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	err = s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var sourceStatus string
		if err := tx.QueryRowContext(txctx, `SELECT status FROM runs WHERE run_id = $1`, sourceRunID).Scan(&sourceStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return &runtimerunlifecycle.RunNotFoundError{RunID: sourceRunID}
			}
			return fmt.Errorf("load selected-contract source event admission status: %w", err)
		}
		if !runForkSelectedContractBranchSourceStatusSupported(sourceStatus) {
			state, parseErr := runtimerunlifecycle.ParseState(sourceStatus)
			if parseErr != nil {
				return parseErr
			}
			return fmt.Errorf("selected-contract source event admission state %s is unsupported", state)
		}
		events, err := loadSQLiteRunForkSelectedContractEvents(txctx, tx, ids)
		if err != nil {
			return fmt.Errorf("load selected-contract source event modes: %w", err)
		}
		if len(events) != len(ids) {
			return fmt.Errorf("selected-contract source event admission found %d of %d events", len(events), len(ids))
		}
		modes = make([]executionmode.Mode, 0, len(events))
		for _, event := range events {
			if event.RunID() != sourceRunID {
				return fmt.Errorf("selected-contract source event %s does not belong to source run %s", event.ID(), sourceRunID)
			}
			modes = append(modes, event.ExecutionMode())
		}
		return nil
	})
	return modes, err
}

func (s *RunForkSQLiteOwner) LoadRunForkSelectedContractSourceEvents(ctx context.Context, sourceRunID, forkRunID string, sourceEventIDs []string, workflowStates []runfork.RunForkSelectedContractWorkflowState) (out []runfork.RunForkSelectedContractSourceEvent, err error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite store is required")
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	forkRunID = strings.TrimSpace(forkRunID)
	if sourceRunID == "" || forkRunID == "" {
		return nil, fmt.Errorf("source and fork run_id are required")
	}
	ids := uniqueNonEmptyStrings(sourceEventIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	err = s.runRuntimeMutation(ctx, "sqlite selected-contract source event preparation", func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		var sourceStatus string
		if err := tx.QueryRowContext(txctx, `SELECT status FROM runs WHERE run_id = $1`, sourceRunID).Scan(&sourceStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return &runtimerunlifecycle.RunNotFoundError{RunID: sourceRunID}
			}
			return fmt.Errorf("load selected-contract source event preparation status: %w", err)
		}
		if !runForkSelectedContractBranchSourceStatusSupported(sourceStatus) {
			state, parseErr := runtimerunlifecycle.ParseState(sourceStatus)
			if parseErr != nil {
				return parseErr
			}
			return fmt.Errorf("selected-contract source event preparation state %s is unsupported", state)
		}
		if err := requireSQLiteRunActive(txctx, tx, forkRunID); err != nil {
			return fmt.Errorf("admit selected-contract source event preparation fork: %w", err)
		}
		events, err := loadSQLiteRunForkSelectedContractEvents(txctx, tx, ids)
		if err != nil {
			return fmt.Errorf("load selected-contract source events: %w", err)
		}
		out = make([]runfork.RunForkSelectedContractSourceEvent, 0, len(ids))
		for _, event := range events {
			if event.RunID() != sourceRunID {
				return fmt.Errorf("selected-contract source event %s does not belong to source run %s", event.ID(), sourceRunID)
			}
			out = append(out, runfork.RunForkSelectedContractSourceEvent{
				SourceEventID: event.ID(), EventName: string(event.Type()), ExecutionMode: event.ExecutionMode(),
				EntityID: event.EntityID(), FlowInstance: event.FlowInstance(), Scope: string(event.Scope()),
				RoutingSource: event.RoutingSource(), Payload: event.Payload(),
			})
		}
		for idx := range out {
			projected, err := projectRunForkSelectedContractSourceEventWorkflowState(sourceRunID, forkRunID, workflowStates, out[idx])
			if err != nil {
				return err
			}
			prepared, err := prepareRunForkSelectedContractSourceEvent(txctx, tx, story, forkRunID, projected)
			if err != nil {
				return err
			}
			out[idx] = prepared
		}
		if err := story.Finalize(txctx); err != nil {
			return fmt.Errorf("finalize selected-contract source event author activity: %w", err)
		}
		if _, err := runforkrevision.FinalizeSQLite(txctx, tx, runforkrevision.NewEffects()); err != nil {
			return fmt.Errorf("finalize selected-contract source event preparation revisions: %w", err)
		}
		return nil
	})
	return out, err
}

func (s *RunForkSQLiteOwner) EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx context.Context, sourceRunID, forkEventID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite store is required")
	}
	return s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := runforkrevision.ValidateCompleteSQLite(txctx, tx, sourceRunID); err != nil {
			return err
		}
		var revision int64
		if err := tx.QueryRowContext(txctx, `
			SELECT MIN(revision)
			FROM run_fork_fact_revisions
			WHERE run_id = $1 AND family = 'events' AND fact_key = $2 AND present
		`, sourceRunID, forkEventID).Scan(&revision); err != nil {
			return fmt.Errorf("resolve committed replay-scope fork revision: %w", err)
		}
		if revision <= 0 {
			return fmt.Errorf("committed replay-scope fork event is not revisioned; recreate the store and retry")
		}
		return ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(txctx, tx, sourceRunID, revision)
	})
}

func (s *RunForkSQLiteOwner) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (result runfork.RunForkActivation, err error) {
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
	if err := s.requireRunForkSelectedContractExecutionAccess(); err != nil {
		return runfork.RunForkActivation{}, err
	}
	handoff, err := reserveRunLifecycleCandidateHandoff(ctx)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	defer handoff.Rollback()
	var divergence *runfork.RunForkSelectedContractBranchDivergence
	err = s.runRuntimeMutation(ctx, "sqlite selected-contract fork activation", func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
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
			return fmt.Errorf("selected-contract fork activation requires materialized fork status %q; got %q", runfork.RunForkMaterializedStatus, lineage.ForkStatus)
		}
		if !runForkSelectedContractBranchSourceStatusSupported(lineage.SourceRunStatus) {
			return fmt.Errorf("selected-contract fork activation requires supported branch source status; got %q", lineage.SourceRunStatus)
		}
		if len(lineage.EntityIDs) == 0 {
			return fmt.Errorf("selected-contract fork activation requires materialized fork entity_state rows")
		}
		binding, err := loadRunForkSelectedContractBinding(txctx, tx, lineage.ForkRunID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("selected-contract fork activation requires selected contract binding")
			}
			return fmt.Errorf("load selected contract binding: %w", err)
		}
		result.SelectedContractBinding = &binding
		plan, err := planRunForkSnapshot(txctx, tx, runfork.RunForkPlanRequest{SourceRunID: lineage.SourceRunID, At: lineage.ForkEventID}, runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint)
		if err != nil {
			return err
		}
		result.ReplayResumeAdmission = runfork.RunForkSelectedContractReplayResumeAdmission(plan)
		expectedRouteRecovery, routeResolved, err := prepareRunForkSelectedContractRouteResolution(plan, lineage.ForkRunID, binding.ContractSelection, req.FrontierAdmission, req.RouteTopology, req.RecipientPlanning)
		if err != nil {
			return err
		}
		if routeResolved {
			if err := validateRunForkSelectedContractRouteRecoveryAtActivation(txctx, tx, expectedRouteRecovery); err != nil {
				return err
			}
			result.ReplayResumeAdmission = runfork.RunForkReplayResumeAdmissionWithSelectedRouteResolution(result.ReplayResumeAdmission)
		}
		if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, result.ReplayResumeAdmission, req.AllowedSourceEventIDs); len(blockers) > 0 {
			result.UnsupportedBlockers = blockers
			return fmt.Errorf("selected-contract fork activation blocked: %s", runForkBlockerCodes(blockers))
		}
		sourceAdvancedFacts, err := collectRunForkSelectedContractSourceAdvancedFacts(txctx, tx, lineage)
		if err != nil {
			return err
		}
		conversationAdvancedFacts := runForkSelectedContractConversationAdvancedFacts(sourceAdvancedFacts)
		result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithSourceAdvancedConversationHistory(result.ReplayResumeAdmission, conversationAdvancedFacts)
		if err := ensureRunForkNoPostForkActiveConversationDeliverySessionCoupling(txctx, tx, sqliteDeliveryAdapter, lineage); err != nil {
			return addRunForkActivationBlocker(&result, err)
		}
		if err := ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(txctx, tx, lineage.SourceRunID, lineage.ForkEventRevision); err != nil {
			return addRunForkActivationBlocker(&result, err)
		}
		sourceAdvancedFacts = append(sourceAdvancedFacts, runfork.ActiveSourceDeliveryConversationCouplingFacts(result.ReplayResumeAdmission)...)
		sourceAdvancedFacts = uniqueNonEmptyStrings(sourceAdvancedFacts)
		result.SourceAdvancedAfterFork = len(sourceAdvancedFacts) > 0
		if err := ensureSQLiteRunForkSelectedContractExecutionForkState(txctx, tx, lineage.ForkRunID, req.AllowedSourceEventIDs); err != nil {
			return addRunForkActivationBlocker(&result, err)
		}
		now := s.now()
		if len(sourceAdvancedFacts) > 0 {
			if _, err := s.RunLifecycleSQLiteOwner.TransitionActiveTx(txctx, tx, story, handoff, runtimerunlifecycle.ActiveTransitionRequest{RunID: lineage.ForkRunID, State: runtimerunlifecycle.StateRunning}); err != nil {
				return fmt.Errorf("activate selected-contract branch fork run lifecycle: %w", err)
			}
			value := runfork.RunForkSelectedContractBranchDivergence{
				Owner:     runfork.RunForkSelectedContractBranchDivergenceOwner,
				ForkRunID: lineage.ForkRunID, SourceRunID: lineage.SourceRunID, ForkEventID: lineage.ForkEventID,
				Policy:                      runfork.RunForkSelectedContractSourceAdvancedBranchPolicy,
				SourceRunStatusAtActivation: lineage.SourceRunStatus, SourceRunStatusAfterActivation: lineage.SourceRunStatus,
				SourceFrozen: false, SourceAdvancedFacts: sourceAdvancedFacts, CreatedAt: now,
			}
			if err := insertSQLiteRunForkSelectedContractBranchDivergence(txctx, tx, value); err != nil {
				return err
			}
			if err := recordRunForkActivationAuthorActivity(txctx, story, lineage, now); err != nil {
				return err
			}
			divergence = &value
		} else if err := s.applyRunForkSourceFreeze(txctx, tx, story, effects, lineage, now, req.ConfirmSourceFreeze, handoff); err != nil {
			return err
		}
		if err := story.Finalize(txctx); err != nil {
			return err
		}
		if _, err := runforkrevision.FinalizeSQLite(txctx, tx, effects); err != nil {
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
	result.Activated = true
	if divergence != nil {
		result.SourceRunStatus = divergence.SourceRunStatusAfterActivation
		result.SourceFrozen = false
		result.BranchDivergence = divergence
	} else {
		result.SourceRunStatus = runfork.RunForkSourceFrozenStatus
		result.SourceFrozen = true
	}
	return result, nil
}

func (s *RunForkSQLiteOwner) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite store is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if err := s.requireRunForkSelectedContractExecutionAccess(); err != nil {
		return err
	}
	return s.runRuntimeMutation(ctx, "sqlite selected-contract fork discard", func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
		snapshot, err := s.RunLifecycleSQLiteOwner.LoadSnapshotTx(txctx, tx, forkRunID)
		if err != nil {
			if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
				return nil
			}
			return err
		}
		if snapshot.State != runtimerunlifecycle.StatePaused {
			return fmt.Errorf("selected-contract fork discard requires materialized fork state %q; got %q", runtimerunlifecycle.StatePaused, snapshot.State)
		}
		if err := guardSQLiteSelectedContractForkDependencies(txctx, tx, forkRunID); err != nil {
			return fmt.Errorf("discard selected-contract fork with dependent lineage: %w", err)
		}
		var preserveCompletionEvidence bool
		if err := tx.QueryRowContext(txctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id = $1)`, forkRunID).Scan(&preserveCompletionEvidence); err != nil {
			return fmt.Errorf("check selected-contract completion evidence preservation: %w", err)
		}
		if _, err := s.TerminalizeRunDeliveriesTx(txctx, tx, story, effects, forkRunID, "fork_discarded"); err != nil {
			return fmt.Errorf("terminalize selected-contract fork deliveries before discard: %w", err)
		}
		if preserveCompletionEvidence {
			if _, _, err := s.RunLifecycleSQLiteOwner.MarkTerminalTx(txctx, tx, story, effects, runtimerunlifecycle.TerminalRequest{RunID: forkRunID, State: runtimerunlifecycle.StateCancelled, EndedAt: s.now()}); err != nil {
				return fmt.Errorf("retain selected-contract completion run tombstone: %w", err)
			}
		}
		if err := story.Finalize(txctx); err != nil {
			return fmt.Errorf("finalize selected-contract fork terminalization activity: %w", err)
		}
		if err := deleteSQLiteSelectedContractForkState(txctx, tx, forkRunID, preserveCompletionEvidence); err != nil {
			return err
		}
		if err := deleteSQLiteSelectedForkEventRecords(txctx, tx, forkRunID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(txctx, `DELETE FROM entity_state WHERE run_id = $1`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork entity state: %w", err)
		}
		if !preserveCompletionEvidence {
			if _, err := tx.ExecContext(txctx, `DELETE FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1`, forkRunID); err != nil {
				return fmt.Errorf("delete selected-contract fork binding: %w", err)
			}
			if err := s.RunLifecycleSQLiteOwner.DeleteMaterializedForkRunTx(txctx, tx, forkRunID); err != nil {
				return err
			}
		} else {
			if err := effects.Add(forkRunID,
				runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations,
				runforkrevision.FamilyEntityMetadata, runforkrevision.FamilyEventDeliveries,
				runforkrevision.FamilyCommittedReplayScopes, runforkrevision.FamilyEventReceipts,
				runforkrevision.FamilyDeadLetters, runforkrevision.FamilyTimers,
				runforkrevision.FamilyAgentSessions,
			); err != nil {
				return err
			}
			if _, err := runforkrevision.FinalizeSQLite(txctx, tx, effects); err != nil {
				return fmt.Errorf("finalize selected-contract fork discard revisions: %w", err)
			}
		}
		return nil
	})
}

func guardSQLiteSelectedContractForkDependencies(ctx context.Context, tx *sql.Tx, forkRunID string) error {
	var dependentRunID string
	err := tx.QueryRowContext(ctx, `
		SELECT run_id FROM runs
		WHERE forked_from_run_id = $1 AND run_id <> $1
		ORDER BY run_id LIMIT 1
	`, forkRunID).Scan(&dependentRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect selected-contract fork dependencies: %w", err)
	}
	return fmt.Errorf("cannot delete source run %s while dependent fork %s remains", forkRunID, dependentRunID)
}

func addRunForkActivationBlocker(result *runfork.RunForkActivation, err error) error {
	if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
		result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
		result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
	}
	return err
}

func insertSQLiteRunForkSelectedContractBranchDivergence(ctx context.Context, tx *sql.Tx, divergence runfork.RunForkSelectedContractBranchDivergence) error {
	if divergence.CreatedAt.IsZero() {
		divergence.CreatedAt = time.Now().UTC()
	}
	facts, err := json.Marshal(uniqueNonEmptyStrings(divergence.SourceAdvancedFacts))
	if err != nil {
		return fmt.Errorf("encode selected-contract branch divergence facts: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_branch_divergences (
			fork_run_id, source_run_id, fork_event_id, owner, policy,
			source_run_status_at_activation, source_run_status_after_activation,
			source_frozen, source_advanced_facts, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (fork_run_id) DO UPDATE SET
			owner = EXCLUDED.owner, policy = EXCLUDED.policy,
			source_run_status_at_activation = EXCLUDED.source_run_status_at_activation,
			source_run_status_after_activation = EXCLUDED.source_run_status_after_activation,
			source_frozen = EXCLUDED.source_frozen,
			source_advanced_facts = EXCLUDED.source_advanced_facts,
			created_at = EXCLUDED.created_at
	`, divergence.ForkRunID, divergence.SourceRunID, divergence.ForkEventID, divergence.Owner, divergence.Policy,
		divergence.SourceRunStatusAtActivation, divergence.SourceRunStatusAfterActivation, divergence.SourceFrozen, string(facts), divergence.CreatedAt)
	if err != nil {
		return fmt.Errorf("record selected-contract branch divergence: %w", err)
	}
	return nil
}

func ensureSQLiteRunForkSelectedContractExecutionForkState(ctx context.Context, tx *sql.Tx, forkRunID string, allowedSourceEventIDs []string) error {
	allowedEvents := uniqueNonEmptyStrings(allowedSourceEventIDs)
	if len(allowedEvents) == 0 {
		return ensureRunForkActivationNoForkReplayState(ctx, tx, sqliteDeliveryAdapter, forkRunID)
	}
	for _, sourceEventID := range allowedEvents {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_executions WHERE fork_run_id = $1 AND source_event_id = $2)`, forkRunID, sourceEventID).Scan(&exists); err != nil {
			return fmt.Errorf("check selected-contract execution lineage completeness: %w", err)
		}
		if !exists {
			return runForkReplayResumeError("fork_selected_contract_execution_lineage_missing", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_selected_contract_execution_lineage_missing")
		}
	}
	deliverySnapshots, err := sqliteDeliveryAdapter.AgentSnapshotsForRun(ctx, tx, forkRunID)
	if err != nil {
		return fmt.Errorf("check selected-contract fork delivery snapshots: %w", err)
	}
	allowedSet := make(map[string]struct{}, len(allowedEvents))
	for _, eventID := range allowedEvents {
		allowedSet[eventID] = struct{}{}
	}
	selectedAgents := []map[string]string{}
	for _, snapshot := range deliverySnapshots {
		if snapshot.Status != runtimedelivery.StatusDelivered {
			return runForkReplayResumeError("fork_selected_contract_agent_delivery_incomplete", runfork.RunForkReplayResumeFactForkReplayState, fmt.Sprintf("fork activation blocked: fork_selected_contract_agent_delivery_incomplete: delivery %s for %s/%s is %s", snapshot.DeliveryID, snapshot.SubscriberClass, snapshot.SubscriberID, snapshot.Status))
		}
		rows, err := tx.QueryContext(ctx, `SELECT source_event_id FROM run_fork_selected_contract_executions WHERE fork_event_id = $1 AND fork_run_id = $2`, snapshot.EventID, forkRunID)
		if err != nil {
			return fmt.Errorf("check selected-contract delivery lineage: %w", err)
		}
		selected := false
		for rows.Next() {
			var sourceEventID string
			if err := rows.Scan(&sourceEventID); err != nil {
				rows.Close()
				return err
			}
			_, selected = allowedSet[sourceEventID]
			if selected {
				break
			}
		}
		rows.Close()
		if selected {
			identity := snapshot.Route.AgentIdentity.Normalize()
			if err := identity.Validate(); err != nil {
				return fmt.Errorf("check selected-contract delivery agent identity: %w", err)
			}
			if identity.AgentID() != strings.TrimSpace(snapshot.SubscriberID) {
				return fmt.Errorf("check selected-contract delivery agent identity: subscriber %q conflicts with %s", snapshot.SubscriberID, identity.Description())
			}
			selectedAgents = append(selectedAgents, map[string]string{"agent_id": identity.AgentID(), "flow_instance": identity.FlowInstance()})
		}
	}
	allowedJSON, _ := json.Marshal(allowedEvents)
	platformJSON, _ := json.Marshal(runForkSelectedContractForkLocalRuntimePlatformEventNames())
	agentsJSON, _ := json.Marshal(selectedAgents)
	var strayEvents int
	if err := tx.QueryRowContext(ctx, `
		WITH RECURSIVE selected_agents(agent_id, flow_instance) AS (
			SELECT json_extract(value, '$.agent_id'), json_extract(value, '$.flow_instance') FROM json_each($4)
		), selected_tree(event_id) AS (
			SELECT e.event_id FROM events e
			JOIN run_fork_selected_contract_executions x ON x.fork_event_id = e.event_id AND x.fork_run_id = $1
			WHERE e.run_id = $1 AND x.source_event_id IN (SELECT value FROM json_each($2))
			UNION
			SELECT e.event_id FROM events e JOIN selected_agents a
			  ON a.agent_id = e.produced_by AND a.flow_instance = COALESCE(json_extract(e.source_route, '$.flow_instance'), '')
			WHERE e.run_id = $1 AND e.produced_by_type = 'agent'
			UNION
			SELECT child.event_id FROM events child JOIN selected_tree parent ON child.source_event_id = parent.event_id
			WHERE child.run_id = $1 AND (child.event_name NOT LIKE 'platform.%' OR child.event_name IN (SELECT value FROM json_each($3)))
		)
		SELECT COUNT(*) FROM events e WHERE e.run_id = $1 AND NOT EXISTS (SELECT 1 FROM selected_tree tree WHERE tree.event_id = e.event_id)
	`, forkRunID, string(allowedJSON), string(platformJSON), string(agentsJSON)).Scan(&strayEvents); err != nil {
		return fmt.Errorf("check selected-contract fork event lineage: %w", err)
	}
	if strayEvents > 0 {
		return runForkReplayResumeError("fork_events_not_selected_contract_lineage", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_events_not_selected_contract_lineage")
	}
	for _, snapshot := range deliverySnapshots {
		var belongs bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE run_id = $1 AND event_id = $2)`, forkRunID, snapshot.EventID).Scan(&belongs); err != nil {
			return fmt.Errorf("check selected-contract fork delivery event: %w", err)
		}
		if !belongs {
			return runForkReplayResumeError("fork_deliveries_not_selected_contract_lineage", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_deliveries_not_selected_contract_lineage")
		}
	}
	return nil
}
