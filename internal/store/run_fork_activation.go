package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

const (
	RunForkActivatedStatus    = "running"
	RunForkSourceFrozenStatus = "forked"
)

type RunForkActivateRequest struct {
	ForkRunID                         string
	ConfirmSourceFreeze               bool
	HistoricalReplayExecutionAdmitter RunForkHistoricalReplayExecutionAdmitter
}

type RunForkActivation struct {
	SourceRunID               string                                   `json:"source_run_id"`
	ForkRunID                 string                                   `json:"fork_run_id"`
	ForkRunStatus             string                                   `json:"fork_run_status"`
	SourceRunStatus           string                                   `json:"source_run_status"`
	ForkPoint                 RunForkPoint                             `json:"fork_point"`
	Activated                 bool                                     `json:"activated"`
	SourceFrozen              bool                                     `json:"source_frozen"`
	ReplayResumeBlocked       bool                                     `json:"replay_resume_blocked"`
	ReplayResumeAdmission     RunForkReplayResumeAdmission             `json:"replay_resume_admission"`
	UnsupportedBlockers       []RunForkUnsupportedBlocker              `json:"unsupported_blockers,omitempty"`
	MaterializedEntityCount   int                                      `json:"materialized_entity_count"`
	HistoricalReplayExecution *RunForkHistoricalReplayExecution        `json:"historical_replay_execution,omitempty"`
	DeliveryEventReplay       *RunForkDeliveryEventReplayResult        `json:"delivery_event_replay,omitempty"`
	SelectedContractBinding   *RunForkSelectedContractBinding          `json:"selected_contract_binding,omitempty"`
	BranchDivergence          *RunForkSelectedContractBranchDivergence `json:"selected_contract_branch_divergence,omitempty"`
	SourceAdvancedAfterFork   bool                                     `json:"source_advanced_after_fork_point,omitempty"`
	RepeatedActivationFailed  bool                                     `json:"repeated_activation_failed,omitempty"`
}

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

func RequireRunForkActivationCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	if err := RequireRunForkMaterializerCapabilities(caps, catalog); err != nil {
		return err
	}
	required := map[string][]string{
		"runs":                          {"run_id", "status", "forked_from_run_id", "forked_from_event_id", "continued_as_run_id", "ended_at"},
		"entity_state":                  {"run_id", "entity_id", "flow_instance", "updated_at"},
		runForkDeliveryEventReplayTable: {"fork_run_id", "source_run_id", "source_event_id", "source_delivery_id", "fork_event_id", "fork_delivery_id", "subscriber_type", "subscriber_id"},
	}
	for tableName, columns := range required {
		if catalog.hasColumns(tableName, columns...) {
			continue
		}
		return fmt.Errorf("run fork activation requires %s columns %v", tableName, columns)
	}
	return nil
}

func (s *PostgresStore) requireRunForkActivationCapabilities(ctx context.Context) (schemaColumnCatalog, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return schemaColumnCatalog{}, err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return schemaColumnCatalog{}, err
	}
	return catalog, RequireRunForkActivationCapabilities(caps, catalog)
}

func (s *PostgresStore) ActivateRunFork(ctx context.Context, req RunForkActivateRequest) (RunForkActivation, error) {
	if s == nil || s.DB == nil {
		return RunForkActivation{}, fmt.Errorf("postgres store is required")
	}
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return RunForkActivation{}, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return RunForkActivation{}, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	catalog, err := s.requireRunForkActivationCapabilities(ctx)
	if err != nil {
		return RunForkActivation{}, err
	}

	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return RunForkActivation{}, fmt.Errorf("begin fork activation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	storyctx, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		return RunForkActivation{}, err
	}
	ctx = storyctx

	lineage, err := loadRunForkActivationLineage(ctx, tx, forkRunID)
	if err != nil {
		return RunForkActivation{}, err
	}
	if err := lockRunForkSourceRevisionFrontier(ctx, tx, &lineage); err != nil {
		return RunForkActivation{}, err
	}
	result := RunForkActivation{
		SourceRunID:             lineage.SourceRunID,
		ForkRunID:               lineage.ForkRunID,
		ForkRunStatus:           lineage.ForkStatus,
		SourceRunStatus:         lineage.SourceRunStatus,
		ForkPoint:               RunForkPoint{Input: lineage.ForkEventID, EventID: lineage.ForkEventID, EventName: lineage.ForkEventName, Timestamp: lineage.ForkEventTime, Revision: lineage.ForkEventRevision},
		ReplayResumeBlocked:     true,
		MaterializedEntityCount: len(lineage.EntityIDs),
	}
	if lineage.ForkStatus != RunForkMaterializedStatus {
		result.RepeatedActivationFailed = lineage.ForkStatus == RunForkActivatedStatus
		return result, fmt.Errorf("fork activation requires materialized fork status %q; got %q", RunForkMaterializedStatus, lineage.ForkStatus)
	}
	if lineage.SourceRunStatus != "running" && lineage.SourceRunStatus != "paused" {
		return result, fmt.Errorf("fork activation requires source run status running or paused before freeze; got %q", lineage.SourceRunStatus)
	}
	if len(lineage.EntityIDs) == 0 {
		return result, fmt.Errorf("fork activation requires materialized fork entity_state rows")
	}
	if catalog.hasColumns(runForkSelectedContractBindingTable, "fork_run_id", "source_run_id", "fork_event_id", "mode", "contracts_root", "bundle_hash", "workflow_name", "workflow_version", "created_at") {
		binding, err := loadRunForkSelectedContractBinding(ctx, tx, lineage.ForkRunID)
		if err == nil {
			result.SelectedContractBinding = &binding
		} else if err != sql.ErrNoRows {
			return result, fmt.Errorf("load selected contract binding: %w", err)
		}
	}

	plan, err := s.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: lineage.SourceRunID, At: lineage.ForkEventID})
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
	if err := ensureRunForkActivationNoForkReplayState(ctx, tx, catalog, lineage.ForkRunID); err != nil {
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
	}

	now := time.Now().UTC()
	replayResult := RunForkDeliveryEventReplayResult{
		Owner:       RunForkDeliveryEventReplayOwner,
		SourceRunID: lineage.SourceRunID,
		ForkRunID:   lineage.ForkRunID,
	}
	if historicalReplayExecution.DeliveryEventReplayReady {
		replayResult, err = applyRunForkDeliveryEventReplay(ctx, tx, s, lineage, historicalReplayExecution, now)
		if err != nil {
			return result, err
		}
	}
	if err := applyRunForkSourceFreeze(ctx, tx, lineage, now, req.ConfirmSourceFreeze); err != nil {
		return result, err
	}
	if err := commitRunForkAuthorActivityTransaction(ctx, tx); err != nil {
		return result, fmt.Errorf("commit fork activation: %w", err)
	}
	committed = true
	result.ForkRunStatus = RunForkActivatedStatus
	result.SourceRunStatus = RunForkSourceFrozenStatus
	result.Activated = true
	result.SourceFrozen = true
	if replayResult.ReplayedEventCount > 0 || replayResult.ReplayedDeliveryCount > 0 {
		historicalReplayExecution.DeliveryEventReplay = &replayResult
		result.HistoricalReplayExecution = &historicalReplayExecution
		result.DeliveryEventReplay = &replayResult
	}
	return result, nil
}

func recordRunForkActivationAuthorActivity(ctx context.Context, lineage runForkActivationLineage, now time.Time, sourceFrozen bool) error {
	occurrences := []struct {
		runID, transition, parentRunID, forkRunID string
	}{{runID: lineage.ForkRunID, transition: "fork_started", parentRunID: lineage.SourceRunID}}
	if sourceFrozen {
		occurrences = append(occurrences, struct {
			runID, transition, parentRunID, forkRunID string
		}{runID: lineage.SourceRunID, transition: "forked", forkRunID: lineage.ForkRunID})
	}
	for _, occurrence := range occurrences {
		bundleHash := lineage.ForkBundleHash
		if occurrence.runID == lineage.SourceRunID {
			bundleHash = lineage.SourceBundleHash
		}
		occurrenceScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, bundleHash)
		if err != nil {
			return fmt.Errorf("record run fork activation source scope: %w", err)
		}
		transitionID := uuid.NewString()
		if err := runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
			Kind: runtimeauthoractivity.KindRunLifecycle, Transition: occurrence.transition,
			SourceOwner: "runs", SourceIdentity: transitionID, DedupKey: "run-transition:" + transitionID,
			OccurredAt: now.UTC(), RunID: occurrence.runID, Scope: occurrenceScope,
			Projection: runtimeauthoractivity.Projection{
				SubjectType: "run", SubjectID: occurrence.runID, ParentRunID: occurrence.parentRunID,
				ForkRunID: occurrence.forkRunID, TriggerEventType: lineage.ForkEventName,
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func commitRunForkAuthorActivityTransaction(ctx context.Context, tx *sql.Tx) error {
	if err := runtimepipeline.CapturePipelineRunForkRevisionChanges(ctx, tx); err != nil {
		return err
	}
	if err := runtimeauthoractivity.Finalize(ctx); err != nil {
		return err
	}
	return tx.Commit()
}

func requireRunForkHistoricalReplayExecution(
	ctx context.Context,
	admitter RunForkHistoricalReplayExecutionAdmitter,
	lineage runForkActivationLineage,
	plan RunForkPlan,
) (RunForkHistoricalReplayExecution, error) {
	if !plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		return RunForkHistoricalReplayExecution{}, nil
	}
	if admitter == nil {
		return RunForkHistoricalReplayExecution{}, fmt.Errorf("%s admission required before delivery_event_replay_ready mutation", RunForkHistoricalReplayExecutionOwner)
	}
	execution, err := admitter.AdmitRunForkHistoricalReplayExecution(ctx, RunForkHistoricalReplayExecutionRequest{
		ForkRunID:             lineage.ForkRunID,
		SourceRunID:           lineage.SourceRunID,
		ForkEventID:           lineage.ForkEventID,
		ReplayResumeAdmission: plan.ReplayResumeAdmission,
		PendingWork:           plan.PendingWork,
	})
	if err != nil {
		return RunForkHistoricalReplayExecution{}, err
	}
	if strings.TrimSpace(execution.Owner) != RunForkHistoricalReplayExecutionOwner {
		return RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation requires %s; got %q", RunForkHistoricalReplayExecutionOwner, execution.Owner)
	}
	if strings.TrimSpace(execution.AdmissionOwner) != RunForkHistoricalReplayExecutionAdmissionOwner {
		return RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation requires %s; got %q", RunForkHistoricalReplayExecutionAdmissionOwner, execution.AdmissionOwner)
	}
	if strings.TrimSpace(execution.ForkRunID) != lineage.ForkRunID ||
		strings.TrimSpace(execution.SourceRunID) != lineage.SourceRunID ||
		strings.TrimSpace(execution.ForkEventID) != lineage.ForkEventID {
		return RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation historical replay execution identity mismatch")
	}
	if !execution.DeliveryEventReplayReady ||
		execution.EventDeliveriesAdmission.Fact != RunForkHistoricalReplayFactEventDeliveries ||
		execution.EventDeliveriesAdmission.Admission != RunForkHistoricalReplayAdmissionExecutableForkWork {
		return RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation requires event_deliveries executable fork work admission")
	}
	if len(execution.DeliveryEventReplayWork) == 0 {
		return RunForkHistoricalReplayExecution{}, fmt.Errorf("delivery/event replay mutation requires owner-authorized delivery_event_replay_ready work")
	}
	return execution, nil
}

func loadRunForkActivationLineage(ctx context.Context, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
	var lineage runForkActivationLineage
	var forkEventTime sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT
			f.run_id::text,
			COALESCE(f.status, ''),
			COALESCE(f.bundle_hash, ''),
			COALESCE(f.forked_from_run_id::text, ''),
			COALESCE(s.bundle_hash, ''),
			COALESCE(f.forked_from_event_id::text, ''),
			COALESCE(s.status, ''),
			COALESCE(e.event_name, ''),
			e.created_at
		FROM runs f
		LEFT JOIN runs s ON s.run_id = f.forked_from_run_id
		LEFT JOIN events e ON e.run_id = f.forked_from_run_id AND e.event_id = f.forked_from_event_id
		WHERE f.run_id = $1::uuid
		FOR UPDATE OF f
	`, forkRunID).Scan(
		&lineage.ForkRunID,
		&lineage.ForkStatus,
		&lineage.ForkBundleHash,
		&lineage.SourceRunID,
		&lineage.SourceBundleHash,
		&lineage.ForkEventID,
		&lineage.SourceRunStatus,
		&lineage.ForkEventName,
		&forkEventTime,
	)
	if err == sql.ErrNoRows {
		return runForkActivationLineage{}, fmt.Errorf("fork run %s not found", forkRunID)
	}
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load fork activation lineage: %w", err)
	}
	if lineage.SourceRunID == "" || lineage.ForkEventID == "" {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires fork lineage")
	}
	if lineage.SourceRunStatus == "" || !forkEventTime.Valid {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run and fork point event")
	}
	lineage.ForkEventTime = forkEventTime.Time
	if err := tx.QueryRowContext(ctx, `SELECT status, COALESCE(bundle_hash, '') FROM runs WHERE run_id = $1::uuid FOR UPDATE`, lineage.SourceRunID).Scan(&lineage.SourceRunStatus, &lineage.SourceBundleHash); err != nil {
		return runForkActivationLineage{}, fmt.Errorf("lock source run: %w", err)
	}
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

func collectRunForkSourceAdvancedFacts(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT family
		FROM run_fork_fact_revisions
		WHERE run_id = $1::uuid
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
	return runForkReplayResumeError(facts[0], RunForkReplayResumeFactSourceAdvanced, fmt.Sprintf("fork activation blocked: %s", facts[0]))
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

func ensureRunForkActivationNoForkReplayState(ctx context.Context, tx *sql.Tx, catalog schemaColumnCatalog, forkRunID string) error {
	checks := []struct {
		code  string
		query string
	}{
		{"fork_events_already_exist", `SELECT EXISTS (SELECT 1 FROM events WHERE run_id = $1::uuid)`},
		{"fork_deliveries_already_exist", `SELECT EXISTS (SELECT 1 FROM event_deliveries WHERE run_id = $1::uuid)`},
	}
	if catalog.hasColumns("agent_sessions", "run_id") {
		checks = append(checks, struct {
			code  string
			query string
		}{"fork_sessions_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_sessions WHERE run_id = $1::uuid)`})
	}
	if catalog.hasColumns("agent_conversation_audits", "run_id") {
		checks = append(checks, struct {
			code  string
			query string
		}{"fork_conversation_audits_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_conversation_audits WHERE run_id = $1::uuid)`})
	}
	if catalog.hasColumns("agent_turns", "run_id") {
		checks = append(checks, struct {
			code  string
			query string
		}{"fork_turns_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_turns WHERE run_id = $1::uuid)`})
	}
	for _, check := range checks {
		var exists bool
		if err := tx.QueryRowContext(ctx, check.query, forkRunID).Scan(&exists); err != nil {
			return fmt.Errorf("check %s: %w", check.code, err)
		}
		if exists {
			return runForkReplayResumeError(check.code, RunForkReplayResumeFactForkReplayState, fmt.Sprintf("fork activation blocked: %s", check.code))
		}
	}
	return nil
}
