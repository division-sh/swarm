package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

const (
	RunForkSelectedContractExecutionLineageOwner = "store.run_fork.selected_contract_execution_lineage"
	runForkSelectedContractExecutionLineageTable = "run_fork_selected_contract_executions"
	runForkSelectedContractBranchDivergenceTable = "run_fork_selected_contract_branch_divergences"
)

type RunForkSelectedContractExecutionMaterializeRequest struct {
	SourceRunID       string
	At                string
	ContractSelection RunForkContractSelection
	BundleHash        string
	BundleSource      string
	FrontierAdmission RunForkContractFrontierAdmission
	RouteTopology     RunForkSelectedContractRouteTopology
	RecipientPlanning RunForkSelectedContractRecipientPlanning
}

type RunForkSelectedContractExecutionActivateRequest struct {
	ForkRunID             string
	AllowedSourceEventIDs []string
	FrontierAdmission     RunForkContractFrontierAdmission
	RouteTopology         RunForkSelectedContractRouteTopology
	RecipientPlanning     RunForkSelectedContractRecipientPlanning
}

type RunForkSelectedContractSourceEvent struct {
	SourceEventID string          `json:"source_event_id"`
	EventName     string          `json:"event_name"`
	EntityID      string          `json:"entity_id,omitempty"`
	FlowInstance  string          `json:"flow_instance,omitempty"`
	Scope         string          `json:"scope,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

type RunForkSelectedContractExecutionLineage struct {
	Owner         string    `json:"owner"`
	ForkRunID     string    `json:"fork_run_id"`
	SourceRunID   string    `json:"source_run_id"`
	SourceEventID string    `json:"source_event_id"`
	ForkEventID   string    `json:"fork_event_id"`
	EventName     string    `json:"event_name"`
	CreatedAt     time.Time `json:"created_at"`
}

type RunForkSelectedContractBranchDivergence struct {
	Owner                          string    `json:"owner"`
	ForkRunID                      string    `json:"fork_run_id"`
	SourceRunID                    string    `json:"source_run_id"`
	ForkEventID                    string    `json:"fork_event_id"`
	Policy                         string    `json:"policy"`
	SourceRunStatusAtActivation    string    `json:"source_run_status_at_activation"`
	SourceRunStatusAfterActivation string    `json:"source_run_status_after_activation"`
	SourceFrozen                   bool      `json:"source_frozen"`
	SourceAdvancedFacts            []string  `json:"source_advanced_facts,omitempty"`
	CreatedAt                      time.Time `json:"created_at"`
}

func prepareRunForkSelectedContractRouteResolution(
	plan RunForkPlan,
	forkRunID string,
	selection RunForkContractSelection,
	frontier RunForkContractFrontierAdmission,
	topology RunForkSelectedContractRouteTopology,
	planning RunForkSelectedContractRecipientPlanning,
) (RunForkSelectedContractRouteRecovery, bool, error) {
	switch strings.TrimSpace(plan.RouteHistory.State) {
	case RunForkRouteHistoryNotApplicable:
		return RunForkSelectedContractRouteRecovery{}, false, nil
	case RunForkRouteHistoryUnknownUnversioned:
	default:
		return RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("selected-contract route resolution received unsupported route history state %q", plan.RouteHistory.State)
	}
	if strings.TrimSpace(frontier.Owner) != RunForkContractFrontierAdmissionOwner || !frontier.NonMutating {
		return RunForkSelectedContractRouteRecovery{}, false, runForkReplayResumeError(
			RunForkBlockerFlowRouteHistoryUnproven,
			RunForkReplayResumeFactRouteHistory,
			"selected-contract route resolution requires canonical frontier admission",
		)
	}
	if !topology.StaticTopologySupported || !topology.DynamicTopologySupported {
		return RunForkSelectedContractRouteRecovery{}, false, runForkReplayResumeError(
			RunForkBlockerFlowRouteHistoryUnproven,
			RunForkReplayResumeFactRouteHistory,
			"selected-contract route resolution requires complete static and dynamic topology proof",
		)
	}
	if err := validateRunForkSelectedContractRouteRecoverySelection("route resolution frontier", selection, frontier.ContractSelection); err != nil {
		return RunForkSelectedContractRouteRecovery{}, false, err
	}
	count, eventIDs, fingerprint := RunForkContractFrontierEvidenceBinding(frontier)
	if count != topology.FrontierEventCount || !equalTrimmedStrings(eventIDs, topology.FrontierSourceEventIDs) || fingerprint != strings.TrimSpace(topology.FrontierEvidenceFingerprint) {
		return RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("selected-contract route topology does not match the fixed-event frontier")
	}
	if plan.historicalSnapshot == nil || plan.historicalSnapshot.Revision != plan.ForkPoint.Revision {
		return RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("selected-contract route resolution requires the fixed-event revision snapshot")
	}
	historicalEvents := map[string]struct{}{}
	for _, event := range plan.historicalSnapshot.Events {
		historicalEvents[strings.TrimSpace(event.EventID)] = struct{}{}
	}
	for _, eventID := range eventIDs {
		if _, ok := historicalEvents[strings.TrimSpace(eventID)]; !ok {
			return RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("selected-contract route frontier event %s is outside fixed revision %d", eventID, plan.ForkPoint.Revision)
		}
	}
	record, err := normalizeRunForkSelectedContractRouteRecovery(RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       plan.SourceRunID,
		ForkEventID:       plan.ForkPoint.EventID,
		ContractSelection: selection,
		RouteTopology:     topology,
		RecipientPlanning: planning,
	}, time.Now().UTC())
	if err != nil {
		return RunForkSelectedContractRouteRecovery{}, false, err
	}
	return record, true, nil
}

func equalTrimmedStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.TrimSpace(left[i]) != strings.TrimSpace(right[i]) {
			return false
		}
	}
	return true
}

func RequireRunForkSelectedContractExecutionCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	if err := RequireRunForkActivationCapabilities(caps, catalog); err != nil {
		return err
	}
	if err := RequireRunForkSelectedContractRouteRecoveryCapabilities(caps, catalog); err != nil {
		return err
	}
	required := []struct {
		table   string
		columns []string
	}{
		{runForkSelectedContractExecutionLineageTable, []string{"fork_run_id", "source_run_id", "source_event_id", "fork_event_id", "event_name", "created_at"}},
		{runForkSelectedContractBranchDivergenceTable, []string{"fork_run_id", "source_run_id", "fork_event_id", "owner", "policy", "source_run_status_at_activation", "source_run_status_after_activation", "source_frozen", "source_advanced_facts", "created_at"}},
		{"activity_attempts", []string{"request_event_id", "run_id", "source_event_id", "parent_event_id", "entity_id", "node_id", "handler_event_key", "activity_id", "tool", "effect_class", "attempt", "status", "success_event", "failure_event", "result_event_id", "result_event_type", "result_payload", "failure", "input_hash", "loop_generation", "loop_stage", "started_at", "completed_at", "updated_at"}},
	}
	for _, requirement := range required {
		if catalog.hasColumns(requirement.table, requirement.columns...) {
			continue
		}
		return fmt.Errorf("selected-contract fork execution requires %s columns %v", requirement.table, requirement.columns)
	}
	return nil
}

func (s *PostgresStore) requireRunForkSelectedContractExecutionCapabilities(ctx context.Context) (schemaColumnCatalog, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return schemaColumnCatalog{}, err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return schemaColumnCatalog{}, err
	}
	return catalog, RequireRunForkSelectedContractExecutionCapabilities(caps, catalog)
}

func (s *PostgresStore) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req RunForkSelectedContractExecutionMaterializeRequest) (RunForkMaterialization, error) {
	if s == nil || s.DB == nil {
		return RunForkMaterialization{}, fmt.Errorf("postgres store is required")
	}
	if _, err := s.requireRunForkSelectedContractExecutionCapabilities(ctx); err != nil {
		return RunForkMaterialization{}, err
	}
	if err := s.requireRunForkMaterializerCapabilities(ctx); err != nil {
		return RunForkMaterialization{}, err
	}
	if err := s.requireRunForkSelectedContractBindingCapabilities(ctx); err != nil {
		return RunForkMaterialization{}, err
	}
	selection, err := normalizeRunForkSelectedContractSelection(req.ContractSelection)
	if err != nil {
		return RunForkMaterialization{}, err
	}
	plan, err := s.PlanRunFork(ctx, RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(req.SourceRunID),
		At:          strings.TrimSpace(req.At),
	})
	if err != nil {
		return RunForkMaterialization{}, err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return RunForkMaterialization{}, err
	}
	replayAdmission := RunForkSelectedContractReplayResumeAdmission(plan)
	timerReconstruction, err := s.planRunForkSelectedContractTimerReconstruction(ctx, catalog, plan)
	if err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			admission := runForkReplayResumeAdmissionWithBlocker(replayAdmission, fact, blocker)
			return RunForkMaterialization{
				SourceRunID:           plan.SourceRunID,
				ForkPoint:             plan.ForkPoint,
				ExecutionReady:        false,
				ReplayResumeAdmission: admission,
				UnsupportedBlockers:   admission.UnsupportedBlockers,
				DeliveryResumeBlocked: true,
			}, err
		}
		return RunForkMaterialization{}, err
	}
	replayAdmission = runForkReplayResumeAdmissionWithTimerReconstruction(replayAdmission, timerReconstruction)
	forkRunID := deterministicRunForkMaterializationID(plan.SourceRunID, plan.ForkPoint.EventID)
	routeRecovery, routeResolved, err := prepareRunForkSelectedContractRouteResolution(plan, forkRunID, selection, req.FrontierAdmission, req.RouteTopology, req.RecipientPlanning)
	if err != nil {
		return RunForkMaterialization{}, err
	}
	if routeResolved {
		replayAdmission = RunForkReplayResumeAdmissionWithSelectedRouteResolution(replayAdmission)
	}
	if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, replayAdmission, nil); len(blockers) > 0 {
		return RunForkMaterialization{
			SourceRunID:           plan.SourceRunID,
			ForkPoint:             plan.ForkPoint,
			ExecutionReady:        false,
			ReplayResumeAdmission: replayAdmission,
			UnsupportedBlockers:   blockers,
			DeliveryResumeBlocked: true,
		}, fmt.Errorf("selected-contract fork execution materialization blocked: %s", runForkBlockerCodes(blockers))
	}

	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return RunForkMaterialization{}, fmt.Errorf("begin selected-contract fork materialization: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := ensureRunForkNotAlreadyMaterialized(ctx, tx, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID); err != nil {
		return RunForkMaterialization{}, err
	}
	if err := ensureRunForkActivationNoForkReplayState(ctx, tx, catalog, forkRunID); err != nil {
		return RunForkMaterialization{}, err
	}
	metadata, err := loadRunForkEntityMetadata(plan)
	if err != nil {
		return RunForkMaterialization{}, err
	}
	now := time.Now().UTC()
	if err := insertRunForkRun(ctx, tx, catalog, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID, len(plan.Entities), now, runForkBundleInsertIdentity{
		BundleHash:   req.BundleHash,
		BundleSource: req.BundleSource,
	}); err != nil {
		return RunForkMaterialization{}, fmt.Errorf("insert selected-contract fork run: %w", err)
	}

	forkCtx := runtimecorrelation.WithRunID(ctx, forkRunID)
	for _, entity := range plan.Entities {
		if err := materializeRunForkEntityState(forkCtx, tx, forkRunID, plan, entity, metadata[entity.EntityID], now); err != nil {
			return RunForkMaterialization{}, err
		}
	}
	binding, err := insertRunForkSelectedContractBinding(ctx, tx, RunForkSelectedContractBindingRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       plan.SourceRunID,
		ForkEventID:       plan.ForkPoint.EventID,
		ContractSelection: selection,
	}, now)
	if err != nil {
		return RunForkMaterialization{}, err
	}
	if routeResolved {
		if err := insertRunForkSelectedContractRouteRecovery(ctx, tx, routeRecovery); err != nil {
			return RunForkMaterialization{}, err
		}
	}
	if err := insertRunForkSelectedContractTimerReconstructions(ctx, tx, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID, timerReconstruction, now); err != nil {
		return RunForkMaterialization{}, err
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return RunForkMaterialization{}, fmt.Errorf("commit selected-contract fork materialization: %w", err)
	}
	committed = true
	unsupportedBlockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, replayAdmission, nil)
	return RunForkMaterialization{
		SourceRunID:              plan.SourceRunID,
		ForkRunID:                forkRunID,
		ForkRunStatus:            RunForkMaterializedStatus,
		ForkPoint:                plan.ForkPoint,
		MaterializedEntityCount:  len(plan.Entities),
		ExecutionReady:           false,
		ReplayResumeAdmission:    replayAdmission,
		SelectedContractBinding:  &binding,
		UnsupportedBlockers:      unsupportedBlockers,
		DeliveryResumeBlocked:    true,
		SourceRunStatusUnchanged: true,
	}, nil
}

func (s *PostgresStore) ActivateRunForkForSelectedContractExecution(ctx context.Context, req RunForkSelectedContractExecutionActivateRequest) (RunForkActivation, error) {
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
	catalog, err := s.requireRunForkSelectedContractExecutionCapabilities(ctx)
	if err != nil {
		return RunForkActivation{}, err
	}

	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return RunForkActivation{}, fmt.Errorf("begin selected-contract fork activation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

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
		return result, fmt.Errorf("selected-contract fork activation requires materialized fork status %q; got %q", RunForkMaterializedStatus, lineage.ForkStatus)
	}
	if !runForkSelectedContractBranchSourceStatusSupported(lineage.SourceRunStatus) {
		return result, fmt.Errorf("selected-contract fork activation requires source run status running, paused, completed, failed, or cancelled for branch lineage; got %q", lineage.SourceRunStatus)
	}
	if len(lineage.EntityIDs) == 0 {
		return result, fmt.Errorf("selected-contract fork activation requires materialized fork entity_state rows")
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, lineage.ForkRunID)
	if err != nil {
		if err == sql.ErrNoRows {
			return result, fmt.Errorf("selected-contract fork activation requires selected contract binding")
		}
		return result, fmt.Errorf("load selected contract binding: %w", err)
	}
	result.SelectedContractBinding = &binding

	plan, err := s.PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: lineage.SourceRunID, At: lineage.ForkEventID})
	if err != nil {
		return result, err
	}
	result.ReplayResumeAdmission = RunForkSelectedContractReplayResumeAdmission(plan)
	expectedRouteRecovery, routeResolved, err := prepareRunForkSelectedContractRouteResolution(
		plan,
		lineage.ForkRunID,
		binding.ContractSelection,
		req.FrontierAdmission,
		req.RouteTopology,
		req.RecipientPlanning,
	)
	if err != nil {
		return result, err
	}
	if routeResolved {
		if err := validateRunForkSelectedContractRouteRecoveryAtActivation(ctx, tx, expectedRouteRecovery); err != nil {
			return result, err
		}
		result.ReplayResumeAdmission = RunForkReplayResumeAdmissionWithSelectedRouteResolution(result.ReplayResumeAdmission)
	}
	timerResolved, err := runForkSelectedContractTimerReconstructionComplete(ctx, tx, lineage, plan)
	if err != nil {
		return result, err
	}
	result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithTimerReconstruction(result.ReplayResumeAdmission, runForkTimerReconstructionPlan{Required: timerResolved})
	if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, result.ReplayResumeAdmission, req.AllowedSourceEventIDs); len(blockers) > 0 {
		result.UnsupportedBlockers = blockers
		return result, fmt.Errorf("selected-contract fork activation blocked: %s", runForkBlockerCodes(blockers))
	}
	sourceAdvancedFacts, err := collectRunForkSelectedContractSourceAdvancedFacts(ctx, tx, lineage)
	if err != nil {
		return result, err
	}
	conversationAdvancedFacts := runForkSelectedContractConversationAdvancedFacts(sourceAdvancedFacts)
	result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithSourceAdvancedConversationHistory(result.ReplayResumeAdmission, conversationAdvancedFacts)
	if err := ensureRunForkNoPostForkActiveConversationDeliverySessionCoupling(ctx, tx, lineage); err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
	}
	if err := ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(ctx, tx, lineage.SourceRunID, lineage.ForkEventRevision); err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
	}
	sourceAdvancedFacts = append(sourceAdvancedFacts, runForkSelectedContractActiveSourceDeliveryConversationCouplingFacts(result.ReplayResumeAdmission)...)
	sourceAdvancedFacts = uniqueNonEmptyStrings(sourceAdvancedFacts)
	if len(sourceAdvancedFacts) > 0 {
		result.SourceAdvancedAfterFork = true
	}
	if err := ensureRunForkSelectedContractExecutionForkState(ctx, tx, catalog, lineage.ForkRunID, req.AllowedSourceEventIDs); err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
	}

	now := time.Now().UTC()
	if len(sourceAdvancedFacts) > 0 {
		forkResult, err := tx.ExecContext(ctx, `
			UPDATE runs
			SET status = $2, ended_at = NULL
			WHERE run_id = $1::uuid
			  AND status = $3
		`, lineage.ForkRunID, RunForkActivatedStatus, RunForkMaterializedStatus)
		if err != nil {
			return result, fmt.Errorf("activate selected-contract branch fork run: %w", err)
		}
		if affected, err := forkResult.RowsAffected(); err != nil {
			return result, fmt.Errorf("confirm selected-contract branch fork activation: %w", err)
		} else if affected != 1 {
			return result, fmt.Errorf("selected-contract branch activation blocked: fork_run_activation_not_applied")
		}
		divergence := RunForkSelectedContractBranchDivergence{
			Owner:                          RunForkSelectedContractBranchDivergenceOwner,
			ForkRunID:                      lineage.ForkRunID,
			SourceRunID:                    lineage.SourceRunID,
			ForkEventID:                    lineage.ForkEventID,
			Policy:                         RunForkSelectedContractSourceAdvancedBranchPolicy,
			SourceRunStatusAtActivation:    lineage.SourceRunStatus,
			SourceRunStatusAfterActivation: lineage.SourceRunStatus,
			SourceFrozen:                   false,
			SourceAdvancedFacts:            sourceAdvancedFacts,
			CreatedAt:                      now,
		}
		if err := insertRunForkSelectedContractBranchDivergence(ctx, tx, divergence); err != nil {
			return result, err
		}
		if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
			return result, fmt.Errorf("commit selected-contract branch activation: %w", err)
		}
		committed = true
		result.ForkRunStatus = RunForkActivatedStatus
		result.SourceRunStatus = lineage.SourceRunStatus
		result.Activated = true
		result.SourceFrozen = false
		result.BranchDivergence = &divergence
		return result, nil
	}

	sourceResult, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = $2, ended_at = COALESCE(ended_at, $3)
		WHERE run_id = $1::uuid
		  AND status IN ('running', 'paused')
	`, lineage.SourceRunID, RunForkSourceFrozenStatus, now)
	if err != nil {
		return result, fmt.Errorf("freeze source run: %w", err)
	}
	if affected, err := sourceResult.RowsAffected(); err != nil {
		return result, fmt.Errorf("confirm source freeze: %w", err)
	} else if affected != 1 {
		return result, fmt.Errorf("selected-contract fork activation blocked: source_run_freeze_not_applied")
	}
	forkResult, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = $2, ended_at = NULL
		WHERE run_id = $1::uuid
		  AND status = $3
	`, lineage.ForkRunID, RunForkActivatedStatus, RunForkMaterializedStatus)
	if err != nil {
		return result, fmt.Errorf("activate fork run: %w", err)
	}
	if affected, err := forkResult.RowsAffected(); err != nil {
		return result, fmt.Errorf("confirm fork activation: %w", err)
	} else if affected != 1 {
		return result, fmt.Errorf("selected-contract fork activation blocked: fork_run_activation_not_applied")
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return result, fmt.Errorf("commit selected-contract fork activation: %w", err)
	}
	committed = true
	result.ForkRunStatus = RunForkActivatedStatus
	result.SourceRunStatus = RunForkSourceFrozenStatus
	result.Activated = true
	result.SourceFrozen = true
	return result, nil
}

func (s *PostgresStore) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	catalog, err := s.requireRunForkSelectedContractExecutionCapabilities(ctx)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin selected-contract fork discard: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, forkRunID).Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("load fork run for discard: %w", err)
	}
	if status != RunForkMaterializedStatus {
		return fmt.Errorf("selected-contract fork discard requires materialized fork status %q; got %q", RunForkMaterializedStatus, status)
	}
	if err := guardDestructiveResetSourceForkDependencies(ctx, tx, []string{forkRunID}); err != nil {
		return fmt.Errorf("discard selected-contract fork with dependent lineage: %w", err)
	}
	var preserveCompletionEvidence bool
	if catalog.hasColumns("run_fork_selected_contract_runtime_executions", "fork_run_id") {
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1::uuid)`, forkRunID).Scan(&preserveCompletionEvidence); err != nil {
			return fmt.Errorf("check selected-contract completion evidence preservation: %w", err)
		}
	}

	if !preserveCompletionEvidence && catalog.hasColumns("agent_turns", "run_id") {
		if _, err := tx.ExecContext(ctx, `DELETE FROM agent_turns WHERE run_id = $1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork turns: %w", err)
		}
	}
	if !preserveCompletionEvidence && catalog.hasColumns("agent_conversation_audits", "run_id") {
		if _, err := tx.ExecContext(ctx, `DELETE FROM agent_conversation_audits WHERE run_id = $1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork conversation audits: %w", err)
		}
	}
	if catalog.hasColumns("agent_sessions", "run_id") {
		if _, err := tx.ExecContext(ctx, `DELETE FROM agent_sessions WHERE run_id = $1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork sessions: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM run_fork_selected_contract_branch_divergences
		WHERE fork_run_id = $1::uuid
	`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract branch divergence: %w", err)
	}
	if catalog.hasColumns(runForkSelectedContractRouteRecoveryTable, "fork_run_id") {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM run_fork_selected_contract_route_recoveries
			WHERE fork_run_id = $1::uuid
		`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract route recovery: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
	`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract execution lineage: %w", err)
	}
	if catalog.hasColumns(runForkDeliveryEventReplayTable, "fork_run_id") {
		if _, err := tx.ExecContext(ctx, `DELETE FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork replay lineage: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM event_receipts
		WHERE event_id IN (SELECT event_id FROM events WHERE run_id = $1::uuid)
	`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork receipts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM dead_letters
		WHERE original_event_id IN (SELECT event_id FROM events WHERE run_id = $1::uuid)
	`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork dead letters: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM event_deliveries
		WHERE run_id = $1::uuid
		   OR event_id IN (SELECT event_id FROM events WHERE run_id = $1::uuid)
	`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork deliveries: %w", err)
	}
	if catalog.hasColumns("timers", "run_id") {
		if _, err := tx.ExecContext(ctx, `DELETE FROM timers WHERE run_id = $1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork timers: %w", err)
		}
	}
	if catalog.hasColumns("activity_attempts", "run_id") {
		if _, err := tx.ExecContext(ctx, `DELETE FROM activity_attempts WHERE run_id = $1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork activity evidence: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM entity_mutations WHERE run_id = $1::uuid`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork mutations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE run_id = $1::uuid`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM entity_state WHERE run_id = $1::uuid`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork entity state: %w", err)
	}
	if preserveCompletionEvidence {
		if _, err := tx.ExecContext(ctx, `DELETE FROM run_fork_fact_revisions WHERE run_id=$1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork fact revisions: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM run_fork_revisions WHERE run_id=$1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork revision ledger: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM run_fork_revision_heads WHERE run_id=$1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork revision head: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET status='cancelled', ended_at=NOW() WHERE run_id=$1::uuid AND status=$2`, forkRunID, RunForkMaterializedStatus); err != nil {
			return fmt.Errorf("retain selected-contract completion run tombstone: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `DELETE FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork binding: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM runs WHERE run_id = $1::uuid AND status = $2`, forkRunID, RunForkMaterializedStatus); err != nil {
			return fmt.Errorf("delete selected-contract fork run: %w", err)
		}
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return fmt.Errorf("commit selected-contract fork discard: %w", err)
	}
	committed = true
	return nil
}

func (s *PostgresStore) LoadRunForkSelectedContractSourceEvents(ctx context.Context, sourceRunID, forkRunID string, sourceEventIDs []string) ([]RunForkSelectedContractSourceEvent, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return nil, fmt.Errorf("source run_id is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return nil, fmt.Errorf("fork run_id is required")
	}
	ids := uniqueNonEmptyStrings(sourceEventIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("begin selected-contract source event preparation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	rows, err := tx.QueryContext(ctx, `
		SELECT
			event_id::text,
			event_name,
			COALESCE(entity_id::text, ''),
			COALESCE(flow_instance, ''),
			COALESCE(scope, ''),
			COALESCE(payload, '{}'::jsonb)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = ANY($2::uuid[])
		ORDER BY created_at ASC, event_id ASC
	`, sourceRunID, pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("load selected-contract source events: %w", err)
	}
	out := make([]RunForkSelectedContractSourceEvent, 0, len(ids))
	for rows.Next() {
		var event RunForkSelectedContractSourceEvent
		if err := rows.Scan(&event.SourceEventID, &event.EventName, &event.EntityID, &event.FlowInstance, &event.Scope, &event.Payload); err != nil {
			return nil, fmt.Errorf("scan selected-contract source event: %w", err)
		}
		if !json.Valid(event.Payload) {
			return nil, fmt.Errorf("selected-contract source event %s payload is not valid json", event.SourceEventID)
		}
		out = append(out, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read selected-contract source events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close selected-contract source events: %w", err)
	}
	if len(out) != len(ids) {
		return nil, fmt.Errorf("selected-contract source event lookup returned %d rows for %d requested events", len(out), len(ids))
	}
	for idx := range out {
		prepared, err := prepareRunForkSelectedContractSourceEvent(ctx, tx, forkRunID, out[idx])
		if err != nil {
			return nil, err
		}
		out[idx] = prepared
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return nil, fmt.Errorf("commit selected-contract source event preparation: %w", err)
	}
	committed = true
	return out, nil
}

func (s *PostgresStore) RecordRunForkSelectedContractExecutionLineage(ctx context.Context, lineage RunForkSelectedContractExecutionLineage) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	if _, err := s.requireRunForkSelectedContractExecutionCapabilities(ctx); err != nil {
		return err
	}
	createdAt := lineage.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_executions (
			fork_run_id, source_run_id, source_event_id, fork_event_id, event_name, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5, $6)
		ON CONFLICT (fork_run_id, source_event_id) DO NOTHING
	`, lineage.ForkRunID, lineage.SourceRunID, lineage.SourceEventID, lineage.ForkEventID, lineage.EventName, createdAt)
	if err != nil {
		return fmt.Errorf("record selected-contract execution lineage: %w", err)
	}
	return nil
}

func runForkSelectedContractBranchSourceStatusSupported(status string) bool {
	switch strings.TrimSpace(status) {
	case "running", "paused", "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func collectRunForkSelectedContractSourceAdvancedFacts(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage) ([]string, error) {
	facts, err := collectRunForkSourceAdvancedFacts(ctx, tx, lineage)
	if err != nil {
		return nil, err
	}
	switch strings.TrimSpace(lineage.SourceRunStatus) {
	case "completed", "failed", "cancelled":
		facts = append(facts, "source_run_terminal_at_activation")
	}
	return uniqueNonEmptyStrings(facts), nil
}

func insertRunForkSelectedContractBranchDivergence(ctx context.Context, tx *sql.Tx, divergence RunForkSelectedContractBranchDivergence) error {
	if divergence.CreatedAt.IsZero() {
		divergence.CreatedAt = time.Now().UTC()
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_branch_divergences (
			fork_run_id,
			source_run_id,
			fork_event_id,
			owner,
			policy,
			source_run_status_at_activation,
			source_run_status_after_activation,
			source_frozen,
			source_advanced_facts,
			created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9::text[], $10)
		ON CONFLICT (fork_run_id) DO UPDATE
		SET owner = EXCLUDED.owner,
		    policy = EXCLUDED.policy,
		    source_run_status_at_activation = EXCLUDED.source_run_status_at_activation,
		    source_run_status_after_activation = EXCLUDED.source_run_status_after_activation,
		    source_frozen = EXCLUDED.source_frozen,
		    source_advanced_facts = EXCLUDED.source_advanced_facts,
		    created_at = EXCLUDED.created_at
	`, divergence.ForkRunID,
		divergence.SourceRunID,
		divergence.ForkEventID,
		divergence.Owner,
		divergence.Policy,
		divergence.SourceRunStatusAtActivation,
		divergence.SourceRunStatusAfterActivation,
		divergence.SourceFrozen,
		pq.Array(divergence.SourceAdvancedFacts),
		divergence.CreatedAt)
	if err != nil {
		return fmt.Errorf("record selected-contract branch divergence: %w", err)
	}
	return nil
}

func runForkSelectedContractExecutionPlanBlockers(plan RunForkPlan, allowedSourceEventIDs []string) []RunForkUnsupportedBlocker {
	return runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, RunForkSelectedContractReplayResumeAdmission(plan), allowedSourceEventIDs)
}

func runForkSelectedContractExecutionPlanBlockersFromAdmission(plan RunForkPlan, admission RunForkReplayResumeAdmission, allowedSourceEventIDs []string) []RunForkUnsupportedBlocker {
	allowedEvents := map[string]struct{}{}
	for _, eventID := range allowedSourceEventIDs {
		if eventID = strings.TrimSpace(eventID); eventID != "" {
			allowedEvents[eventID] = struct{}{}
		}
	}
	blockers := []RunForkUnsupportedBlocker{}
	for _, blocker := range admission.UnsupportedBlockers {
		switch strings.TrimSpace(blocker.Code) {
		case RunForkBlockerDeliveryHistoryUnproven, RunForkBlockerNonAgentDeliveryReplayUnsupported:
			continue
		default:
			blockers = appendRunForkBlocker(blockers, blocker)
		}
	}
	for _, item := range plan.PendingWork {
		classification := strings.TrimSpace(item.Classification)
		if classification == RunForkPendingClassificationDeliveredCompleted {
			continue
		}
		if RunForkSelectedContractDiagnosticPlatformOutcomePolicyApplies(item) {
			continue
		}
		if classification == RunForkPendingClassificationCommittedReplay {
			if runForkSelectedContractCommittedReplayScopeMarkerAdmitted(admission) {
				continue
			}
			blockers = appendRunForkBlocker(blockers, runForkReplayResumeBlocker(RunForkBlockerCommittedReplayScopeReplayUnsupported))
			continue
		}
		if runForkSelectedContractActiveSourceDeliveryConversationCouplingAdmitted(plan, item) {
			continue
		}
		if runForkSelectedContractPendingWorkHasActiveDeliverySessionCoupling(item) {
			if blocker, ok := runForkSelectedContractAdmissionBlockerForPendingWork(admission, item); ok {
				blockers = appendRunForkBlocker(blockers, blocker)
			} else {
				blockers = appendRunForkBlocker(blockers, runForkReplayResumeBlocker(RunForkBlockerDeliveryHistoryUnproven))
			}
			continue
		}
		if len(allowedEvents) == 0 {
			continue
		}
		if _, ok := allowedEvents[strings.TrimSpace(item.EventID)]; !ok {
			blockers = appendRunForkBlocker(blockers, RunForkUnsupportedBlocker{
				Code:    RunForkBlockerDeliveryHistoryUnproven,
				Message: "selected-contract execution cannot absorb pending source delivery outside selected frontier evidence",
			})
		}
	}
	return blockers
}

func runForkSelectedContractAdmissionBlockerForPendingWork(admission RunForkReplayResumeAdmission, item RunForkPendingWork) (RunForkUnsupportedBlocker, bool) {
	key := runForkSelectedContractPendingWorkKey(item)
	for _, disposition := range admission.Dispositions {
		if strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionFailClosedBlocker {
			continue
		}
		if runForkSelectedContractDispositionKey(disposition) != key {
			continue
		}
		if code := strings.TrimSpace(disposition.BlockerCode); code != "" {
			return runForkReplayResumeBlocker(code), true
		}
	}
	return RunForkUnsupportedBlocker{}, false
}

func (s *PostgresStore) EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx context.Context, sourceRunID, forkEventID string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := runforkrevision.ValidateComplete(ctx, tx, sourceRunID); err != nil {
		return err
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, `
		SELECT MIN(revision)
		FROM run_fork_fact_revisions
		WHERE run_id = $1::uuid
		  AND family = 'events'
		  AND fact_key = $2
		  AND present
	`, sourceRunID, forkEventID).Scan(&revision); err != nil {
		return fmt.Errorf("resolve committed replay-scope fork revision: %w", err)
	}
	if revision <= 0 {
		return fmt.Errorf("committed replay-scope fork event is not revisioned; recreate the store and retry")
	}
	return ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(ctx, tx, sourceRunID, revision)
}

func ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(ctx context.Context, q timerReconstructionQueryer, sourceRunID string, forkRevision int64) error {
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries d
			JOIN LATERAL (
				SELECT MAX(revision) AS revision
				FROM run_fork_fact_revisions
				WHERE run_id = d.run_id
				  AND family = 'event_deliveries'
				  AND fact_key = d.delivery_id::text
				  AND present
			) history ON TRUE
			WHERE d.run_id = $1::uuid
			  AND d.subscriber_type = $2
			  AND d.subscriber_id = $3
			  AND d.reason_code = ANY($4::text[])
			  AND history.revision > $5
		)
	`
	if err := q.QueryRowContext(ctx, query, sourceRunID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, pq.Array(runForkReplayScopeMarkerReasonCodes()), forkRevision).Scan(&exists); err != nil {
		return fmt.Errorf("check selected-contract source_committed_replay_scope_advanced_after_fork_point: %w", err)
	}
	if exists {
		code := "source_committed_replay_scope_advanced_after_fork_point"
		return runForkReplayResumeError(code, RunForkReplayResumeFactSourceAdvanced, fmt.Sprintf("selected-contract committed replay-scope marker policy blocked: %s", code))
	}
	return nil
}

func runForkReplayScopeMarkerReasonCodes() []string {
	return []string{replayScopeReasonDirect, replayScopeReasonSubscribed}
}

func runForkSelectedContractConversationAdvancedFacts(facts []string) []string {
	out := []string{}
	for _, fact := range facts {
		switch strings.TrimSpace(fact) {
		case "source_sessions_advanced_after_fork_point", "source_conversation_audits_advanced_after_fork_point", "source_turns_advanced_after_fork_point":
			out = append(out, fact)
		}
	}
	return uniqueNonEmptyStrings(out)
}

func ensureRunForkNoPostForkActiveConversationDeliverySessionCoupling(ctx context.Context, q timerReconstructionQueryer, lineage runForkActivationLineage) error {
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries d
			JOIN LATERAL (
				SELECT MAX(revision) AS revision
				FROM run_fork_fact_revisions
				WHERE run_id = d.run_id
				  AND family = 'event_deliveries'
				  AND fact_key = d.delivery_id::text
				  AND present
			) history ON TRUE
			WHERE d.run_id = $1::uuid
			  AND history.revision > $2
			  AND (
					d.status = 'in_progress'
					OR d.active_session_id IS NOT NULL
					OR (d.started_at IS NOT NULL AND d.delivered_at IS NULL)
			  )
		)
	`
	if err := q.QueryRowContext(ctx, query, lineage.SourceRunID, lineage.ForkEventRevision).Scan(&exists); err != nil {
		return fmt.Errorf("check selected-contract source_active_conversation_session_coupling_after_fork_point: %w", err)
	}
	if exists {
		code := "source_active_conversation_session_coupling_after_fork_point"
		return runForkReplayResumeError(code, RunForkReplayResumeFactSessionHistory, fmt.Sprintf("%s blocked unsafe post-T active source delivery/session coupling: %s", RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner, code))
	}
	return nil
}

func runForkReplayResumeAdmissionWithSourceAdvancedConversationHistory(admission RunForkReplayResumeAdmission, facts []string) RunForkReplayResumeAdmission {
	if len(facts) == 0 {
		return admission
	}
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = RunForkReplayResumeAdmissionOwner
	}
	seen := map[string]struct{}{}
	for _, disposition := range admission.Dispositions {
		if strings.TrimSpace(disposition.Fact) != RunForkReplayResumeFactSourceAdvanced ||
			strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionLineageOnly ||
			strings.TrimSpace(disposition.Owner) != RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner {
			continue
		}
		seen[strings.TrimSpace(disposition.Classification)] = struct{}{}
	}
	for _, fact := range uniqueNonEmptyStrings(facts) {
		if _, ok := seen[fact]; ok {
			continue
		}
		admission.Dispositions = append(admission.Dispositions, RunForkReplayResumeDisposition{
			Fact:           RunForkReplayResumeFactSourceAdvanced,
			Disposition:    RunForkReplayResumeDispositionLineageOnly,
			Owner:          RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner,
			Classification: fact,
			Message:        fmt.Sprintf("%s classifies post-T source conversation-history fact %s as selected-contract branch-divergence lineage only; fresh fork-local conversation rows must be created by normal runtime execution under the fork run_id", RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner, fact),
		})
	}
	return admission
}

func ensureRunForkSelectedContractExecutionForkState(ctx context.Context, tx *sql.Tx, catalog schemaColumnCatalog, forkRunID string, allowedSourceEventIDs []string) error {
	allowedEvents := uniqueNonEmptyStrings(allowedSourceEventIDs)
	if len(allowedEvents) == 0 {
		return ensureRunForkActivationNoForkReplayState(ctx, tx, catalog, forkRunID)
	}

	// Materialization preflights empty fork-local replay state. At activation
	// time, sessions/turns/audits may be fresh outputs from selected execution.
	var missingLineage int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM unnest($2::uuid[]) AS allowed(source_event_id)
		WHERE NOT EXISTS (
			SELECT 1
			FROM run_fork_selected_contract_executions x
			WHERE x.fork_run_id = $1::uuid
			  AND x.source_event_id = allowed.source_event_id
		)
	`, forkRunID, pq.Array(allowedEvents)).Scan(&missingLineage); err != nil {
		return fmt.Errorf("check selected-contract execution lineage completeness: %w", err)
	}
	if missingLineage > 0 {
		return runForkReplayResumeError("fork_selected_contract_execution_lineage_missing", RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_selected_contract_execution_lineage_missing")
	}

	var strayEvents int
	if err := tx.QueryRowContext(ctx, `
		WITH RECURSIVE selected_agents AS (
			SELECT DISTINCT d.subscriber_id AS agent_id
			FROM event_deliveries d
			INNER JOIN run_fork_selected_contract_executions x
				ON x.fork_event_id = d.event_id
			   AND x.fork_run_id = $1::uuid
			   AND x.source_event_id = ANY($2::uuid[])
			WHERE d.run_id = $1::uuid
			  AND d.subscriber_type = 'agent'
		),
		selected_tree AS (
			SELECT e.event_id
			FROM events e
			INNER JOIN run_fork_selected_contract_executions x
				ON x.fork_event_id = e.event_id
			   AND x.fork_run_id = $1::uuid
			   AND x.source_event_id = ANY($2::uuid[])
			WHERE e.run_id = $1::uuid
			UNION
			SELECT e.event_id
			FROM events e
			INNER JOIN selected_agents a ON a.agent_id = e.produced_by
			WHERE e.run_id = $1::uuid
			  AND e.produced_by_type = 'agent'
			UNION
			SELECT child.event_id
			FROM events child
			INNER JOIN selected_tree parent ON child.source_event_id = parent.event_id
			WHERE child.run_id = $1::uuid
			  AND (
				child.event_name NOT LIKE 'platform.%'
				OR child.event_name = ANY($3::text[])
			  )
		)
		SELECT COUNT(*)
		FROM events e
		WHERE e.run_id = $1::uuid
		  AND NOT EXISTS (
			SELECT 1 FROM selected_tree tree WHERE tree.event_id = e.event_id
		  )
	`, forkRunID, pq.Array(allowedEvents), pq.Array(runForkSelectedContractForkLocalRuntimePlatformEventNames())).Scan(&strayEvents); err != nil {
		return fmt.Errorf("check selected-contract fork event lineage: %w", err)
	}
	if strayEvents > 0 {
		return runForkReplayResumeError("fork_events_not_selected_contract_lineage", RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_events_not_selected_contract_lineage")
	}

	var strayDeliveries int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries d
		WHERE d.run_id = $1::uuid
		  AND NOT EXISTS (
			SELECT 1 FROM events e WHERE e.run_id = $1::uuid AND e.event_id = d.event_id
		  )
	`, forkRunID).Scan(&strayDeliveries); err != nil {
		return fmt.Errorf("check selected-contract fork deliveries: %w", err)
	}
	if strayDeliveries > 0 {
		return runForkReplayResumeError("fork_deliveries_not_selected_contract_lineage", RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_deliveries_not_selected_contract_lineage")
	}
	return nil
}

func runForkSelectedContractForkLocalRuntimePlatformEventNames() []string {
	// runtime.run_fork.selected_contract_execution.fork_local_runtime_platform_event_lineage_policy:
	// fresh runtime platform/control outputs are fork-local lineage only when
	// they remain causally parented to selected-fork execution.
	return []string{
		"platform.agent_failed",
		"platform.agent_panic",
		"platform.agent_started",
		"platform.auth_required",
		"platform.budget_threshold_crossed",
		"platform.dead_letter_escalation",
		"platform.event_quarantined",
		"platform.paused",
		"platform.resumed",
		"platform.run_stalled",
		"platform.runtime_log",
	}
}

func uniqueNonEmptyStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
