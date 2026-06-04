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
}

type RunForkSelectedContractExecutionActivateRequest struct {
	ForkRunID             string
	AllowedSourceEventIDs []string
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
	conversationAdvancedFacts, err := collectRunForkSelectedContractSourceAdvancedConversationHistoryFacts(ctx, s.DB, catalog, plan.SourceRunID, plan.ForkPoint.Timestamp)
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
	replayAdmission = runForkReplayResumeAdmissionWithSourceAdvancedConversationHistory(replayAdmission, conversationAdvancedFacts)
	if err := ensureRunForkNoPostForkCommittedReplayScopeMarkers(ctx, s.DB, catalog, plan.SourceRunID, plan.ForkPoint.EventID, plan.ForkPoint.Timestamp); err != nil {
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

	forkRunID := deterministicRunForkMaterializationID(plan.SourceRunID, plan.ForkPoint.EventID)
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
	if err := insertRunForkSelectedContractTimerReconstructions(ctx, tx, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID, timerReconstruction, now); err != nil {
		return RunForkMaterialization{}, err
	}
	if err := tx.Commit(); err != nil {
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
	result := RunForkActivation{
		SourceRunID:             lineage.SourceRunID,
		ForkRunID:               lineage.ForkRunID,
		ForkRunStatus:           lineage.ForkStatus,
		SourceRunStatus:         lineage.SourceRunStatus,
		ForkPoint:               RunForkPoint{Input: lineage.ForkEventID, EventID: lineage.ForkEventID, EventName: lineage.ForkEventName, Timestamp: lineage.ForkEventTime},
		HistoricalReplayBlocked: true,
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
	timerResolved, err := runForkSelectedContractTimerReconstructionComplete(ctx, tx, lineage, plan)
	if err != nil {
		return result, err
	}
	result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithTimerReconstruction(result.ReplayResumeAdmission, runForkTimerReconstructionPlan{Required: timerResolved})
	if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, result.ReplayResumeAdmission, req.AllowedSourceEventIDs); len(blockers) > 0 {
		result.UnsupportedBlockers = blockers
		return result, fmt.Errorf("selected-contract fork activation blocked: %s", runForkBlockerCodes(blockers))
	}
	conversationAdvancedFacts, err := collectRunForkSelectedContractSourceAdvancedConversationHistoryFacts(ctx, tx, catalog, lineage.SourceRunID, lineage.ForkEventTime)
	if err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
	}
	result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithSourceAdvancedConversationHistory(result.ReplayResumeAdmission, conversationAdvancedFacts)
	if err := ensureRunForkNoPostForkCommittedReplayScopeMarkers(ctx, tx, catalog, lineage.SourceRunID, lineage.ForkEventID, lineage.ForkEventTime); err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
	}
	sourceAdvancedAdmissionFacts := append([]string{}, conversationAdvancedFacts...)
	sourceAdvancedAdmissionFacts = append(sourceAdvancedAdmissionFacts, runForkSelectedContractActiveSourceDeliveryConversationCouplingFacts(result.ReplayResumeAdmission)...)
	sourceAdvancedFacts, err := collectRunForkSelectedContractSourceAdvancedFacts(ctx, tx, catalog, lineage, sourceAdvancedAdmissionFacts)
	if err != nil {
		return result, err
	}
	if len(sourceAdvancedFacts) > 0 {
		result.SourceAdvancedAfterFork = true
	}
	if err := ensureRunForkNoRelevantPostForkTimers(ctx, tx, catalog, lineage); err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
	}
	if err := ensureRunForkNoRelevantPostForkRoutes(ctx, tx, catalog, lineage); err != nil {
		if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
			result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
			result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
		}
		return result, err
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
		if err := tx.Commit(); err != nil {
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
	if err := tx.Commit(); err != nil {
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

	if catalog.hasColumns("agent_turns", "run_id") {
		if _, err := tx.ExecContext(ctx, `DELETE FROM agent_turns WHERE run_id = $1::uuid`, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork turns: %w", err)
		}
	}
	if catalog.hasColumns("agent_conversation_audits", "run_id") {
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM entity_mutations WHERE run_id = $1::uuid`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork mutations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE run_id = $1::uuid`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM entity_state WHERE run_id = $1::uuid`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork entity state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1::uuid`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-contract fork binding: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM runs WHERE run_id = $1::uuid AND status = $2`, forkRunID, RunForkMaterializedStatus); err != nil {
		return fmt.Errorf("delete selected-contract fork run: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit selected-contract fork discard: %w", err)
	}
	committed = true
	return nil
}

func (s *PostgresStore) LoadRunForkSelectedContractSourceEvents(ctx context.Context, sourceRunID string, sourceEventIDs []string) ([]RunForkSelectedContractSourceEvent, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return nil, fmt.Errorf("source run_id is required")
	}
	ids := uniqueNonEmptyStrings(sourceEventIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
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
	defer rows.Close()
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
		return nil, fmt.Errorf("read selected-contract source events: %w", err)
	}
	if len(out) != len(ids) {
		return nil, fmt.Errorf("selected-contract source event lookup returned %d rows for %d requested events", len(out), len(ids))
	}
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

func collectRunForkSelectedContractSourceAdvancedFacts(ctx context.Context, tx *sql.Tx, catalog schemaColumnCatalog, lineage runForkActivationLineage, conversationAdvancedFacts []string) ([]string, error) {
	checks := []struct {
		code    string
		enabled bool
		query   string
		args    []any
	}{
		{
			code:    "source_events_advanced_after_fork_point",
			enabled: true,
			query: `
				SELECT EXISTS (
					SELECT 1 FROM events
					WHERE run_id = $1::uuid
					  AND (created_at, event_id) > ($2::timestamptz, $3::uuid)
				)
			`,
			args: []any{lineage.SourceRunID, lineage.ForkEventTime, lineage.ForkEventID},
		},
		{
			code:    "source_mutations_advanced_after_fork_point",
			enabled: true,
			query: `
				SELECT EXISTS (
					SELECT 1 FROM entity_mutations
					WHERE run_id = $1::uuid
					  AND created_at > $2::timestamptz
				)
			`,
			args: []any{lineage.SourceRunID, lineage.ForkEventTime},
		},
		{
			code:    "source_current_state_advanced_after_fork_point",
			enabled: true,
			query: `
				SELECT EXISTS (
					SELECT 1 FROM entity_state
					WHERE run_id = $1::uuid
					  AND updated_at > $2::timestamptz
				)
			`,
			args: []any{lineage.SourceRunID, lineage.ForkEventTime},
		},
		{
			code:    "source_deliveries_advanced_after_fork_point",
			enabled: true,
			query: `
				SELECT EXISTS (
					SELECT 1
					FROM event_deliveries d
					LEFT JOIN events e ON e.event_id = d.event_id
					   AND e.run_id = d.run_id
					WHERE d.run_id = $1::uuid
					  AND (
							d.created_at > $2::timestamptz
							OR d.started_at > $2::timestamptz
							OR d.delivered_at > $2::timestamptz
					  )
					  AND NOT (
							d.subscriber_type = $4
							AND d.subscriber_id = $5
							AND d.reason_code = ANY($6::text[])
							AND e.event_id IS NOT NULL
							AND (e.created_at, e.event_id) <= ($2::timestamptz, $3::uuid)
					  )
				)
			`,
			args: []any{
				lineage.SourceRunID,
				lineage.ForkEventTime,
				lineage.ForkEventID,
				replayScopeMarkerSubscriberType,
				replayScopeMarkerSubscriberID,
				pq.Array(runForkReplayScopeMarkerReasonCodes()),
			},
		},
		{
			code:    "source_receipts_advanced_after_fork_point",
			enabled: catalog.hasColumns("event_receipts", "event_id", "processed_at") && catalog.hasColumns("events", "event_id", "run_id", "created_at"),
			query: `
				SELECT EXISTS (
					SELECT 1
					FROM event_receipts r
					INNER JOIN events e ON e.event_id = r.event_id
					WHERE e.run_id = $1::uuid
					  AND (
							r.processed_at > $2::timestamptz
							OR (e.created_at, e.event_id) > ($2::timestamptz, $3::uuid)
					  )
				)
			`,
			args: []any{lineage.SourceRunID, lineage.ForkEventTime, lineage.ForkEventID},
		},
		{
			code:    "source_dead_letters_advanced_after_fork_point",
			enabled: catalog.hasColumns("dead_letters", "original_event_id", "created_at") && catalog.hasColumns("events", "event_id", "run_id", "created_at"),
			query: `
				SELECT EXISTS (
					SELECT 1
					FROM dead_letters dl
					INNER JOIN events e ON e.event_id = dl.original_event_id
					WHERE e.run_id = $1::uuid
					  AND (
							dl.created_at > $2::timestamptz
							OR (e.created_at, e.event_id) > ($2::timestamptz, $3::uuid)
					  )
				)
			`,
			args: []any{lineage.SourceRunID, lineage.ForkEventTime, lineage.ForkEventID},
		},
	}
	facts := []string{}
	switch strings.TrimSpace(lineage.SourceRunStatus) {
	case "completed", "failed", "cancelled":
		facts = append(facts, "source_run_terminal_at_activation")
	}
	facts = append(facts, conversationAdvancedFacts...)
	for _, check := range checks {
		if !check.enabled {
			continue
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, check.query, check.args...).Scan(&exists); err != nil {
			return nil, fmt.Errorf("check selected-contract branch %s: %w", check.code, err)
		}
		if exists {
			facts = append(facts, check.code)
		}
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

func (s *PostgresStore) EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx context.Context, sourceRunID, forkEventID string, forkTime time.Time) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	return ensureRunForkNoPostForkCommittedReplayScopeMarkers(ctx, s.DB, catalog, sourceRunID, forkEventID, forkTime)
}

func ensureRunForkNoPostForkCommittedReplayScopeMarkers(ctx context.Context, q timerReconstructionQueryer, catalog schemaColumnCatalog, sourceRunID, forkEventID string, forkTime time.Time) error {
	if !catalog.hasColumns("event_deliveries", "run_id", "event_id", "subscriber_type", "subscriber_id", "reason_code") {
		return nil
	}
	if !catalog.hasColumns("events", "event_id", "run_id", "created_at") {
		return nil
	}
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries d
			LEFT JOIN events e ON e.event_id = d.event_id
			   AND e.run_id = d.run_id
			WHERE d.run_id = $1::uuid
			  AND d.subscriber_type = $2
			  AND d.subscriber_id = $3
			  AND d.reason_code = ANY($4::text[])
			  AND (
					e.event_id IS NULL
					OR (e.created_at, e.event_id) > ($5::timestamptz, $6::uuid)
			  )
		)
	`
	if err := q.QueryRowContext(ctx, query, sourceRunID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, pq.Array(runForkReplayScopeMarkerReasonCodes()), forkTime, forkEventID).Scan(&exists); err != nil {
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

func collectRunForkSelectedContractSourceAdvancedConversationHistoryFacts(ctx context.Context, q timerReconstructionQueryer, catalog schemaColumnCatalog, sourceRunID string, forkTime time.Time) ([]string, error) {
	if err := ensureRunForkNoPostForkActiveConversationDeliverySessionCoupling(ctx, q, catalog, sourceRunID, forkTime); err != nil {
		return nil, err
	}
	checks := []struct {
		code       string
		table      string
		predicates []string
	}{
		{
			code:       "source_sessions_advanced_after_fork_point",
			table:      "agent_sessions",
			predicates: runForkPostForkConversationPredicates(catalog, "agent_sessions", "created_at", "updated_at", "terminated_at"),
		},
		{
			code:       "source_conversation_audits_advanced_after_fork_point",
			table:      "agent_conversation_audits",
			predicates: runForkPostForkConversationPredicates(catalog, "agent_conversation_audits", "created_at", "updated_at"),
		},
		{
			code:       "source_turns_advanced_after_fork_point",
			table:      "agent_turns",
			predicates: runForkPostForkConversationPredicates(catalog, "agent_turns", "created_at"),
		},
	}
	facts := []string{}
	for _, check := range checks {
		if len(check.predicates) == 0 || !catalog.hasColumns(check.table, "run_id") {
			continue
		}
		var exists bool
		query := fmt.Sprintf(`
			SELECT EXISTS (
				SELECT 1
				FROM %s
				WHERE run_id = $1::uuid
				  AND (%s)
			)
		`, check.table, strings.Join(check.predicates, " OR "))
		if err := q.QueryRowContext(ctx, query, sourceRunID, forkTime).Scan(&exists); err != nil {
			return nil, fmt.Errorf("check selected-contract %s: %w", check.code, err)
		}
		if exists {
			facts = append(facts, check.code)
		}
	}
	return uniqueNonEmptyStrings(facts), nil
}

func ensureRunForkNoPostForkActiveConversationDeliverySessionCoupling(ctx context.Context, q timerReconstructionQueryer, catalog schemaColumnCatalog, sourceRunID string, forkTime time.Time) error {
	if !catalog.hasColumns("event_deliveries", "run_id", "status") {
		return nil
	}
	postForkPredicates := []string{}
	for _, column := range []string{"created_at", "started_at", "delivered_at"} {
		if catalog.hasColumns("event_deliveries", column) {
			postForkPredicates = append(postForkPredicates, fmt.Sprintf("d.%s > $2::timestamptz", column))
		}
	}
	if len(postForkPredicates) == 0 {
		return nil
	}
	couplingPredicates := []string{"d.status = 'in_progress'"}
	if catalog.hasColumns("event_deliveries", "active_session_id") {
		couplingPredicates = append(couplingPredicates, "d.active_session_id IS NOT NULL")
	}
	if catalog.hasColumns("event_deliveries", "started_at", "delivered_at") {
		couplingPredicates = append(couplingPredicates, "(d.started_at IS NOT NULL AND d.delivered_at IS NULL)")
	}
	var exists bool
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries d
			WHERE d.run_id = $1::uuid
			  AND (%s)
			  AND (%s)
		)
	`, strings.Join(postForkPredicates, " OR "), strings.Join(couplingPredicates, " OR "))
	if err := q.QueryRowContext(ctx, query, sourceRunID, forkTime).Scan(&exists); err != nil {
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

func runForkPostForkConversationPredicates(catalog schemaColumnCatalog, table string, columns ...string) []string {
	predicates := []string{}
	for _, column := range columns {
		if catalog.hasColumns(table, column) {
			predicates = append(predicates, fmt.Sprintf("%s > $2::timestamptz", column))
		}
	}
	return predicates
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
