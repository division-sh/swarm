package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

const (
	RunForkSelectedContractExecutionLineageOwner = "store.run_fork.selected_contract_execution_lineage"
	runForkSelectedContractExecutionLineageTable = "run_fork_selected_contract_executions"
)

type RunForkSelectedContractExecutionMaterializeRequest struct {
	SourceRunID       string
	At                string
	ContractSelection RunForkContractSelection
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

func RequireRunForkSelectedContractExecutionCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	if err := RequireRunForkActivationCapabilities(caps, catalog); err != nil {
		return err
	}
	required := map[string][]string{
		runForkSelectedContractExecutionLineageTable: {"fork_run_id", "source_run_id", "source_event_id", "fork_event_id", "event_name", "created_at"},
	}
	for tableName, columns := range required {
		if catalog.hasColumns(tableName, columns...) {
			continue
		}
		return fmt.Errorf("selected-contract fork execution requires %s columns %v", tableName, columns)
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
	if blockers := runForkSelectedContractExecutionPlanBlockers(plan, nil); len(blockers) > 0 {
		return RunForkMaterialization{
			SourceRunID:           plan.SourceRunID,
			ForkPoint:             plan.ForkPoint,
			ExecutionReady:        false,
			ReplayResumeAdmission: plan.ReplayResumeAdmission,
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
	metadata, err := loadRunForkEntityMetadata(ctx, tx, plan)
	if err != nil {
		return RunForkMaterialization{}, err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, forked_from_run_id, forked_from_event_id,
			entity_count, event_count, started_at
		)
		VALUES (
			$1::uuid, $2, $3::uuid, $4::uuid,
			$5, 0, $6
		)
	`, forkRunID, RunForkMaterializedStatus, plan.SourceRunID, plan.ForkPoint.EventID, len(plan.Entities), now); err != nil {
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
	if err := tx.Commit(); err != nil {
		return RunForkMaterialization{}, fmt.Errorf("commit selected-contract fork materialization: %w", err)
	}
	committed = true
	return RunForkMaterialization{
		SourceRunID:              plan.SourceRunID,
		ForkRunID:                forkRunID,
		ForkRunStatus:            RunForkMaterializedStatus,
		ForkPoint:                plan.ForkPoint,
		MaterializedEntityCount:  len(plan.Entities),
		ExecutionReady:           false,
		ReplayResumeAdmission:    plan.ReplayResumeAdmission,
		SelectedContractBinding:  &binding,
		UnsupportedBlockers:      plan.UnsupportedBlockers,
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
	if lineage.SourceRunStatus != "running" && lineage.SourceRunStatus != "paused" {
		return result, fmt.Errorf("selected-contract fork activation requires source run status running or paused before freeze; got %q", lineage.SourceRunStatus)
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
	result.ReplayResumeAdmission = plan.ReplayResumeAdmission
	if blockers := runForkSelectedContractExecutionPlanBlockers(plan, req.AllowedSourceEventIDs); len(blockers) > 0 {
		result.UnsupportedBlockers = blockers
		return result, fmt.Errorf("selected-contract fork activation blocked: %s", runForkBlockerCodes(blockers))
	}
	if err := ensureRunForkSourceNotAdvanced(ctx, tx, catalog, lineage); err != nil {
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

	now := time.Now().UTC()
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

func runForkSelectedContractExecutionPlanBlockers(plan RunForkPlan, allowedSourceEventIDs []string) []RunForkUnsupportedBlocker {
	allowedEvents := map[string]struct{}{}
	for _, eventID := range allowedSourceEventIDs {
		if eventID = strings.TrimSpace(eventID); eventID != "" {
			allowedEvents[eventID] = struct{}{}
		}
	}
	blockers := []RunForkUnsupportedBlocker{}
	for _, blocker := range plan.UnsupportedBlockers {
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
		if classification == RunForkPendingClassificationCommittedReplay {
			blockers = appendRunForkBlocker(blockers, runForkReplayResumeBlocker(RunForkBlockerCommittedReplayScopeReplayUnsupported))
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
