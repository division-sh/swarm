package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
)

const (
	RunForkActivatedStatus    = "running"
	RunForkSourceFrozenStatus = "forked"
)

type RunForkActivateRequest struct {
	ForkRunID string
}

type RunForkActivation struct {
	SourceRunID              string                            `json:"source_run_id"`
	ForkRunID                string                            `json:"fork_run_id"`
	ForkRunStatus            string                            `json:"fork_run_status"`
	SourceRunStatus          string                            `json:"source_run_status"`
	ForkPoint                RunForkPoint                      `json:"fork_point"`
	Activated                bool                              `json:"activated"`
	SourceFrozen             bool                              `json:"source_frozen"`
	HistoricalReplayBlocked  bool                              `json:"historical_replay_blocked"`
	ReplayResumeAdmission    RunForkReplayResumeAdmission      `json:"replay_resume_admission"`
	UnsupportedBlockers      []RunForkUnsupportedBlocker       `json:"unsupported_blockers,omitempty"`
	MaterializedEntityCount  int                               `json:"materialized_entity_count"`
	DeliveryEventReplay      *RunForkDeliveryEventReplayResult `json:"delivery_event_replay,omitempty"`
	SourceAdvancedAfterFork  bool                              `json:"source_advanced_after_fork_point,omitempty"`
	RepeatedActivationFailed bool                              `json:"repeated_activation_failed,omitempty"`
}

type runForkActivationLineage struct {
	ForkRunID       string
	ForkStatus      string
	SourceRunID     string
	ForkEventID     string
	ForkEventName   string
	ForkEventTime   time.Time
	SourceRunStatus string
	EntityIDs       []string
	FlowInstances   []string
	SourceFlows     []string
}

func RequireRunForkActivationCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	if err := RequireRunForkMaterializerCapabilities(caps, catalog); err != nil {
		return err
	}
	required := map[string][]string{
		"runs":                          {"run_id", "status", "forked_from_run_id", "forked_from_event_id", "ended_at"},
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
		return result, fmt.Errorf("fork activation requires materialized fork status %q; got %q", RunForkMaterializedStatus, lineage.ForkStatus)
	}
	if lineage.SourceRunStatus != "running" && lineage.SourceRunStatus != "paused" {
		return result, fmt.Errorf("fork activation requires source run status running or paused before freeze; got %q", lineage.SourceRunStatus)
	}
	if len(lineage.EntityIDs) == 0 {
		return result, fmt.Errorf("fork activation requires materialized fork entity_state rows")
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
		return result, fmt.Errorf("fork activation blocked: source_run_freeze_not_applied")
	}
	replayResult, err := applyRunForkDeliveryEventReplay(ctx, tx, lineage, plan, now)
	if err != nil {
		return result, err
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
		return result, fmt.Errorf("fork activation blocked: fork_run_activation_not_applied")
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit fork activation: %w", err)
	}
	committed = true
	result.ForkRunStatus = RunForkActivatedStatus
	result.SourceRunStatus = RunForkSourceFrozenStatus
	result.Activated = true
	result.SourceFrozen = true
	if replayResult.ReplayedEventCount > 0 || replayResult.ReplayedDeliveryCount > 0 {
		result.DeliveryEventReplay = &replayResult
	}
	return result, nil
}

func loadRunForkActivationLineage(ctx context.Context, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
	var lineage runForkActivationLineage
	var forkEventTime sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT
			f.run_id::text,
			COALESCE(f.status, ''),
			COALESCE(f.forked_from_run_id::text, ''),
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
		&lineage.SourceRunID,
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
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid FOR UPDATE`, lineage.SourceRunID).Scan(&lineage.SourceRunStatus); err != nil {
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
	return lineage, nil
}

func ensureRunForkSourceNotAdvanced(ctx context.Context, tx *sql.Tx, catalog schemaColumnCatalog, lineage runForkActivationLineage) error {
	checks := []struct {
		code  string
		query string
		args  []any
	}{
		{
			code: "source_events_advanced_after_fork_point",
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
			code: "source_mutations_advanced_after_fork_point",
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
			code: "source_current_state_advanced_after_fork_point",
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
			code: "source_deliveries_advanced_after_fork_point",
			query: `
				SELECT EXISTS (
					SELECT 1 FROM event_deliveries d
					WHERE d.run_id = $1::uuid
					  AND (
							d.created_at > $2::timestamptz
							OR d.started_at > $2::timestamptz
							OR d.delivered_at > $2::timestamptz
					  )
				)
			`,
			args: []any{lineage.SourceRunID, lineage.ForkEventTime},
		},
	}
	for _, check := range checks {
		var exists bool
		if err := tx.QueryRowContext(ctx, check.query, check.args...).Scan(&exists); err != nil {
			return fmt.Errorf("check %s: %w", check.code, err)
		}
		if exists {
			return runForkReplayResumeError(check.code, RunForkReplayResumeFactSourceAdvanced, fmt.Sprintf("fork activation blocked: %s", check.code))
		}
	}
	if err := ensureRunForkNoRelevantPostForkTimers(ctx, tx, catalog, lineage); err != nil {
		return err
	}
	if err := ensureRunForkNoRelevantPostForkRoutes(ctx, tx, catalog, lineage); err != nil {
		return err
	}
	return nil
}

func ensureRunForkNoRelevantPostForkTimers(ctx context.Context, tx *sql.Tx, catalog schemaColumnCatalog, lineage runForkActivationLineage) error {
	if !catalog.hasColumns("timers", "entity_id", "flow_instance", "created_at") {
		return nil
	}
	if len(lineage.EntityIDs) == 0 && len(lineage.FlowInstances) == 0 {
		return nil
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM timers
			WHERE created_at > $1::timestamptz
			  AND (
					(entity_id IS NOT NULL AND entity_id::text = ANY($2::text[]))
					OR
					(COALESCE(flow_instance, '') <> '' AND flow_instance = ANY($3::text[]))
			  )
		)
	`, lineage.ForkEventTime, pq.Array(lineage.EntityIDs), pq.Array(lineage.FlowInstances)).Scan(&exists); err != nil {
		return fmt.Errorf("check source timer advancement: %w", err)
	}
	if exists {
		return runForkReplayResumeError("source_timers_advanced_after_fork_point", RunForkReplayResumeFactSourceAdvanced, "fork activation blocked: source_timers_advanced_after_fork_point")
	}
	return nil
}

func ensureRunForkNoRelevantPostForkRoutes(ctx context.Context, tx *sql.Tx, catalog schemaColumnCatalog, lineage runForkActivationLineage) error {
	if !catalog.hasColumns("routing_rules", "flow_instance", "source_flow", "created_at") {
		return nil
	}
	if len(lineage.FlowInstances) == 0 && len(lineage.SourceFlows) == 0 {
		return nil
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM routing_rules
			WHERE created_at > $1::timestamptz
			  AND (
					(COALESCE(flow_instance, '') <> '' AND flow_instance = ANY($2::text[]))
					OR
					(COALESCE(source_flow, '') <> '' AND source_flow = ANY($3::text[]))
			  )
		)
	`, lineage.ForkEventTime, pq.Array(lineage.FlowInstances), pq.Array(lineage.SourceFlows)).Scan(&exists); err != nil {
		return fmt.Errorf("check source route advancement: %w", err)
	}
	if exists {
		return runForkReplayResumeError("source_routes_advanced_after_fork_point", RunForkReplayResumeFactSourceAdvanced, "fork activation blocked: source_routes_advanced_after_fork_point")
	}
	return nil
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
