package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

func TestSelectedContractExecutionMaterializationAllowsSelectedPendingNodeFrontier(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002400, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	_, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err == nil || !strings.Contains(err.Error(), RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("MaterializeRunFork error = %v, want non-agent blocker", err)
	}

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" || materialized.SelectedContractBinding == nil || !materialized.DeliveryResumeBlocked {
		t.Fatalf("materialization = %#v", materialized)
	}
	var replayRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`, materialized.ForkRunID).Scan(&replayRows); err != nil {
		t.Fatalf("count replay rows: %v", err)
	}
	if replayRows != 0 {
		t.Fatalf("delivery replay rows = %d, want selected execution materialization to avoid #570 replay", replayRows)
	}
}

func TestSelectedContractExecutionMaterializationPreflightsLineageCapability(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002450, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `DROP TABLE run_fork_selected_contract_executions`); err != nil {
		t.Fatalf("drop selected execution lineage table: %v", err)
	}

	_, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "run_fork_selected_contract_executions") {
		t.Fatalf("materialization error = %v, want lineage capability failure", err)
	}
	var strayForks int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
	`, sourceRunID).Scan(&strayForks); err != nil {
		t.Fatalf("count stray forks: %v", err)
	}
	if strayForks != 0 {
		t.Fatalf("stray materialized forks = %d, want 0", strayForks)
	}
}

func TestSelectedContractExecutionMaterializationPreflightsBranchDivergenceOwnerCapability(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002475, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `ALTER TABLE run_fork_selected_contract_branch_divergences DROP COLUMN owner`); err != nil {
		t.Fatalf("drop branch divergence owner column: %v", err)
	}

	_, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "run_fork_selected_contract_branch_divergences") || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("materialization error = %v, want branch divergence owner capability failure", err)
	}
	var strayForks int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
	`, sourceRunID).Scan(&strayForks); err != nil {
		t.Fatalf("count stray forks: %v", err)
	}
	if strayForks != 0 {
		t.Fatalf("stray materialized forks = %d, want 0", strayForks)
	}
}

func TestSelectedContractExecutionMaterializationPreflightsRouteRecoveryCapability(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002485, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `DROP TABLE run_fork_selected_contract_route_recoveries`); err != nil {
		t.Fatalf("drop selected route recovery table: %v", err)
	}

	_, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "run_fork_selected_contract_route_recoveries") {
		t.Fatalf("materialization error = %v, want route recovery capability failure", err)
	}
	var strayForks int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE forked_from_run_id = $1::uuid
	`, sourceRunID).Scan(&strayForks); err != nil {
		t.Fatalf("count stray forks: %v", err)
	}
	if strayForks != 0 {
		t.Fatalf("stray materialized forks = %d, want 0", strayForks)
	}
}

func TestSelectedContractExecutionMaterializationReconstructsActiveTimer(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002500, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload, fire_at, owner_agent, task_type, status, created_at)
		VALUES ($1::uuid, 'selected-timer', $2::uuid, 'flow-a/1', 'timer.selected', '{"source":true}'::jsonb, $3, 'agent-a', 'timer', 'active', $4)
	`, sourceRunID, entityID, at.Add(time.Hour), at); err != nil {
		t.Fatalf("seed timer: %v", err)
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	for _, blocker := range materialized.ReplayResumeAdmission.UnsupportedBlockers {
		if blocker.Code == RunForkBlockerTimerHistoryUnproven {
			t.Fatalf("timer blocker survived reconstruction: %#v", materialized.ReplayResumeAdmission.UnsupportedBlockers)
		}
	}
	var forkTimerCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = $1::uuid
		  AND source_timer_id IS NOT NULL
		  AND forked_from_run_id = $2::uuid
		  AND forked_from_event_id = $3::uuid
		  AND reconstruction_owner = $4
		  AND status = 'active'
	`, materialized.ForkRunID, sourceRunID, eventID, RunForkHistoricalReplayTimerReconstructionOwner).Scan(&forkTimerCount); err != nil {
		t.Fatalf("count reconstructed fork timers: %v", err)
	}
	if forkTimerCount != 1 {
		t.Fatalf("reconstructed fork timers = %d, want 1", forkTimerCount)
	}
	var sourceTimerCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = $1::uuid
		  AND source_timer_id IS NULL
		  AND status = 'active'
	`, sourceRunID).Scan(&sourceTimerCount); err != nil {
		t.Fatalf("count source timers: %v", err)
	}
	if sourceTimerCount != 1 {
		t.Fatalf("source timers = %d, want 1", sourceTimerCount)
	}
}

func TestSelectedContractExecutionMaterializationPreservesRouteBlocker(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002525, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow,
			is_materialized, status, created_at
		)
		VALUES ('item.received', 'node', 'selected-route-node', 'flow-a/2', 'flow-a', true, 'active', $1)
	`, at); err != nil {
		t.Fatalf("seed routing rule: %v", err)
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), RunForkBlockerFlowRouteHistoryUnproven) {
		t.Fatalf("materialization error = %v, want route blocker", err)
	}
	if materialized.ForkRunID != "" {
		t.Fatalf("materialized fork despite route blocker: %#v", materialized)
	}
	assertNoSelectedContractForkRows(t, db, sourceRunID)
}

func seedSelectedContractExecutionStoreSource(t *testing.T, db execContextDB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, sourceRunID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'item.received', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, eventID, entityID, at); err != nil {
		t.Fatalf("seed source event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'test-node', 'pending', $3)
	`, sourceRunID, eventID, at); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'selected-store-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Selected Store Entity"'::jsonb, $3::uuid, 'platform', 'selected-store-test', 'seed', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Selected Store Entity',
			'pending', '{}'::jsonb, '{"name":"Selected Store Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
}

func assertNoSelectedContractForkRows(t *testing.T, db *sql.DB, sourceRunID string) {
	t.Helper()
	ctx := context.Background()
	for name, query := range map[string]string{
		"runs": `
			SELECT COUNT(*)
			FROM runs
			WHERE forked_from_run_id = $1::uuid
		`,
		"selected_contract_bindings": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_bindings
			WHERE source_run_id = $1::uuid
		`,
		"selected_contract_executions": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_executions
			WHERE source_run_id = $1::uuid
		`,
		"selected_contract_branch_divergences": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_branch_divergences
			WHERE source_run_id = $1::uuid
		`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, sourceRunID).Scan(&count); err != nil {
			t.Fatalf("count %s rows: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s rows for blocked selected-contract fork = %d, want 0", name, count)
		}
	}
}
