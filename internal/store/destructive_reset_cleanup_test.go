package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/runtime/destructivereset"
	"swarm/internal/testutil"
)

func TestPostgresStore_ApplyDestructiveResetCleanup_DeletesRunScopedRowsAndPreservesBoundaries(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })
	ctx := context.Background()
	seedDestructiveResetCleanupRows(t, ctx, pg)

	now := time.Date(2026, 5, 16, 18, 30, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup: %v", err)
	}
	if result.DryRun {
		t.Fatal("cleanup result DryRun = true, want false")
	}
	if len(result.RunIDs) != 2 {
		t.Fatalf("cleanup run IDs = %#v, want two runs", result.RunIDs)
	}
	assertCleanupTableResult(t, result, "runs", 2, 2)
	assertCleanupTableResult(t, result, "events", 4, 4)
	assertCleanupTableResult(t, result, "event_receipts", 3, 3)
	assertCleanupTableResult(t, result, "dead_letters", 1, 1)
	assertCleanupTableResult(t, result, "timers", 3, 3)
	assertCleanupTableResult(t, result, "mailbox", 0, 0)
	assertCleanupTableResult(t, result, "generated_entity_tables", 0, 0)

	for _, table := range []string{
		"runs",
		"event_deliveries",
		"run_fork_delivery_event_replays",
		"run_fork_selected_contract_executions",
		"run_fork_selected_contract_branch_divergences",
		"run_fork_selected_contract_route_recoveries",
		"run_fork_selected_contract_bindings",
		"agent_turns",
		"agent_conversation_audits",
		"agent_sessions",
		"entity_mutations",
		"entity_state",
		"run_control_state",
	} {
		if got := countRows(t, ctx, pg, table); got != 0 {
			t.Fatalf("%s rows after cleanup = %d, want 0", table, got)
		}
	}
	for _, table := range []string{"events", "event_receipts", "dead_letters", "timers"} {
		if got := countRows(t, ctx, pg, table); got != 1 {
			t.Fatalf("%s rows after cleanup = %d, want 1 preserved global row", table, got)
		}
	}
	for _, table := range []string{
		"schema_version",
		"api_idempotency",
		"runtime_ingress_state",
		"agents",
		"flow_instances",
		"routing_rules",
		"mailbox",
		"spend_ledger",
		"generated_entity_fixture",
		"generated_node_state_fixture",
	} {
		if got := countRows(t, ctx, pg, table); got != 1 {
			t.Fatalf("%s rows after cleanup = %d, want 1 preserved row", table, got)
		}
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_DryRunCountsWithoutMutation(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })
	ctx := context.Background()
	seedDestructiveResetCleanupRows(t, ctx, pg)

	now := time.Date(2026, 5, 16, 18, 35, 0, 0, time.UTC)
	result, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			DryRun:        true,
			PlannedAt:     now.Add(-time.Minute),
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetCleanup dry-run: %v", err)
	}
	if !result.DryRun {
		t.Fatal("cleanup result DryRun = false, want true")
	}
	assertCleanupTableResult(t, result, "runs", 2, 0)
	assertCleanupTableResult(t, result, "events", 4, 0)
	assertCleanupTableResult(t, result, "mailbox", 0, 0)
	if got := countRows(t, ctx, pg, "runs"); got != 2 {
		t.Fatalf("runs after dry-run = %d, want 2", got)
	}
	if got := countRows(t, ctx, pg, "events"); got != 5 {
		t.Fatalf("events after dry-run = %d, want 5 including preserved no-run event", got)
	}
	if got := countRows(t, ctx, pg, "mailbox"); got != 1 {
		t.Fatalf("mailbox after dry-run = %d, want preserved", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_RollsBackOnDeleteError(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })
	ctx := context.Background()
	runID := uuid.NewString()
	activeSessionID := uuid.NewString()
	predecessorSessionID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO agents (agent_id, role, model_tier, conversation_mode) VALUES ('agent-a', 'operator', 'default', 'session')`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, scope_key, scope, runtime_mode, status)
		VALUES ($1::uuid, $2::uuid, 'agent-a', 'agent-a:global', 'global', 'session', 'active')
	`, activeSessionID, runID); err != nil {
		t.Fatalf("seed active session: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, scope_key, scope, runtime_mode, status, termination_reason, terminated_at, successor_session_id
		) VALUES (
			$1::uuid, NULL, 'agent-a', 'agent-a:global', 'global', 'session', 'terminated', 'cancelled', now(), $2::uuid
		)
	`, predecessorSessionID, activeSessionID); err != nil {
		t.Fatalf("seed preserved predecessor session: %v", err)
	}

	now := time.Date(2026, 5, 16, 18, 45, 0, 0, time.UTC)
	_, err = pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if err == nil {
		t.Fatal("ApplyDestructiveResetCleanup error = nil, want FK rollback failure")
	}
	if got := countRows(t, ctx, pg, "runs"); got != 1 {
		t.Fatalf("runs after rollback failure = %d, want 1", got)
	}
	if got := countRows(t, ctx, pg, "agent_sessions"); got != 2 {
		t.Fatalf("agent_sessions after rollback failure = %d, want 2", got)
	}
}

func TestPostgresStore_ApplyDestructiveResetCleanup_RequiresAppliedQuiescence(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	now := time.Date(2026, 5, 16, 18, 50, 0, 0, time.UTC)
	_, err = pg.ApplyDestructiveResetCleanup(context.Background(), destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
		},
		Quiescence: destructivereset.QuiescenceResult{DryRun: true, AppliedAt: now.Add(-30 * time.Second)},
	})
	if err == nil {
		t.Fatal("ApplyDestructiveResetCleanup error = nil, want applied quiescence failure")
	}
}

func seedDestructiveResetCleanupRows(t *testing.T, ctx context.Context, pg *PostgresStore) {
	t.Helper()
	runA := uuid.NewString()
	runB := uuid.NewString()
	sourceEvent := uuid.NewString()
	forkEvent := uuid.NewString()
	noRunEvent := uuid.NewString()
	sourceDelivery := uuid.NewString()
	forkDelivery := uuid.NewString()
	sessionID := uuid.NewString()
	entityID := uuid.NewString()
	timerRun := uuid.NewString()
	timerForkRun := uuid.NewString()
	timerForkEvent := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `CREATE TABLE generated_entity_fixture (entity_id UUID PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		t.Fatalf("create generated entity fixture: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `CREATE TABLE generated_node_state_fixture (entity_id UUID NOT NULL, node_id TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(entity_id, node_id))`); err != nil {
		t.Fatalf("create generated node fixture: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, trigger_event_id, trigger_event_type) VALUES
			($1::uuid, 'running', $3::uuid, 'source.event'),
			($2::uuid, 'completed', $4::uuid, 'fork.event')
	`, runA, runB, sourceEvent, forkEvent); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO events (event_id, run_id, event_name, entity_id, flow_instance, scope, payload, produced_by_type) VALUES
			($1::uuid, $3::uuid, 'source.event', $5::uuid, 'flow/a', 'entity', '{}'::jsonb, 'external'),
			($2::uuid, $4::uuid, 'fork.event', $5::uuid, 'flow/a', 'entity', '{}'::jsonb, 'platform'),
			($6::uuid, NULL, 'platform.runtime_log', NULL, NULL, 'global', '{}'::jsonb, 'platform'),
			($7::uuid, $3::uuid, 'timer.event', $5::uuid, 'flow/a', 'entity', '{}'::jsonb, 'node'),
			($8::uuid, $3::uuid, 'extra.event', $5::uuid, 'flow/a', 'entity', '{}'::jsonb, 'agent')
	`, sourceEvent, forkEvent, runA, runB, entityID, noRunEvent, timerForkEvent, uuid.NewString()); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status) VALUES
			($1::uuid, $3::uuid, $5::uuid, 'agent', 'agent-a', 'dead_letter'),
			($2::uuid, $4::uuid, $6::uuid, 'node', 'node-a', 'delivered')
	`, sourceDelivery, forkDelivery, runA, runB, sourceEvent, forkEvent); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects) VALUES
			($1::uuid, 'agent', 'agent-a', 'dead_letter', '{}'::jsonb),
			($2::uuid, 'node', 'node-a', 'success', '{}'::jsonb),
			($3::uuid, 'platform', 'pipeline', 'success', '{}'::jsonb),
			($4::uuid, 'platform', 'preserved', 'success', '{}'::jsonb)
	`, sourceEvent, forkEvent, timerForkEvent, noRunEvent); err != nil {
		t.Fatalf("seed receipts: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO dead_letters (original_event_id, original_event, original_payload, flow_instance, failure_type) VALUES
			($1::uuid, 'source.event', '{}'::jsonb, 'flow/a', 'handler_error'),
			(NULL, 'global.failure', '{}'::jsonb, 'flow/a', 'handler_error')
	`, sourceEvent); err != nil {
		t.Fatalf("seed dead letters: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO run_fork_delivery_event_replays (fork_run_id, source_run_id, source_event_id, source_delivery_id, fork_event_id, fork_delivery_id, subscriber_type, subscriber_id)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6::uuid, 'agent', 'agent-a')
	`, runB, runA, sourceEvent, sourceDelivery, forkEvent, forkDelivery); err != nil {
		t.Fatalf("seed delivery replay: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings (fork_run_id, source_run_id, fork_event_id, mode, contracts_root, workflow_name, workflow_version)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'selected_contracts', '/contracts', 'wf', 'v1')
	`, runB, runA, forkEvent); err != nil {
		t.Fatalf("seed selected binding: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_executions (fork_run_id, source_run_id, source_event_id, fork_event_id, event_name)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'source.event')
	`, runB, runA, sourceEvent, forkEvent); err != nil {
		t.Fatalf("seed selected execution: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_branch_divergences (fork_run_id, source_run_id, fork_event_id, owner, policy, source_run_status_at_activation, source_run_status_after_activation)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'test', 'selected_contract_source_advanced_branch', 'running', 'completed')
	`, runB, runA, forkEvent); err != nil {
		t.Fatalf("seed selected branch divergence: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_route_recoveries (
			fork_run_id, source_run_id, fork_event_id, owner, runtime_recovery_owner, mode, contracts_root, workflow_name, workflow_version,
			route_topology_owner, recipient_planning_owner, frontier_evidence_fingerprint, route_topology_fingerprint,
			recipient_planning_fingerprint, route_topology, recipient_planning
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'test', 'test', 'selected_contracts', '/contracts', 'wf', 'v1',
			'topology', 'recipients', 'frontier', 'route', 'recipient', '{}'::jsonb, '{}'::jsonb
		)
	`, runB, runA, forkEvent); err != nil {
		t.Fatalf("seed selected route recovery: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO agents (agent_id, role, model_tier, conversation_mode) VALUES ('agent-a', 'operator', 'default', 'session')`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, scope_key, scope, runtime_mode, status)
		VALUES ($1::uuid, $2::uuid, 'agent-a', 'agent-a:global', 'global', 'session', 'active')
	`, sessionID, runA); err != nil {
		t.Fatalf("seed agent session: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_turns (run_id, agent_id, session_id, runtime_mode, scope_key)
		VALUES ($1::uuid, 'agent-a', $2::uuid, 'session', 'agent-a:global')
	`, runA, sessionID); err != nil {
		t.Fatalf("seed agent turn: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (run_id, agent_id, scope_key, status)
		VALUES ($1::uuid, 'agent-a', 'agent-a:task', 'active')
	`, runA); err != nil {
		t.Fatalf("seed agent audit: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO entity_state (run_id, entity_id, flow_instance, current_state) VALUES ($1::uuid, $2::uuid, 'flow/a', 'active')`, runA, entityID); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO entity_mutations (run_id, entity_id, field, writer_type, writer_id) VALUES ($1::uuid, $2::uuid, 'status', 'platform', 'test')`, runA, entityID); err != nil {
		t.Fatalf("seed entity mutation: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO timers (timer_id, timer_name, run_id, entity_id, flow_instance, fire_event, fire_at) VALUES
			($1::uuid, 'run timer', $4::uuid, $5::uuid, 'flow/a', 'timer.fire', now()),
			($2::uuid, 'fork run timer', NULL, $5::uuid, 'flow/a', 'timer.fire', now()),
			($3::uuid, 'fork event timer', NULL, $5::uuid, 'flow/a', 'timer.fire', now()),
			($6::uuid, 'global timer', NULL, NULL, NULL, 'timer.global', now())
	`, timerRun, timerForkRun, timerForkEvent, runA, entityID, uuid.NewString()); err != nil {
		t.Fatalf("seed timers: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `UPDATE timers SET forked_from_run_id = $1::uuid WHERE timer_id = $2::uuid`, runB, timerForkRun); err != nil {
		t.Fatalf("seed timer forked_from_run_id: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `UPDATE timers SET forked_from_event_id = $1::uuid WHERE timer_id = $2::uuid`, timerForkEvent, timerForkEvent); err != nil {
		t.Fatalf("seed timer forked_from_event_id: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, controlled_by) VALUES ($1::uuid, 'stopped', 'test')`, runA); err != nil {
		t.Fatalf("seed run control: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO schema_version (platform_version) VALUES ('test') ON CONFLICT (id) DO UPDATE SET platform_version = EXCLUDED.platform_version`); err != nil {
		t.Fatalf("seed schema version: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO api_idempotency (method, actor_token_id, idempotency_key, request_hash, resource_id, response, expires_at)
		VALUES ('runtime.nuke', 'operator', 'idem', 'hash', 'runtime', '{}'::jsonb, now() + interval '1 hour')
	`); err != nil {
		t.Fatalf("seed api idempotency: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO runtime_ingress_state (status, controlled_by)
		VALUES ('running', 'test')
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, controlled_by = EXCLUDED.controlled_by
	`); err != nil {
		t.Fatalf("seed runtime ingress: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO flow_instances (instance_id, flow_template, mode) VALUES ('flow/a', 'flow', 'static')`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance)
		VALUES ('source.event', 'agent', 'agent-a', 'flow/a')
	`); err != nil {
		t.Fatalf("seed routing rule: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO mailbox (entity_id, flow_instance, item_type, source_event_id, from_agent, summary)
		VALUES ($1::uuid, 'flow/a', 'human_task', $2::uuid, 'agent-a', 'preserve mailbox')
	`, entityID, sourceEvent); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO spend_ledger (entity_id, flow_instance, agent_id, model, invocation_type)
		VALUES ($1::uuid, 'flow/a', 'agent-a', 'model', 'agent_turn')
	`, entityID); err != nil {
		t.Fatalf("seed spend ledger: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO generated_entity_fixture (entity_id) VALUES ($1::uuid)`, entityID); err != nil {
		t.Fatalf("seed generated entity table: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO generated_node_state_fixture (entity_id, node_id) VALUES ($1::uuid, 'node-a')`, entityID); err != nil {
		t.Fatalf("seed generated node table: %v", err)
	}
}

func assertCleanupTableResult(t *testing.T, result destructivereset.CleanupResult, table string, matched, deleted int64) {
	t.Helper()
	for _, row := range result.Tables {
		if row.Table == table {
			if row.MatchedRows != matched || row.DeletedRows != deleted {
				t.Fatalf("cleanup table %s result = matched %d deleted %d, want %d/%d", table, row.MatchedRows, row.DeletedRows, matched, deleted)
			}
			return
		}
	}
	t.Fatalf("cleanup result missing table %s: %#v", table, result.Tables)
}

func countRows(t *testing.T, ctx context.Context, pg *PostgresStore, table string) int64 {
	t.Helper()
	var count int64
	if err := pg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+quoteIdent(table)).Scan(&count); err != nil {
		if err == sql.ErrNoRows {
			return 0
		}
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}
