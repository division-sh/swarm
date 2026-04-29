package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"swarm/internal/runtime/runforkadmission"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestExecuteSelectedContractRunForkWritesForkLocalExecutionAndLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := filepath.Join(repoRoot, "docs/specs/swarm-platform/platform/contracts/platform-spec.yaml")
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002200, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Owner != store.RunForkSelectedContractExecutionOwner || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
		t.Fatalf("result = %#v", result)
	}
	forkEventID := result.ForkEvents[0].ForkEventID
	if forkEventID == "" || forkEventID == sourceEventID {
		t.Fatalf("fork event id = %q, source = %q", forkEventID, sourceEventID)
	}

	var forkEventRun, forkEventName, forkSourceEvent string
	if err := db.QueryRowContext(ctx, `
		SELECT run_id::text, event_name, COALESCE(source_event_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
	`, forkEventID).Scan(&forkEventRun, &forkEventName, &forkSourceEvent); err != nil {
		t.Fatalf("load fork event: %v", err)
	}
	if forkEventRun != result.Materialization.ForkRunID || forkEventName != "item.received" {
		t.Fatalf("fork event = run:%s name:%s", forkEventRun, forkEventName)
	}
	if forkSourceEvent == sourceEventID {
		t.Fatalf("fork event source_event_id copied source event %s; lineage must be explicit table evidence", sourceEventID)
	}

	var lineageCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND source_event_id = $3::uuid
		  AND fork_event_id = $4::uuid
	`, result.Materialization.ForkRunID, sourceRunID, sourceEventID, forkEventID).Scan(&lineageCount); err != nil {
		t.Fatalf("count selected execution lineage: %v", err)
	}
	if lineageCount != 1 {
		t.Fatalf("selected execution lineage rows = %d, want 1", lineageCount)
	}

	var sourceCopiedEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&sourceCopiedEvents); err != nil {
		t.Fatalf("count copied source event ids: %v", err)
	}
	if sourceCopiedEvents != 0 {
		t.Fatalf("copied source event ids into fork run = %d, want 0", sourceCopiedEvents)
	}

	var forkReceipts, forkDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, forkEventID).Scan(&forkReceipts); err != nil {
		t.Fatalf("count fork receipts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND event_id = $2::uuid`, result.Materialization.ForkRunID, forkEventID).Scan(&forkDeliveries); err != nil {
		t.Fatalf("count fork deliveries: %v", err)
	}
	if forkReceipts == 0 || forkDeliveries == 0 {
		t.Fatalf("fork outcomes = receipts:%d deliveries:%d, want fork-local writes", forkReceipts, forkDeliveries)
	}

	var emittedFollowUps int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'item.processed'
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, forkEventID).Scan(&emittedFollowUps); err != nil {
		t.Fatalf("count emitted follow-ups: %v", err)
	}
	if emittedFollowUps != 1 {
		t.Fatalf("fork follow-up events = %d, want 1", emittedFollowUps)
	}

	var sourceStatus, forkStatus, forkEntityState string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, result.Materialization.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT current_state FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, result.Materialization.ForkRunID, entityID).Scan(&forkEntityState); err != nil {
		t.Fatalf("load fork entity state: %v", err)
	}
	if sourceStatus != store.RunForkSourceFrozenStatus || forkStatus != store.RunForkActivatedStatus || forkEntityState == "" {
		t.Fatalf("post execution = source:%s fork:%s entity:%s", sourceStatus, forkStatus, forkEntityState)
	}
}

func TestExecuteSelectedContractRunForkPreservesSessionBlocker(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := filepath.Join(repoRoot, "docs/specs/swarm-platform/platform/contracts/platform-spec.yaml")
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	sessionID := uuid.NewString()
	at := time.Unix(1700002300, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model_tier, conversation_mode, status, created_at)
		VALUES ('agent-a', 'test-agent', 'tier1', 'session_per_entity', 'active', $1)
		ON CONFLICT (agent_id) DO NOTHING
	`, at); err != nil {
		t.Fatalf("seed source session agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			runtime_mode, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'flow-a/1', $3::text, 'entity',
			'session_per_entity', 'active', $4, $4)
	`, sessionID, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source session: %v", err)
	}

	_, err = ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkBlockerSessionHistoryUnproven) {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want session blocker", err)
	}
}

func runForkExecutionRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	return root
}

func seedSelectedExecutionSourceRun(t *testing.T, db *sql.DB, sourceRunID, entityID, sourceEventID, eventName string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]any{"entity_id": entityID})
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
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'flow-a/1', 'entity', $5::jsonb, 'source-runtime', 'platform', $6)
	`, sourceRunID, sourceEventID, eventName, entityID, string(payload), at); err != nil {
		t.Fatalf("seed source event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'test-node', 'pending', 'source_pending_node_delivery', $3)
	`, sourceRunID, sourceEventID, at); err != nil {
		t.Fatalf("seed source delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'selected-execution-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Selected Execution Entity"'::jsonb, $3::uuid, 'platform', 'selected-execution-test', 'seed', $4)
	`, sourceRunID, entityID, sourceEventID, at); err != nil {
		t.Fatalf("seed source mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Selected Execution Entity',
			'pending', '{}'::jsonb, '{"name":"Selected Execution Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
}

func seedSourceOutcomeThatMustNotSuppressFork(t *testing.T, db *sql.DB, sourceEventID, entityID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance, outcome, reason_code, side_effects, processed_at
		)
		VALUES ($1::uuid, 'node', 'old-source-node', $2::uuid, 'flow-a/1', 'success', 'source_outcome_must_not_suppress_fork', '{}'::jsonb, $3)
	`, sourceEventID, entityID, at); err != nil {
		t.Fatalf("seed source receipt: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dead_letters (
			original_event_id, original_event, entity_id, flow_instance, failure_type, error_message, handler_node, created_at
		)
		VALUES ($1::uuid, 'item.received', $2::uuid, 'flow-a/1', 'handler_error', 'source dead letter must not suppress fork', 'old-source-node', $3)
	`, sourceEventID, entityID, at); err != nil {
		t.Fatalf("seed source dead letter: %v", err)
	}
}
