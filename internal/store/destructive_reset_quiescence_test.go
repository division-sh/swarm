package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/runtime/destructivereset"
	runtimemanager "swarm/internal/runtime/manager"
	"swarm/internal/testutil"
)

func TestPostgresStore_ApplyDestructiveResetQuiescence_TerminalizesRunsAndDeliveries(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })
	ctx := context.Background()
	now := time.Date(2026, 5, 15, 2, 40, 0, 0, time.UTC)
	runID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	agentPending := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.pending")
	agentInProgress := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.in_progress")
	agentRetryableFailed := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.failed_retryable")
	agentExhaustedFailed := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.failed_exhausted")
	nodePending := seedDestructiveResetEvent(t, ctx, pg, runID, "node.pending")
	nodeInProgress := seedDestructiveResetEvent(t, ctx, pg, runID, "node.in_progress")
	delivered := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.delivered")
	activeSessionID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, retry_count, reason_code, created_at, delivered_at
		) VALUES
			($1::uuid, $2::uuid, 'agent', 'agent-a', 'pending', NULL, 0, 'matched_agent_subscription', now(), NULL),
			($1::uuid, $3::uuid, 'agent', 'agent-a', 'in_progress', $8::uuid, 0, 'agent_processing', now(), NULL),
			($1::uuid, $4::uuid, 'agent', 'agent-a', 'failed', NULL, 1, 'agent_retryable_error', now() - interval '5 minutes', now() - interval '2 minutes'),
			($1::uuid, $5::uuid, 'agent', 'agent-a', 'failed', NULL, 2, 'retry_exhausted', now() - interval '5 minutes', now() - interval '2 minutes'),
			($1::uuid, $6::uuid, 'node', 'node-a', 'pending', NULL, 0, 'matched_node_subscription', now(), NULL),
			($1::uuid, $7::uuid, 'node', 'node-a', 'in_progress', $8::uuid, 0, 'node_processing', now(), NULL),
			($1::uuid, $9::uuid, 'agent', 'agent-a', 'delivered', NULL, 0, 'agent_processed', now(), now())
	`, runID, agentPending, agentInProgress, agentRetryableFailed, agentExhaustedFailed, nodePending, nodeInProgress, activeSessionID, delivered); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, agentPending, "processed", ""); err != nil {
		t.Fatalf("seed pipeline receipt: %v", err)
	}

	result, err := pg.ApplyDestructiveResetQuiescence(ctx, destructivereset.QuiescenceRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan:          destructivereset.Plan{ActiveRuns: []destructivereset.RunRef{{RunID: runID, Status: "running"}}},
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetQuiescence: %v", err)
	}
	if len(result.Runs) != 1 || result.Runs[0].Status != "cancelled" || !result.Runs[0].Changed {
		t.Fatalf("runs = %#v, want one cancelled run", result.Runs)
	}
	if len(result.Deliveries) != 5 {
		t.Fatalf("deliveries = %#v, want five active/retryable deliveries", result.Deliveries)
	}
	if result.PipelineReceiptCount != 5 {
		t.Fatalf("pipeline receipt count = %d, want 5", result.PipelineReceiptCount)
	}

	var runStatus, controlStatus, reason, controlledBy string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT r.status, rc.control_status, COALESCE(rc.reason,''), COALESCE(rc.controlled_by,'')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&runStatus, &controlStatus, &reason, &controlledBy); err != nil {
		t.Fatalf("load run/control state: %v", err)
	}
	if runStatus != "cancelled" || controlStatus != "stopped" || reason != destructivereset.QuiescenceReasonCode || controlledBy != destructivereset.QuiescenceControlledBy {
		t.Fatalf("run/control = %s/%s/%s/%s", runStatus, controlStatus, reason, controlledBy)
	}

	assertDestructiveResetDelivery(t, ctx, pg, agentPending, "agent", "agent-a")
	assertDestructiveResetDelivery(t, ctx, pg, agentInProgress, "agent", "agent-a")
	assertDestructiveResetDelivery(t, ctx, pg, agentRetryableFailed, "agent", "agent-a")
	assertDestructiveResetDelivery(t, ctx, pg, nodePending, "node", "node-a")
	assertDestructiveResetDelivery(t, ctx, pg, nodeInProgress, "node", "node-a")

	var deliveredStatus, deliveredReason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
	`, delivered).Scan(&deliveredStatus, &deliveredReason); err != nil {
		t.Fatalf("load delivered row: %v", err)
	}
	if deliveredStatus != "delivered" || deliveredReason != "agent_processed" {
		t.Fatalf("delivered row = %s/%s, want untouched", deliveredStatus, deliveredReason)
	}
	var exhaustedStatus, exhaustedReason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
	`, agentExhaustedFailed).Scan(&exhaustedStatus, &exhaustedReason); err != nil {
		t.Fatalf("load exhausted failed row: %v", err)
	}
	if exhaustedStatus != "failed" || exhaustedReason != "retry_exhausted" {
		t.Fatalf("exhausted failed row = %s/%s, want untouched", exhaustedStatus, exhaustedReason)
	}

	if err := pg.MarkEventDeliveryInProgress(ctx, agentInProgress, "agent-a", uuid.NewString()); err != nil {
		t.Fatalf("late MarkEventDeliveryInProgress: %v", err)
	}
	if err := pg.MarkEventDeliveryInProgress(ctx, agentRetryableFailed, "agent-a", uuid.NewString()); err != nil {
		t.Fatalf("late retryable failed MarkEventDeliveryInProgress: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, agentInProgress, "agent-a", runtimemanager.ReceiptStatusProcessed, ""); err != nil {
		t.Fatalf("late UpsertEventReceipt: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, agentInProgress, "processed", ""); err != nil {
		t.Fatalf("late UpsertPipelineReceipt: %v", err)
	}
	assertDestructiveResetDelivery(t, ctx, pg, agentInProgress, "agent", "agent-a")
	assertDestructiveResetDelivery(t, ctx, pg, agentRetryableFailed, "agent", "agent-a")
	assertDestructiveResetReceipt(t, ctx, pg, agentInProgress, "agent-a")
	assertDestructiveResetReceipt(t, ctx, pg, agentInProgress, destructiveResetPipelineSubscriberID)
	assertDestructiveResetReceipt(t, ctx, pg, agentRetryableFailed, "agent-a")
	assertDestructiveResetReceipt(t, ctx, pg, agentRetryableFailed, destructiveResetPipelineSubscriberID)

	pending, err := pg.ListPendingEventsForAgent(ctx, "agent-a", now.Add(-time.Hour), 50)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending events = %#v, want none after quiescence", pending)
	}
	events, err := pg.ListOperatorEvents(ctx, OperatorEventListOptions{
		Filter: OperatorEventListFilter{RunID: runID, DeliveryStatus: "dead_letter", ReasonCode: destructivereset.QuiescenceReasonCode},
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("ListOperatorEvents: %v", err)
	}
	if len(events.Events) != 5 {
		t.Fatalf("operator events = %d, want 5 nuke-quiesced events", len(events.Events))
	}
}

func TestPostgresStore_ApplyDestructiveResetQuiescence_DryRunDoesNotMutate(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })
	ctx := context.Background()
	now := time.Date(2026, 5, 15, 2, 45, 0, 0, time.UTC)
	runID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	eventID := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.pending")
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, status, created_at)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-a', 'pending', now())
	`, runID, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	result, err := pg.ApplyDestructiveResetQuiescence(ctx, destructivereset.QuiescenceRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			DryRun:    true,
			PlannedAt: now.Add(-time.Minute),
			Plan:      destructivereset.Plan{ActiveRuns: []destructivereset.RunRef{{RunID: runID, Status: "running"}}},
		},
	})
	if err != nil {
		t.Fatalf("ApplyDestructiveResetQuiescence dry-run: %v", err)
	}
	if !result.DryRun || len(result.Deliveries) != 1 || !result.Deliveries[0].Changed {
		t.Fatalf("dry-run result = %#v", result)
	}
	var status, reason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
	`, eventID).Scan(&status, &reason); err != nil {
		t.Fatalf("load delivery after dry-run: %v", err)
	}
	if status != "pending" || reason != "" {
		t.Fatalf("dry-run mutated delivery = %s/%s", status, reason)
	}
}

func seedDestructiveResetEvent(t *testing.T, ctx context.Context, pg *PostgresStore, runID, name string) string {
	t.Helper()
	eventID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3, 'global', '{}'::jsonb, 'test', 'agent', now()
		)
	`, eventID, runID, name); err != nil {
		t.Fatalf("seed event %s: %v", name, err)
	}
	return eventID
}

func assertDestructiveResetDelivery(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, subscriberType, subscriberID string) {
	t.Helper()
	var status, reason string
	var activeSession sql.NullString
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, ''), active_session_id::text
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = $2
		  AND subscriber_id = $3
	`, eventID, subscriberType, subscriberID).Scan(&status, &reason, &activeSession); err != nil {
		t.Fatalf("load delivery %s/%s/%s: %v", eventID, subscriberType, subscriberID, err)
	}
	if status != "dead_letter" || reason != destructivereset.QuiescenceReasonCode || activeSession.Valid {
		t.Fatalf("delivery %s/%s/%s = %s/%s active=%v, want nuke dead_letter", eventID, subscriberType, subscriberID, status, reason, activeSession.Valid)
	}
	assertDestructiveResetReceipt(t, ctx, pg, eventID, subscriberID)
}

func assertDestructiveResetReceipt(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, subscriberID string) {
	t.Helper()
	var outcome, reason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_id = $2
	`, eventID, subscriberID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load receipt %s/%s: %v", eventID, subscriberID, err)
	}
	if outcome != "dead_letter" || reason != destructivereset.QuiescenceReasonCode {
		t.Fatalf("receipt %s/%s = %s/%s, want nuke dead_letter", eventID, subscriberID, outcome, reason)
	}
}
