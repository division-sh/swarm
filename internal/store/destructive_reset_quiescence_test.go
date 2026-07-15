package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
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
	agentDelayedRetryableFailed := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.failed_retryable_delayed")
	agentExhaustedFailed := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.failed_exhausted")
	nodePending := seedDestructiveResetEvent(t, ctx, pg, runID, "node.pending")
	nodeInProgress := seedDestructiveResetEvent(t, ctx, pg, runID, "node.in_progress")
	nodeDelayedRetryableFailed := seedDestructiveResetEvent(t, ctx, pg, runID, "node.failed_retryable_delayed")
	delivered := seedDestructiveResetEvent(t, ctx, pg, runID, "agent.delivered")
	activeSessionID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, retry_count, reason_code, created_at, delivered_at
		) VALUES
			($1::uuid, $2::uuid, 'agent', 'agent-a', 'pending', NULL, 0, 'matched_agent_subscription', now(), NULL),
			($1::uuid, $3::uuid, 'agent', 'agent-a', 'in_progress', $10::uuid, 0, 'agent_processing', now(), NULL),
			($1::uuid, $4::uuid, 'agent', 'agent-a', 'failed', NULL, 1, 'agent_retryable_error', now() - interval '5 minutes', now() - interval '2 minutes'),
			($1::uuid, $5::uuid, 'agent', 'agent-a', 'failed', NULL, 1, 'agent_retryable_error', now(), now()),
			($1::uuid, $6::uuid, 'agent', 'agent-a', 'failed', NULL, 2, 'retry_exhausted', now() - interval '5 minutes', now() - interval '2 minutes'),
			($1::uuid, $7::uuid, 'node', 'node-a', 'pending', NULL, 0, 'matched_node_subscription', now(), NULL),
			($1::uuid, $8::uuid, 'node', 'node-a', 'in_progress', $10::uuid, 0, 'node_processing', now(), NULL),
			($1::uuid, $9::uuid, 'node', 'node-a', 'failed', NULL, 1, 'node_retryable_error', now(), now()),
			($1::uuid, $11::uuid, 'agent', 'agent-a', 'delivered', NULL, 0, 'agent_processed', now(), now())
	`, runID, agentPending, agentInProgress, agentRetryableFailed, agentDelayedRetryableFailed, agentExhaustedFailed, nodePending, nodeInProgress, nodeDelayedRetryableFailed, activeSessionID, delivered); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, agentPending, "processed", nil); err != nil {
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
	if len(result.Deliveries) != 7 {
		t.Fatalf("deliveries = %#v, want seven active/retryable deliveries", result.Deliveries)
	}
	if result.PipelineReceiptCount != 7 {
		t.Fatalf("pipeline receipt count = %d, want 7", result.PipelineReceiptCount)
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
	assertDestructiveResetDelivery(t, ctx, pg, agentDelayedRetryableFailed, "agent", "agent-a")
	assertDestructiveResetDelivery(t, ctx, pg, nodePending, "node", "node-a")
	assertDestructiveResetDelivery(t, ctx, pg, nodeInProgress, "node", "node-a")
	assertDestructiveResetDelivery(t, ctx, pg, nodeDelayedRetryableFailed, "node", "node-a")

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
	if _, err := pg.DB.ExecContext(ctx, `
		UPDATE event_deliveries
		SET delivered_at = now() - interval '2 minutes'
		WHERE event_id = $1::uuid
	`, agentDelayedRetryableFailed); err != nil {
		t.Fatalf("age delayed retryable failed delivery after quiescence: %v", err)
	}
	if err := pg.MarkEventDeliveryInProgress(ctx, agentDelayedRetryableFailed, "agent-a", uuid.NewString()); err != nil {
		t.Fatalf("late delayed retryable failed MarkEventDeliveryInProgress: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, agentInProgress, "agent-a", runtimemanager.ReceiptStatusProcessed, nil); err != nil {
		t.Fatalf("late UpsertEventReceipt: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, agentInProgress, "processed", nil); err != nil {
		t.Fatalf("late UpsertPipelineReceipt: %v", err)
	}
	assertDestructiveResetDelivery(t, ctx, pg, agentInProgress, "agent", "agent-a")
	assertDestructiveResetDelivery(t, ctx, pg, agentRetryableFailed, "agent", "agent-a")
	assertDestructiveResetDelivery(t, ctx, pg, agentDelayedRetryableFailed, "agent", "agent-a")
	assertDestructiveResetReceipt(t, ctx, pg, agentInProgress, "agent-a")
	assertDestructiveResetReceipt(t, ctx, pg, agentInProgress, destructiveResetPipelineSubscriberID)
	assertDestructiveResetReceipt(t, ctx, pg, agentRetryableFailed, "agent-a")
	assertDestructiveResetReceipt(t, ctx, pg, agentRetryableFailed, destructiveResetPipelineSubscriberID)
	assertDestructiveResetReceipt(t, ctx, pg, agentDelayedRetryableFailed, "agent-a")
	assertDestructiveResetReceipt(t, ctx, pg, agentDelayedRetryableFailed, destructiveResetPipelineSubscriberID)

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
	if len(events.Events) != 7 {
		t.Fatalf("operator events = %d, want 7 nuke-quiesced events", len(events.Events))
	}
}

func TestPostgresStore_ApplyServeAbandonActiveRunQuiescence_QuiescesRecoverableWork(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })
	ctx := context.Background()
	now := time.Date(2026, 5, 18, 2, 10, 0, 0, time.UTC)
	runID := uuid.NewString()
	terminalRunID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status) VALUES
			($1::uuid, 'running'),
			($2::uuid, 'completed')
	`, runID, terminalRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	agentPending := seedDestructiveResetEvent(t, ctx, pg, runID, "serve.agent.pending")
	agentInProgress := seedDestructiveResetEvent(t, ctx, pg, runID, "serve.agent.in_progress")
	agentRetryableFailed := seedDestructiveResetEvent(t, ctx, pg, runID, "serve.agent.failed_retryable")
	nodePending := seedDestructiveResetEvent(t, ctx, pg, runID, "serve.node.pending")
	terminalRunPending := seedDestructiveResetEvent(t, ctx, pg, terminalRunID, "serve.terminal.pending")
	activeSessionID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, retry_count, reason_code, created_at, delivered_at
		) VALUES
			($1::uuid, $2::uuid, 'agent', 'agent-a', 'pending', NULL, 0, 'matched_agent_subscription', now(), NULL),
			($1::uuid, $3::uuid, 'agent', 'agent-a', 'in_progress', $7::uuid, 0, 'agent_processing', now(), NULL),
			($1::uuid, $4::uuid, 'agent', 'agent-a', 'failed', NULL, 1, 'agent_retryable_error', now() - interval '5 minutes', now() - interval '2 minutes'),
			($1::uuid, $5::uuid, 'node', 'node-a', 'pending', NULL, 0, 'matched_node_subscription', now(), NULL),
			($6::uuid, $8::uuid, 'agent', 'agent-a', 'pending', NULL, 0, 'matched_agent_subscription', now(), NULL)
	`, runID, agentPending, agentInProgress, agentRetryableFailed, nodePending, terminalRunID, activeSessionID, terminalRunPending); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	result, err := pg.ApplyServeAbandonActiveRunQuiescence(ctx, now)
	if err != nil {
		t.Fatalf("ApplyServeAbandonActiveRunQuiescence: %v", err)
	}
	if result.OperationName != runtimerunquiescence.ServeAbandonOperationName || result.ReasonCode != runtimerunquiescence.ServeAbandonReasonCode || result.ControlledBy != runtimerunquiescence.ServeAbandonControlledBy {
		t.Fatalf("serve abandon result metadata = %#v", result)
	}
	if len(result.Runs) != 1 || result.Runs[0].RunID != runID || result.Runs[0].Status != "cancelled" || !result.Runs[0].Changed {
		t.Fatalf("runs = %#v, want one cancelled active run", result.Runs)
	}
	if len(result.Deliveries) != 4 || result.PipelineReceiptCount != 4 {
		t.Fatalf("deliveries=%d pipeline_receipts=%d, want 4/4", len(result.Deliveries), result.PipelineReceiptCount)
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
	if runStatus != "cancelled" || controlStatus != "stopped" || reason != runtimerunquiescence.ServeAbandonReasonCode || controlledBy != runtimerunquiescence.ServeAbandonControlledBy {
		t.Fatalf("run/control = %s/%s/%s/%s", runStatus, controlStatus, reason, controlledBy)
	}

	assertServeAbandonDelivery(t, ctx, pg, agentPending, "agent", "agent-a")
	assertServeAbandonDelivery(t, ctx, pg, agentInProgress, "agent", "agent-a")
	assertServeAbandonDelivery(t, ctx, pg, agentRetryableFailed, "agent", "agent-a")
	assertServeAbandonDelivery(t, ctx, pg, nodePending, "node", "node-a")

	if err := pg.MarkEventDeliveryInProgress(ctx, agentRetryableFailed, "agent-a", uuid.NewString()); err != nil {
		t.Fatalf("late retryable failed MarkEventDeliveryInProgress: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, agentInProgress, "agent-a", runtimemanager.ReceiptStatusProcessed, nil); err != nil {
		t.Fatalf("late UpsertEventReceipt: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, agentInProgress, "processed", nil); err != nil {
		t.Fatalf("late UpsertPipelineReceipt: %v", err)
	}
	assertServeAbandonDelivery(t, ctx, pg, agentRetryableFailed, "agent", "agent-a")
	assertServeAbandonReceipt(t, ctx, pg, agentInProgress, "agent-a")
	assertServeAbandonReceipt(t, ctx, pg, agentInProgress, activeRunQuiescencePipelineSubscriberID)

	replay, err := pg.ListEventsMissingPipelineReceiptForRun(ctx, runID, now.Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceiptForRun: %v", err)
	}
	if len(replay) != 0 {
		t.Fatalf("missing pipeline replay events = %#v, want none", replay)
	}
	var terminalDeliveryStatus, terminalDeliveryReason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
	`, terminalRunPending).Scan(&terminalDeliveryStatus, &terminalDeliveryReason); err != nil {
		t.Fatalf("load terminal run delivery: %v", err)
	}
	if terminalDeliveryStatus != "pending" || terminalDeliveryReason != "matched_agent_subscription" {
		t.Fatalf("terminal run delivery = %s/%s, want untouched", terminalDeliveryStatus, terminalDeliveryReason)
	}
}

func TestSQLiteRuntimeStore_ApplyServeAbandonActiveRunQuiescence_QuiescesRecoverableWork(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 18, 2, 10, 0, 0, time.UTC)
	runID := uuid.NewString()
	pausedRunID := uuid.NewString()
	terminalRunID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at, ended_at) VALUES
			(?, 'running', ?, NULL),
			(?, 'paused', ?, NULL),
			(?, 'completed', ?, ?)
	`, runID, now.Add(-time.Hour), pausedRunID, now.Add(-time.Hour), terminalRunID, now.Add(-time.Hour), now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed sqlite runs: %v", err)
	}
	agentPending := seedSQLiteServeAbandonEvent(t, ctx, store, runID, "serve.agent.pending", now)
	agentInProgress := seedSQLiteServeAbandonEvent(t, ctx, store, runID, "serve.agent.in_progress", now)
	agentRetryableFailed := seedSQLiteServeAbandonEvent(t, ctx, store, runID, "serve.agent.failed_retryable", now)
	agentExhaustedFailed := seedSQLiteServeAbandonEvent(t, ctx, store, runID, "serve.agent.failed_exhausted", now)
	nodePending := seedSQLiteServeAbandonEvent(t, ctx, store, runID, "serve.node.pending", now)
	terminalRunPending := seedSQLiteServeAbandonEvent(t, ctx, store, terminalRunID, "serve.terminal.pending", now)
	activeSessionID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, retry_count, reason_code, created_at, delivered_at
		) VALUES
			(?, ?, ?, 'agent', 'agent-a', 'pending', NULL, 0, 'matched_agent_subscription', ?, NULL),
			(?, ?, ?, 'agent', 'agent-a', 'in_progress', ?, 0, 'agent_processing', ?, NULL),
			(?, ?, ?, 'agent', 'agent-a', 'failed', NULL, 1, 'agent_retryable_error', ?, ?),
			(?, ?, ?, 'agent', 'agent-a', 'failed', NULL, 2, 'retry_exhausted', ?, ?),
			(?, ?, ?, 'node', 'node-a', 'pending', NULL, 0, 'matched_node_subscription', ?, NULL),
			(?, ?, ?, 'agent', 'agent-a', 'pending', NULL, 0, 'matched_agent_subscription', ?, NULL)
	`, uuid.NewString(), runID, agentPending, now,
		uuid.NewString(), runID, agentInProgress, activeSessionID, now,
		uuid.NewString(), runID, agentRetryableFailed, now.Add(-5*time.Minute), now.Add(-2*time.Minute),
		uuid.NewString(), runID, agentExhaustedFailed, now.Add(-5*time.Minute), now.Add(-2*time.Minute),
		uuid.NewString(), runID, nodePending, now,
		uuid.NewString(), terminalRunID, terminalRunPending, now); err != nil {
		t.Fatalf("seed sqlite deliveries: %v", err)
	}

	result, err := store.ApplyServeAbandonActiveRunQuiescence(ctx, now)
	if err != nil {
		t.Fatalf("ApplyServeAbandonActiveRunQuiescence: %v", err)
	}
	if result.OperationName != runtimerunquiescence.ServeAbandonOperationName || result.ReasonCode != runtimerunquiescence.ServeAbandonReasonCode || result.ControlledBy != runtimerunquiescence.ServeAbandonControlledBy {
		t.Fatalf("serve abandon result metadata = %#v", result)
	}
	if len(result.Runs) != 2 {
		t.Fatalf("runs = %#v, want running and paused rows cancelled", result.Runs)
	}
	if len(result.Deliveries) != 4 || result.PipelineReceiptCount != 4 {
		t.Fatalf("deliveries=%d pipeline_receipts=%d, want 4/4", len(result.Deliveries), result.PipelineReceiptCount)
	}
	assertSQLiteServeAbandonRun(t, ctx, store, runID)
	assertSQLiteServeAbandonRun(t, ctx, store, pausedRunID)
	assertSQLiteServeAbandonDelivery(t, ctx, store, agentPending, "agent", "agent-a")
	assertSQLiteServeAbandonDelivery(t, ctx, store, agentInProgress, "agent", "agent-a")
	assertSQLiteServeAbandonDelivery(t, ctx, store, agentRetryableFailed, "agent", "agent-a")
	assertSQLiteServeAbandonDelivery(t, ctx, store, nodePending, "node", "node-a")

	if err := store.MarkEventDeliveryInProgress(ctx, agentRetryableFailed, "agent-a", uuid.NewString()); err != nil {
		t.Fatalf("late retryable failed MarkEventDeliveryInProgress: %v", err)
	}
	if err := store.UpsertEventReceipt(ctx, agentInProgress, "agent-a", runtimemanager.ReceiptStatusProcessed, nil); err != nil {
		t.Fatalf("late UpsertEventReceipt: %v", err)
	}
	if err := store.UpsertPipelineReceipt(ctx, agentInProgress, "processed", nil); err != nil {
		t.Fatalf("late UpsertPipelineReceipt: %v", err)
	}
	assertSQLiteServeAbandonDelivery(t, ctx, store, agentRetryableFailed, "agent", "agent-a")
	assertSQLiteServeAbandonReceipt(t, ctx, store, agentInProgress, "agent-a")
	assertSQLiteServeAbandonReceipt(t, ctx, store, agentInProgress, activeRunQuiescencePipelineSubscriberID)

	replay, err := store.ListEventsMissingPipelineReceiptForRun(ctx, runID, now.Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceiptForRun: %v", err)
	}
	if len(replay) != 0 {
		t.Fatalf("missing pipeline replay events = %#v, want none for cancelled run", replay)
	}
	var exhaustedStatus, exhaustedReason string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = ?
	`, agentExhaustedFailed).Scan(&exhaustedStatus, &exhaustedReason); err != nil {
		t.Fatalf("load exhausted failed delivery: %v", err)
	}
	if exhaustedStatus != "failed" || exhaustedReason != "retry_exhausted" {
		t.Fatalf("exhausted failed delivery = %s/%s, want untouched", exhaustedStatus, exhaustedReason)
	}
	var terminalDeliveryStatus, terminalDeliveryReason string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = ?
	`, terminalRunPending).Scan(&terminalDeliveryStatus, &terminalDeliveryReason); err != nil {
		t.Fatalf("load terminal run delivery: %v", err)
	}
	if terminalDeliveryStatus != "pending" || terminalDeliveryReason != "matched_agent_subscription" {
		t.Fatalf("terminal run delivery = %s/%s, want untouched", terminalDeliveryStatus, terminalDeliveryReason)
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

func assertServeAbandonDelivery(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, subscriberType, subscriberID string) {
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
	if status != "dead_letter" || reason != runtimerunquiescence.ServeAbandonReasonCode || activeSession.Valid {
		t.Fatalf("delivery %s/%s/%s = %s/%s active=%v, want serve abandon dead_letter", eventID, subscriberType, subscriberID, status, reason, activeSession.Valid)
	}
	assertServeAbandonReceipt(t, ctx, pg, eventID, subscriberID)
}

func assertServeAbandonReceipt(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, subscriberID string) {
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
	if outcome != "dead_letter" || reason != runtimerunquiescence.ServeAbandonReasonCode {
		t.Fatalf("receipt %s/%s = %s/%s, want serve abandon dead_letter", eventID, subscriberID, outcome, reason)
	}
}

func seedSQLiteServeAbandonEvent(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, runID, name string, at time.Time) string {
	t.Helper()
	eventID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES ('live',
			?, ?, ?, 'global', '{}', 'test', 'agent', ?
		)
	`, eventID, runID, name, at.UTC()); err != nil {
		t.Fatalf("seed sqlite event %s: %v", name, err)
	}
	return eventID
}

func assertSQLiteServeAbandonRun(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, runID string) {
	t.Helper()
	var runStatus, controlStatus, reason, controlledBy string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT r.status, rc.control_status, COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = ?
	`, runID).Scan(&runStatus, &controlStatus, &reason, &controlledBy); err != nil {
		t.Fatalf("load sqlite run/control state: %v", err)
	}
	if runStatus != "cancelled" || controlStatus != "stopped" || reason != runtimerunquiescence.ServeAbandonReasonCode || controlledBy != runtimerunquiescence.ServeAbandonControlledBy {
		t.Fatalf("sqlite run/control = %s/%s/%s/%s, want cancelled/stopped/%s/%s", runStatus, controlStatus, reason, controlledBy, runtimerunquiescence.ServeAbandonReasonCode, runtimerunquiescence.ServeAbandonControlledBy)
	}
}

func assertSQLiteServeAbandonDelivery(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, eventID, subscriberType, subscriberID string) {
	t.Helper()
	var status, reason string
	var activeSession sql.NullString
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, ''), active_session_id
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = ?
		  AND subscriber_id = ?
	`, eventID, subscriberType, subscriberID).Scan(&status, &reason, &activeSession); err != nil {
		t.Fatalf("load sqlite delivery %s/%s/%s: %v", eventID, subscriberType, subscriberID, err)
	}
	if status != "dead_letter" || reason != runtimerunquiescence.ServeAbandonReasonCode || activeSession.Valid {
		t.Fatalf("sqlite delivery %s/%s/%s = %s/%s active=%v, want serve abandon dead_letter", eventID, subscriberType, subscriberID, status, reason, activeSession.Valid)
	}
	assertSQLiteServeAbandonReceipt(t, ctx, store, eventID, subscriberID)
}

func assertSQLiteServeAbandonReceipt(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, eventID, subscriberID string) {
	t.Helper()
	var outcome, reason string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = ?
		  AND subscriber_id = ?
	`, eventID, subscriberID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load sqlite receipt %s/%s: %v", eventID, subscriberID, err)
	}
	if outcome != "dead_letter" || reason != runtimerunquiescence.ServeAbandonReasonCode {
		t.Fatalf("sqlite receipt %s/%s = %s/%s, want serve abandon dead_letter", eventID, subscriberID, outcome, reason)
	}
}

func seedDestructiveResetEvent(t *testing.T, ctx context.Context, pg *PostgresStore, runID, name string) string {
	t.Helper()
	eventID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES ('live',
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
