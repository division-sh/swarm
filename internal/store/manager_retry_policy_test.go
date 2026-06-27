package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const retryPolicyEntityStateRunID = "22222222-2222-2222-2222-222222222222"

func TestUpsertEventReceipt_DeadLettersAfterOneRetry_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.retry_upsert")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	for i := 1; i <= 2; i++ {
		if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, "error", "boom"); err != nil {
			t.Fatalf("upsert receipt error #%d: %v", i, err)
		}

		var status string
		var retryCount int
		if err := pg.DB.QueryRowContext(ctx, `
			SELECT COALESCE(side_effects->>'manager_status', ''), COALESCE((side_effects->>'retry_count')::int, 0)
			FROM event_receipts
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'agent'
			  AND subscriber_id = $2
		`, evt.ID(), agentID).Scan(&status, &retryCount); err != nil {
			t.Fatalf("query receipt after #%d: %v", i, err)
		}

		wantStatus := "error"
		if i == 2 {
			wantStatus = "dead_letter"
		}
		if status != wantStatus || retryCount != i {
			t.Fatalf("after %d errors: got status=%q retry_count=%d, want status=%q retry_count=%d", i, status, retryCount, wantStatus, i)
		}
	}
}

func TestUpsertEventReceipt_PreservesRetryableVsTerminalDeliveryStatus_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.retry_delivery_status")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert retryable error: %v", err)
	}

	var (
		deliveryStatus string
		reasonCode     string
		deliveryRetry  int
		managerStatus  string
	)
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(r.side_effects->>'manager_status', '')
		FROM event_deliveries d
		INNER JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
	`, evt.ID(), agentID).Scan(&deliveryStatus, &reasonCode, &deliveryRetry, &managerStatus); err != nil {
		t.Fatalf("query retryable delivery status: %v", err)
	}
	if deliveryStatus != "failed" || managerStatus != "error" || deliveryRetry != 1 || reasonCode != "handler_error" {
		t.Fatalf("retryable status mismatch: delivery=%q manager=%q retry=%d reason=%q", deliveryStatus, managerStatus, deliveryRetry, reasonCode)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert terminal error: %v", err)
	}
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(r.side_effects->>'manager_status', '')
		FROM event_deliveries d
		INNER JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
	`, evt.ID(), agentID).Scan(&deliveryStatus, &reasonCode, &deliveryRetry, &managerStatus); err != nil {
		t.Fatalf("query terminal delivery status: %v", err)
	}
	if deliveryStatus != "dead_letter" || managerStatus != "dead_letter" || deliveryRetry != 2 || reasonCode != "retry_exhausted" {
		t.Fatalf("terminal status mismatch: delivery=%q manager=%q retry=%d reason=%q", deliveryStatus, managerStatus, deliveryRetry, reasonCode)
	}
}

func TestUpsertEventReceipt_AlignsRetryOwnershipOnCanonicalDelivery_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.retry_alignment.delivery_backed")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert retryable error: %v", err)
	}

	var (
		deliveryStatus string
		deliveryRetry  int
		reasonCode     string
		managerStatus  string
		receiptRetry   int
	)
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.reason_code, ''),
			COALESCE(r.side_effects->>'manager_status', ''),
			COALESCE((r.side_effects->>'retry_count')::int, 0)
		FROM event_deliveries d
		INNER JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
	`, evt.ID(), agentID).Scan(&deliveryStatus, &deliveryRetry, &reasonCode, &managerStatus, &receiptRetry); err != nil {
		t.Fatalf("query retryable aligned state: %v", err)
	}
	if deliveryStatus != "failed" || deliveryRetry != 1 || reasonCode != "handler_error" || managerStatus != "error" || receiptRetry != 1 {
		t.Fatalf("retryable aligned state mismatch: delivery=%q retry=%d reason=%q manager=%q receiptRetry=%d", deliveryStatus, deliveryRetry, reasonCode, managerStatus, receiptRetry)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert exhausted error: %v", err)
	}

	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.reason_code, ''),
			COALESCE(r.side_effects->>'manager_status', ''),
			COALESCE((r.side_effects->>'retry_count')::int, 0)
		FROM event_deliveries d
		INNER JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
	`, evt.ID(), agentID).Scan(&deliveryStatus, &deliveryRetry, &reasonCode, &managerStatus, &receiptRetry); err != nil {
		t.Fatalf("query exhausted aligned state: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryRetry != 2 || reasonCode != "retry_exhausted" || managerStatus != "dead_letter" || receiptRetry != 2 {
		t.Fatalf("exhausted aligned state mismatch: delivery=%q retry=%d reason=%q manager=%q receiptRetry=%d", deliveryStatus, deliveryRetry, reasonCode, managerStatus, receiptRetry)
	}
}

func TestUpsertEventReceipt_FailsClosedOnLegacyReceiptOnlyRetryHistory_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.retry_legacy_receipt_only")
	insertLegacyAgentReceiptState(t, ctx, pg, evt.ID(), agentID, runtimemanager.ReceiptStatusError, 1, "handler_error", "boom", time.Now().Add(-2*time.Minute))

	err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom")
	if err == nil {
		t.Fatal("expected legacy receipt-only retry history write to fail closed")
	}
	if !strings.Contains(err.Error(), "delivery row required") {
		t.Fatalf("upsert legacy receipt-only error = %v, want delivery row required", err)
	}

	var deliveryCount int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, evt.ID(), agentID).Scan(&deliveryCount); err != nil {
		t.Fatalf("query delivery count: %v", err)
	}
	if deliveryCount != 0 {
		t.Fatalf("delivery_count = %d, want 0", deliveryCount)
	}

	var managerStatus string
	var receiptRetry int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			COALESCE(side_effects->>'manager_status', ''),
			COALESCE((side_effects->>'retry_count')::int, 0)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, evt.ID(), agentID).Scan(&managerStatus, &receiptRetry); err != nil {
		t.Fatalf("query receipt after fail-closed write: %v", err)
	}
	if managerStatus != "error" || receiptRetry != 1 {
		t.Fatalf("legacy receipt mutated after fail-closed write: status=%q retry=%d", managerStatus, receiptRetry)
	}
}

func TestUpsertEventReceipt_ConcurrentErrorRetriesAdvanceAtomically_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.concurrent_retry_upsert")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	lockTx, err := pg.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := lockTx.ExecContext(ctx, `
		SELECT 1
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
		FOR UPDATE
	`, evt.ID(), agentID); err != nil {
		t.Fatalf("lock delivery row: %v", err)
	}

	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			errCh <- pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom")
		}()
	}
	time.Sleep(150 * time.Millisecond)
	if err := lockTx.Commit(); err != nil {
		t.Fatalf("release delivery row lock: %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("concurrent upsert #%d: %v", i+1, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for concurrent receipt upserts")
		}
	}

	var (
		deliveryStatus string
		deliveryRetry  int
		managerStatus  string
		receiptRetry   int
	)
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(r.side_effects->>'manager_status', ''),
			COALESCE((r.side_effects->>'retry_count')::int, 0)
		FROM event_deliveries d
		INNER JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
	`, evt.ID(), agentID).Scan(&deliveryStatus, &deliveryRetry, &managerStatus, &receiptRetry); err != nil {
		t.Fatalf("query concurrent retry state: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryRetry != 2 || managerStatus != "dead_letter" || receiptRetry != 2 {
		t.Fatalf("concurrent retry state mismatch: delivery=%q retry=%d manager=%q receipt_retry=%d", deliveryStatus, deliveryRetry, managerStatus, receiptRetry)
	}
}

func TestUpsertEventReceipt_ConcurrentTerminalReceiptsConvergeStandaloneRuntimeRun_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentA := seedEntityAndAgent(t, ctx, pg)
	agentB := "agent-" + uuid.NewString()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       agentB,
			Type:     "test",
			Role:     "test",
			Mode:     "worker",
			Model:    "regular",
			EntityID: entityID,
			Config:   []byte(`{"system_prompt":"x"}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed second agent: %v", err)
	}
	payload, _ := json.Marshal(map[string]any{"k": "v"})
	evt := events.NewRuntimeControlEvent(
		uuid.NewString(),
		events.EventType("platform.paused"),
		"runtime",
		"",
		payload,
		0,
		"",
		"",
		events.EventEnvelope{EntityID: entityID},
		time.Now().Add(-1*time.Hour),
	)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentA, agentB}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	if _, err := pg.DB.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION slow_receipt_delivery_sync()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			PERFORM pg_sleep(0.2);
			RETURN NEW;
		END;
		$$;
	`); err != nil {
		t.Fatalf("create slow trigger function: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		CREATE TRIGGER event_deliveries_slow_terminal_sync
		BEFORE UPDATE ON event_deliveries
		FOR EACH ROW
		EXECUTE FUNCTION slow_receipt_delivery_sync()
	`); err != nil {
		t.Fatalf("create slow trigger: %v", err)
	}

	errCh := make(chan error, 2)
	for _, agentID := range []string{agentA, agentB} {
		agentID := agentID
		go func() {
			errCh <- pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusProcessed, "")
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("concurrent processed receipt #%d: %v", i+1, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for concurrent processed receipts")
		}
	}

	var (
		runStatus      string
		pendingCount   int
		deliveredCount int
	)
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COALESCE(r.status, '')
		FROM events e
		INNER JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = $1::uuid
	`, evt.ID()).Scan(&runStatus); err != nil {
		t.Fatalf("load run status: %v", err)
	}
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status IN ('pending', 'in_progress')),
			COUNT(*) FILTER (WHERE status = 'delivered')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
	`, evt.ID()).Scan(&pendingCount, &deliveredCount); err != nil {
		t.Fatalf("load delivery counts: %v", err)
	}
	if runStatus != "completed" {
		t.Fatalf("run status = %q, want completed", runStatus)
	}
	if pendingCount != 0 || deliveredCount != 2 {
		t.Fatalf("delivery counts = pending:%d delivered:%d, want pending:0 delivered:2", pendingCount, deliveredCount)
	}
}

func TestUpsertEventReceipt_RollsBackReceiptWhenDeliverySyncFails_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.receipt_delivery_atomicity")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	var originalReasonCode string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, evt.ID(), agentID).Scan(&originalReasonCode); err != nil {
		t.Fatalf("load original delivery reason: %v", err)
	}

	if _, err := pg.DB.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION fail_receipt_delivery_sync()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'forced delivery sync failure';
		END;
		$$;
	`); err != nil {
		t.Fatalf("create trigger function: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		CREATE TRIGGER event_deliveries_fail_sync
		BEFORE UPDATE ON event_deliveries
		FOR EACH ROW
		EXECUTE FUNCTION fail_receipt_delivery_sync()
	`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusProcessed, "")
	if err == nil {
		t.Fatal("expected delivery sync failure")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "sync event delivery") {
		t.Fatalf("upsert receipt error = %v, want sync event delivery failure", err)
	}

	var receiptCount int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, evt.ID(), agentID).Scan(&receiptCount); err != nil {
		t.Fatalf("count receipts after rollback: %v", err)
	}
	if receiptCount != 0 {
		t.Fatalf("receipt count after rollback = %d, want 0", receiptCount)
	}

	var (
		deliveryStatus string
		retryCount     int
		reasonCode     string
	)
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(retry_count, 0), COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, evt.ID(), agentID).Scan(&deliveryStatus, &retryCount, &reasonCode); err != nil {
		t.Fatalf("load delivery after rollback: %v", err)
	}
	if deliveryStatus != "pending" || retryCount != 0 || reasonCode != originalReasonCode {
		t.Fatalf("delivery after rollback = status:%q retry:%d reason:%q want reason:%q", deliveryStatus, retryCount, reasonCode, originalReasonCode)
	}
}

func TestListPendingEventsForAgent_RetryBackoff_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.pending_direct")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	since := time.Now().Add(-2 * time.Hour)

	// No receipt: should be immediately pending.
	evts, err := pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (no receipt): %v", err)
	}
	if len(evts) != 1 || evts[0].ID() != evt.ID() {
		t.Fatalf("list pending (no receipt): got %v events, want 1 (%s)", len(evts), evt.ID())
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert retryable receipt: %v", err)
	}
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=1 not ready): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=1 not ready): got %d events, want 0", len(evts))
	}

	rewindCanonicalDeliveryAttempt(t, ctx, pg, evt.ID(), agentID, time.Now().Add(-2*time.Minute))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=1 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID() != evt.ID() {
		t.Fatalf("list pending (retry=1 ready): got %v events, want 1 (%s)", len(evts), evt.ID())
	}

	// After retries are exhausted, the event should not be pending.
	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert dead_letter receipt: %v", err)
	}
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=2 exhausted): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=2 exhausted): got %d events, want 0", len(evts))
	}
}

func TestListPendingSubscribedEvents_RetryBackoff_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.pending_subscribed")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	since := time.Now().Add(-2 * time.Hour)
	subs := []events.EventType{evt.Type()}

	evts, err := pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (no receipt): %v", err)
	}
	if len(evts) != 1 || evts[0].ID() != evt.ID() {
		t.Fatalf("list subscribed pending (no receipt): got %v events, want 1 (%s)", len(evts), evt.ID())
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert retryable receipt: %v", err)
	}
	rewindCanonicalDeliveryAttempt(t, ctx, pg, evt.ID(), agentID, time.Now().Add(-2*time.Minute))
	evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (retry=1 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID() != evt.ID() {
		t.Fatalf("list subscribed pending (retry=1 ready): got %v events, want 1 (%s)", len(evts), evt.ID())
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert dead_letter receipt: %v", err)
	}
	evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (dead_letter): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list subscribed pending (dead_letter): got %d events, want 0", len(evts))
	}
}

func TestPendingAgentEvents_IgnoreLegacyReceiptOnlyRetryOwner_V2(t *testing.T) {
	tests := []struct {
		name       string
		subscribed bool
	}{
		{name: "direct", subscribed: false},
		{name: "subscribed", subscribed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg, cleanup := newTestPostgresStore(t)
			defer cleanup()

			ctx := context.Background()
			entityID, agentID := seedEntityAndAgent(t, ctx, pg)
			evt := seedEvent(t, ctx, pg, entityID, "test.pending_legacy_retry_owner."+tt.name)
			insertLegacyAgentReceiptState(t, ctx, pg, evt.ID(), agentID, runtimemanager.ReceiptStatusError, 1, "handler_error", "boom", time.Now().Add(-2*time.Minute))

			var (
				evts []events.Event
				err  error
			)
			since := time.Now().Add(-2 * time.Hour)
			if tt.subscribed {
				evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, []events.EventType{evt.Type()}, since, 100)
			} else {
				evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
			}
			if err != nil {
				t.Fatalf("list pending legacy receipt-only event: %v", err)
			}
			if len(evts) != 0 {
				t.Fatalf("pending legacy receipt-only event mismatch: got %d events, want 0", len(evts))
			}

			var deliveryCount int
			if err := pg.DB.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM event_deliveries
				WHERE event_id = $1::uuid
				  AND subscriber_type = 'agent'
				  AND subscriber_id = $2
			`, evt.ID(), agentID).Scan(&deliveryCount); err != nil {
				t.Fatalf("query delivery count: %v", err)
			}
			if deliveryCount != 0 {
				t.Fatalf("delivery_count = %d, want 0", deliveryCount)
			}
		})
	}
}

func TestListPendingAgentDeliveryFacts_IgnoresLegacyReceiptOnlyRetryOwner_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.pending_facts.legacy_receipt_only")
	insertLegacyAgentReceiptState(t, ctx, pg, evt.ID(), agentID, runtimemanager.ReceiptStatusError, 1, "handler_error", "boom", time.Now().Add(-2*time.Minute))

	factsByAgent, err := pg.ListPendingAgentDeliveryFacts(ctx, []string{agentID}, time.Now().Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("ListPendingAgentDeliveryFacts: %v", err)
	}
	facts := factsByAgent[agentID]
	if facts.PendingCount != 0 || facts.OldestPendingAgeSec != 0 {
		t.Fatalf("legacy receipt-only pending facts = %+v, want zero", facts)
	}

	var deliveryCount int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, evt.ID(), agentID).Scan(&deliveryCount); err != nil {
		t.Fatalf("query delivery count: %v", err)
	}
	if deliveryCount != 0 {
		t.Fatalf("delivery_count = %d, want 0", deliveryCount)
	}
}

func TestListPendingAgentDeliveryFacts_AlignsWithCanonicalPendingStates_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)

	pendingEvt := seedEvent(t, ctx, pg, entityID, "test.pending_facts.pending")
	retryableEvt := seedEvent(t, ctx, pg, entityID, "test.pending_facts.failed")
	inProgressEvt := seedEvent(t, ctx, pg, entityID, "test.pending_facts.in_progress")
	deadEvt := seedEvent(t, ctx, pg, entityID, "test.pending_facts.dead")

	for _, eventID := range []string{pendingEvt.ID(), retryableEvt.ID(), inProgressEvt.ID(), deadEvt.ID()} {
		if err := pg.InsertEventDeliveries(ctx, eventID, []string{agentID}); err != nil {
			t.Fatalf("insert deliveries for %s: %v", eventID, err)
		}
	}
	if _, err := pg.DB.ExecContext(ctx, `
		UPDATE event_deliveries
		SET created_at = now() - interval '8 minutes'
		WHERE event_id = $1::uuid AND subscriber_id = $2
	`, pendingEvt.ID(), agentID); err != nil {
		t.Fatalf("age pending delivery: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, retryableEvt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert retryable receipt: %v", err)
	}
	rewindCanonicalDeliveryAttempt(t, ctx, pg, retryableEvt.ID(), agentID, time.Now().Add(-2*time.Minute))
	if err := pg.MarkEventDeliveryInProgress(ctx, inProgressEvt.ID(), agentID, ""); err != nil {
		t.Fatalf("mark in progress: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, deadEvt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert dead-letter first error: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, deadEvt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert dead-letter second error: %v", err)
	}

	factsByAgent, err := pg.ListPendingAgentDeliveryFacts(ctx, []string{agentID}, time.Now().Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("ListPendingAgentDeliveryFacts: %v", err)
	}
	facts := factsByAgent[agentID]
	if facts.PendingCount != 3 {
		t.Fatalf("pending_count = %d, want 3", facts.PendingCount)
	}
	if facts.OldestPendingAgeSec <= 0 {
		t.Fatalf("oldest_pending_age_sec = %d, want > 0", facts.OldestPendingAgeSec)
	}
}

func TestListPendingAgentDeliveryDetails_PagesCanonicalQueueTruth_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)

	pendingEvt := seedEvent(t, ctx, pg, entityID, "test.pending_details.pending")
	retryableEvt := seedEvent(t, ctx, pg, entityID, "test.pending_details.failed")
	inProgressEvt := seedEvent(t, ctx, pg, entityID, "test.pending_details.in_progress")
	deadEvt := seedEvent(t, ctx, pg, entityID, "test.pending_details.dead")
	deliveredEvt := seedEvent(t, ctx, pg, entityID, "test.pending_details.delivered")
	legacyEvt := seedEvent(t, ctx, pg, entityID, "test.pending_details.legacy_receipt_only")

	for _, eventID := range []string{pendingEvt.ID(), retryableEvt.ID(), inProgressEvt.ID(), deadEvt.ID(), deliveredEvt.ID()} {
		if err := pg.InsertEventDeliveries(ctx, eventID, []string{agentID}); err != nil {
			t.Fatalf("insert deliveries for %s: %v", eventID, err)
		}
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	eventTimes := map[string]time.Time{
		pendingEvt.ID():    now.Add(-30 * time.Minute),
		retryableEvt.ID():  now.Add(-20 * time.Minute),
		inProgressEvt.ID(): now.Add(-10 * time.Minute),
		deadEvt.ID():       now.Add(-5 * time.Minute),
		deliveredEvt.ID():  now.Add(-4 * time.Minute),
		legacyEvt.ID():     now.Add(-3 * time.Minute),
	}
	for eventID, createdAt := range eventTimes {
		if _, err := pg.DB.ExecContext(ctx, `
			UPDATE events
			SET created_at = $2
			WHERE event_id = $1::uuid
		`, eventID, createdAt); err != nil {
			t.Fatalf("set event created_at for %s: %v", eventID, err)
		}
	}

	if err := pg.UpsertEventReceipt(ctx, retryableEvt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert retryable receipt: %v", err)
	}
	rewindCanonicalDeliveryAttempt(t, ctx, pg, retryableEvt.ID(), agentID, now.Add(-2*time.Minute))
	if err := pg.MarkEventDeliveryInProgress(ctx, inProgressEvt.ID(), agentID, ""); err != nil {
		t.Fatalf("mark in progress: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, deadEvt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert dead-letter first error: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, deadEvt.ID(), agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert dead-letter second error: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, deliveredEvt.ID(), agentID, runtimemanager.ReceiptStatusProcessed, "done"); err != nil {
		t.Fatalf("upsert delivered receipt: %v", err)
	}
	insertLegacyAgentReceiptState(t, ctx, pg, legacyEvt.ID(), agentID, runtimemanager.ReceiptStatusError, 1, "handler_error", "boom", now.Add(-2*time.Minute))

	firstPage, err := pg.ListPendingAgentDeliveryDetails(ctx, store.PendingAgentDeliveryListOptions{
		AgentID: agentID,
		Since:   now.Add(-2 * time.Hour),
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("ListPendingAgentDeliveryDetails first page: %v", err)
	}
	if firstPage.PendingCount != 3 {
		t.Fatalf("pending_count = %d, want 3", firstPage.PendingCount)
	}
	if firstPage.OldestPendingAgeSec <= 0 {
		t.Fatalf("oldest_pending_age_sec = %d, want > 0", firstPage.OldestPendingAgeSec)
	}
	if len(firstPage.PendingDeliveries) != 2 || firstPage.NextCursor == "" {
		t.Fatalf("first page = %+v, want 2 rows with next cursor", firstPage)
	}
	assertPendingDeliveryDetail(t, firstPage.PendingDeliveries[0], pendingEvt.ID(), string(pendingEvt.Type()), eventTimes[pendingEvt.ID()], 0)
	assertPendingDeliveryDetail(t, firstPage.PendingDeliveries[1], retryableEvt.ID(), string(retryableEvt.Type()), eventTimes[retryableEvt.ID()], 1)

	secondPage, err := pg.ListPendingAgentDeliveryDetails(ctx, store.PendingAgentDeliveryListOptions{
		AgentID: agentID,
		Since:   now.Add(-2 * time.Hour),
		Limit:   2,
		Cursor:  firstPage.NextCursor,
	})
	if err != nil {
		t.Fatalf("ListPendingAgentDeliveryDetails second page: %v", err)
	}
	if secondPage.PendingCount != 3 || secondPage.OldestPendingAgeSec != firstPage.OldestPendingAgeSec {
		t.Fatalf("second page summary = %+v, want stable full-count summary", secondPage)
	}
	if len(secondPage.PendingDeliveries) != 1 || secondPage.NextCursor != "" {
		t.Fatalf("second page = %+v, want final single row", secondPage)
	}
	assertPendingDeliveryDetail(t, secondPage.PendingDeliveries[0], inProgressEvt.ID(), string(inProgressEvt.Type()), eventTimes[inProgressEvt.ID()], 0)

	runtimeEvents, err := pg.ListPendingEventsForAgent(ctx, agentID, now.Add(-2*time.Hour), 2)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(runtimeEvents) != 2 || runtimeEvents[0].ID() != pendingEvt.ID() || runtimeEvents[1].ID() != retryableEvt.ID() {
		t.Fatalf("runtime pending events = %#v, want shared first detail page order", runtimeEvents)
	}

	_, err = pg.ListPendingAgentDeliveryDetails(ctx, store.PendingAgentDeliveryListOptions{
		AgentID: agentID,
		Cursor:  "not-a-valid-cursor",
	})
	if !errors.Is(err, store.ErrInvalidPendingAgentDeliveryCursor) {
		t.Fatalf("bad cursor error = %v, want ErrInvalidPendingAgentDeliveryCursor", err)
	}
}

func assertPendingDeliveryDetail(t *testing.T, got store.PendingAgentDeliveryDetail, eventID, eventName string, enqueuedAt time.Time, attempts int) {
	t.Helper()
	if got.EventID != eventID || got.EventName != eventName || !got.EnqueuedAt.Equal(enqueuedAt) || got.Attempts != attempts {
		t.Fatalf("pending delivery detail = %+v, want event_id=%s event_name=%s enqueued_at=%s attempts=%d", got, eventID, eventName, enqueuedAt, attempts)
	}
	if got.Event.ID() != eventID {
		t.Fatalf("pending delivery embedded event id = %q, want %q", got.Event.ID(), eventID)
	}
}

func TestListPendingAgentDeliveryFacts_UsesFullPendingHorizon_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.pending_facts.full_horizon")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		UPDATE events
		SET created_at = now() - interval '45 days'
		WHERE event_id = $1::uuid
	`, evt.ID()); err != nil {
		t.Fatalf("age event: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		UPDATE event_deliveries
		SET created_at = now() - interval '45 days'
		WHERE event_id = $1::uuid
		  AND subscriber_id = $2
	`, evt.ID(), agentID); err != nil {
		t.Fatalf("age delivery: %v", err)
	}

	factsByAgent, err := pg.ListPendingAgentDeliveryFacts(ctx, []string{agentID}, time.Time{})
	if err != nil {
		t.Fatalf("ListPendingAgentDeliveryFacts: %v", err)
	}
	facts := factsByAgent[agentID]
	if facts.PendingCount != 1 {
		t.Fatalf("pending_count = %d, want 1", facts.PendingCount)
	}
	if facts.OldestPendingAgeSec < 30*24*60*60 {
		t.Fatalf("oldest_pending_age_sec = %d, want at least 30 days", facts.OldestPendingAgeSec)
	}
}

func TestListPendingEventsForAgent_InProgressWithoutReceipt_RemainsPending(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.pending_in_progress")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	if err := pg.MarkEventDeliveryInProgress(ctx, evt.ID(), agentID, ""); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress: %v", err)
	}

	evts, err := pg.ListPendingEventsForAgent(ctx, agentID, time.Now().Add(-2*time.Hour), 100)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(evts) != 1 || evts[0].ID() != evt.ID() {
		t.Fatalf("in_progress without receipt pending events = %#v, want [%s]", evts, evt.ID())
	}
}

func TestMarkEventDeliveryInProgress_AllowsRetryableFailedDeliveryClaim_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.retry_claim.failed")
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, "retryable"); err != nil {
		t.Fatalf("upsert retryable receipt: %v", err)
	}
	rewindCanonicalDeliveryAttempt(t, ctx, pg, evt.ID(), agentID, time.Now().Add(-2*time.Minute))

	if err := pg.MarkEventDeliveryInProgress(ctx, evt.ID(), agentID, ""); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress retryable failed delivery: %v", err)
	}

	var status, reason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_id = $2
	`, evt.ID(), agentID).Scan(&status, &reason); err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if status != "in_progress" || reason != "agent_processing" {
		t.Fatalf("delivery = %s/%s, want in_progress/agent_processing", status, reason)
	}
}

func TestListPendingSubscribedEvents_UsesCanonicalMatcherParity(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)

	deep := seedEvent(t, ctx, pg, entityID, "operating/child/grandchild/opco.launched")
	segment := seedEvent(t, ctx, pg, entityID, "review.ready")
	tooDeep := seedEvent(t, ctx, pg, entityID, "scoring/a/b")
	invalidPattern := seedEvent(t, ctx, pg, entityID, "budget.alert")
	for _, eventID := range []string{deep.ID(), segment.ID(), tooDeep.ID(), invalidPattern.ID()} {
		if err := pg.InsertEventDeliveries(ctx, eventID, []string{agentID}); err != nil {
			t.Fatalf("insert deliveries for %s: %v", eventID, err)
		}
	}

	since := time.Now().Add(-2 * time.Hour)
	tests := []struct {
		name          string
		subscriptions []events.EventType
		eventType     string
		wantIDs       []string
	}{
		{
			name:          "recursive wildcard matches deep scoped event",
			subscriptions: []events.EventType{"operating/**/opco.launched"},
			eventType:     string(deep.Type()),
			wantIDs:       []string{deep.ID()},
		},
		{
			name:          "segment glob matches canonical runtime semantics",
			subscriptions: []events.EventType{"*.ready"},
			eventType:     string(segment.Type()),
			wantIDs:       []string{segment.ID()},
		},
		{
			name:          "single segment wildcard does not span multiple segments",
			subscriptions: []events.EventType{"scoring/*"},
			eventType:     string(tooDeep.Type()),
			wantIDs:       nil,
		},
		{
			name:          "invalid pattern fails closed",
			subscriptions: []events.EventType{"["},
			eventType:     string(invalidPattern.Type()),
			wantIDs:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runtimebus.RouteMatches(string(tt.subscriptions[0]), tt.eventType); got != (len(tt.wantIDs) > 0) {
				t.Fatalf("runtime matcher parity mismatch for %q vs %q: got %v want %v", tt.subscriptions[0], tt.eventType, got, len(tt.wantIDs) > 0)
			}
			evts, err := pg.ListPendingSubscribedEvents(ctx, agentID, tt.subscriptions, since, 100)
			if err != nil {
				t.Fatalf("ListPendingSubscribedEvents: %v", err)
			}
			gotIDs := make([]string, 0, len(evts))
			for _, evt := range evts {
				gotIDs = append(gotIDs, evt.ID())
			}
			if len(gotIDs) != len(tt.wantIDs) {
				t.Fatalf("subscriptions=%v got=%v want=%v", tt.subscriptions, gotIDs, tt.wantIDs)
			}
			for i := range gotIDs {
				if gotIDs[i] != tt.wantIDs[i] {
					t.Fatalf("subscriptions=%v got=%v want=%v", tt.subscriptions, gotIDs, tt.wantIDs)
				}
			}
		})
	}
}

func rewindCanonicalDeliveryAttempt(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID string, when time.Time) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `
		UPDATE event_deliveries
		SET delivered_at = $3
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, agentID, when.UTC()); err != nil {
		t.Fatalf("rewind event_deliveries.delivered_at: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		UPDATE event_receipts
		SET processed_at = $3
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, agentID, when.UTC()); err != nil {
		t.Fatalf("rewind event_receipts.processed_at: %v", err)
	}
}

func newTestPostgresStore(t *testing.T) (*store.PostgresStore, func()) {
	t.Helper()
	dsn, _, cleanup := testutil.StartPostgres(t)
	appDSN := dsn
	pg, err := store.NewPostgresStore(appDSN)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := pg.Ping(context.Background()); err != nil {
		_ = pg.DB.Close()
		t.Fatalf("ping app db: %v", err)
	}
	return pg, func() {
		_ = pg.DB.Close()
		cleanup()
	}
}

func seedEntityAndAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore) (entityID, agentID string) {
	t.Helper()

	entityID = uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, retryPolicyEntityStateRunID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('retry-policy-entity', 'test', 'static', '{"instance_kind":"entity","workflow_version":"v1"}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'retry-policy-entity', 'default', 'retry-policy', 'Store Retry Policy Test', 'approved',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now())
	`, retryPolicyEntityStateRunID, entityID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}

	agentID = "agent-" + uuid.NewString()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       agentID,
			Type:     "test",
			Role:     "test",
			Mode:     "worker",
			Model:    "regular",
			EntityID: entityID,
			Config:   []byte(`{"system_prompt":"x"}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	return entityID, agentID
}

func seedEvent(t *testing.T, ctx context.Context, pg *store.PostgresStore, entityID, eventType string) events.Event {
	t.Helper()

	payload, _ := json.Marshal(map[string]any{"k": "v"})
	evt := eventtest.WithEntityID((eventtest.Projection(uuid.NewString(),
		events.EventType(eventType),
		"store-test", "", payload, 0, "", "", events.EventEnvelope{}, time.Now().Add(-1*time.Hour))), entityID)

	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("append event: %v", err)
	}
	return evt
}

func insertLegacyAgentReceiptState(
	t *testing.T,
	ctx context.Context,
	pg *store.PostgresStore,
	eventID, agentID string,
	status runtimemanager.ReceiptStatus,
	retryCount int,
	reasonCode, errText string,
	processedAt time.Time,
) {
	t.Helper()

	outcome := "processed"
	switch status {
	case runtimemanager.ReceiptStatusError:
		outcome = "dead_letter"
	case runtimemanager.ReceiptStatusDeadLetter:
		outcome = "dead_letter"
	default:
		outcome = "success"
	}
	sideEffects := fmt.Sprintf(
		`{"manager_status":%q,"reason_code":%q,"retry_count":%d,"error":%q}`,
		string(status),
		strings.TrimSpace(reasonCode),
		retryCount,
		strings.TrimSpace(errText),
	)
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'agent', $2, e.entity_id, e.flow_instance,
			$3, NULLIF($4, ''), $5::jsonb, $6
		FROM events e
		WHERE e.event_id = $1::uuid
	`, eventID, agentID, outcome, strings.TrimSpace(reasonCode), sideEffects, processedAt.UTC()); err != nil {
		t.Fatalf("insert legacy agent receipt state: %v", err)
	}
}
