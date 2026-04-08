package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestUpsertEventReceipt_DeadLettersAfterOneRetry_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.retry_upsert")

	for i := 1; i <= 2; i++ {
		if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, "error", "boom"); err != nil {
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
		`, evt.ID, agentID).Scan(&status, &retryCount); err != nil {
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
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
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
	`, evt.ID, agentID).Scan(&deliveryStatus, &reasonCode, &deliveryRetry, &managerStatus); err != nil {
		t.Fatalf("query retryable delivery status: %v", err)
	}
	if deliveryStatus != "failed" || managerStatus != "error" || deliveryRetry != 1 || reasonCode != "handler_error" {
		t.Fatalf("retryable status mismatch: delivery=%q manager=%q retry=%d reason=%q", deliveryStatus, managerStatus, deliveryRetry, reasonCode)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
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
	`, evt.ID, agentID).Scan(&deliveryStatus, &reasonCode, &deliveryRetry, &managerStatus); err != nil {
		t.Fatalf("query terminal delivery status: %v", err)
	}
	if deliveryStatus != "dead_letter" || managerStatus != "dead_letter" || deliveryRetry != 2 || reasonCode != "retry_exhausted" {
		t.Fatalf("terminal status mismatch: delivery=%q manager=%q retry=%d reason=%q", deliveryStatus, managerStatus, deliveryRetry, reasonCode)
	}
}

func TestUpsertEventReceipt_AlignsRetryOwnershipAcrossDeliveryBackedAndReceiptOnly_V2(t *testing.T) {
	tests := []struct {
		name           string
		insertDelivery bool
	}{
		{name: "delivery_backed", insertDelivery: true},
		{name: "receipt_only_normalized_to_delivery", insertDelivery: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg, cleanup := newTestPostgresStore(t)
			defer cleanup()

			ctx := context.Background()
			entityID, agentID := seedEntityAndAgent(t, ctx, pg)
			evt := seedEvent(t, ctx, pg, entityID, "test.retry_alignment."+tt.name)
			if tt.insertDelivery {
				if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{agentID}); err != nil {
					t.Fatalf("insert deliveries: %v", err)
				}
			}

			if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
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
			`, evt.ID, agentID).Scan(&deliveryStatus, &deliveryRetry, &reasonCode, &managerStatus, &receiptRetry); err != nil {
				t.Fatalf("query retryable aligned state: %v", err)
			}
			if deliveryStatus != "failed" || deliveryRetry != 1 || reasonCode != "handler_error" || managerStatus != "error" || receiptRetry != 1 {
				t.Fatalf("retryable aligned state mismatch: delivery=%q retry=%d reason=%q manager=%q receiptRetry=%d", deliveryStatus, deliveryRetry, reasonCode, managerStatus, receiptRetry)
			}

			if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
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
			`, evt.ID, agentID).Scan(&deliveryStatus, &deliveryRetry, &reasonCode, &managerStatus, &receiptRetry); err != nil {
				t.Fatalf("query exhausted aligned state: %v", err)
			}
			if deliveryStatus != "dead_letter" || deliveryRetry != 2 || reasonCode != "retry_exhausted" || managerStatus != "dead_letter" || receiptRetry != 2 {
				t.Fatalf("exhausted aligned state mismatch: delivery=%q retry=%d reason=%q manager=%q receiptRetry=%d", deliveryStatus, deliveryRetry, reasonCode, managerStatus, receiptRetry)
			}
		})
	}
}

func TestUpsertEventReceipt_PreservesLegacyReceiptOnlyRetryHistory_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.retry_legacy_receipt_only")
	insertLegacyAgentReceiptState(t, ctx, pg, evt.ID, agentID, runtimemanager.ReceiptStatusError, 1, "handler_error", "boom", time.Now().Add(-2*time.Minute))

	if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert exhausted legacy receipt-only error: %v", err)
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
	`, evt.ID, agentID).Scan(&deliveryStatus, &deliveryRetry, &reasonCode, &managerStatus, &receiptRetry); err != nil {
		t.Fatalf("query legacy exhausted aligned state: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryRetry != 2 || reasonCode != "retry_exhausted" || managerStatus != "dead_letter" || receiptRetry != 2 {
		t.Fatalf("legacy exhausted aligned state mismatch: delivery=%q retry=%d reason=%q manager=%q receiptRetry=%d", deliveryStatus, deliveryRetry, reasonCode, managerStatus, receiptRetry)
	}
}

func TestUpsertEventReceipt_ConcurrentErrorRetriesAdvanceAtomically_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.concurrent_retry_upsert")
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{agentID}); err != nil {
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
	`, evt.ID, agentID); err != nil {
		t.Fatalf("lock delivery row: %v", err)
	}

	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			errCh <- pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom")
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
	`, evt.ID, agentID).Scan(&deliveryStatus, &deliveryRetry, &managerStatus, &receiptRetry); err != nil {
		t.Fatalf("query concurrent retry state: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryRetry != 2 || managerStatus != "dead_letter" || receiptRetry != 2 {
		t.Fatalf("concurrent retry state mismatch: delivery=%q retry=%d manager=%q receipt_retry=%d", deliveryStatus, deliveryRetry, managerStatus, receiptRetry)
	}
}

func TestUpsertEventReceipt_RollsBackReceiptWhenDeliverySyncFails_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.receipt_delivery_atomicity")
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	var originalReasonCode string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, evt.ID, agentID).Scan(&originalReasonCode); err != nil {
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

	err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusProcessed, "")
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
	`, evt.ID, agentID).Scan(&receiptCount); err != nil {
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
	`, evt.ID, agentID).Scan(&deliveryStatus, &retryCount, &reasonCode); err != nil {
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
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	since := time.Now().Add(-2 * time.Hour)

	// No receipt: should be immediately pending.
	evts, err := pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (no receipt): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list pending (no receipt): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert retryable receipt: %v", err)
	}
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=1 not ready): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=1 not ready): got %d events, want 0", len(evts))
	}

	rewindCanonicalDeliveryAttempt(t, ctx, pg, evt.ID, agentID, time.Now().Add(-2*time.Minute))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=1 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list pending (retry=1 ready): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	// After retries are exhausted, the event should not be pending.
	if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
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

	since := time.Now().Add(-2 * time.Hour)
	subs := []events.EventType{evt.Type}

	evts, err := pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (no receipt): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list subscribed pending (no receipt): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
		t.Fatalf("upsert retryable receipt: %v", err)
	}
	rewindCanonicalDeliveryAttempt(t, ctx, pg, evt.ID, agentID, time.Now().Add(-2*time.Minute))
	evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (retry=1 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list subscribed pending (retry=1 ready): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, runtimemanager.ReceiptStatusError, "boom"); err != nil {
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

func TestPendingAgentEvents_NormalizesLegacyReceiptOnlyRetryOwner_V2(t *testing.T) {
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
			insertLegacyAgentReceiptState(t, ctx, pg, evt.ID, agentID, runtimemanager.ReceiptStatusError, 1, "handler_error", "boom", time.Now().Add(-2*time.Minute))

			var (
				evts []events.Event
				err  error
			)
			since := time.Now().Add(-2 * time.Hour)
			if tt.subscribed {
				evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, []events.EventType{evt.Type}, since, 100)
			} else {
				evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
			}
			if err != nil {
				t.Fatalf("list pending legacy receipt-only event: %v", err)
			}
			if len(evts) != 1 || evts[0].ID != evt.ID {
				t.Fatalf("pending legacy receipt-only event mismatch: got %d events, want 1 (%s)", len(evts), evt.ID)
			}

			var (
				deliveryStatus string
				deliveryRetry  int
				reasonCode     string
			)
			if err := pg.DB.QueryRowContext(ctx, `
				SELECT
					COALESCE(status, ''),
					COALESCE(retry_count, 0),
					COALESCE(reason_code, '')
				FROM event_deliveries
				WHERE event_id = $1::uuid
				  AND subscriber_type = 'agent'
				  AND subscriber_id = $2
			`, evt.ID, agentID).Scan(&deliveryStatus, &deliveryRetry, &reasonCode); err != nil {
				t.Fatalf("query normalized legacy delivery: %v", err)
			}
			if deliveryStatus != "failed" || deliveryRetry != 1 || reasonCode != "handler_error" {
				t.Fatalf("normalized legacy delivery mismatch: status=%q retry=%d reason=%q", deliveryStatus, deliveryRetry, reasonCode)
			}
		})
	}
}

func TestListPendingEventsForAgent_InProgressWithoutReceipt_RemainsPending(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.pending_in_progress")
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	if err := pg.MarkEventDeliveryInProgress(ctx, evt.ID, agentID, ""); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress: %v", err)
	}

	evts, err := pg.ListPendingEventsForAgent(ctx, agentID, time.Now().Add(-2*time.Hour), 100)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("in_progress without receipt pending events = %#v, want [%s]", evts, evt.ID)
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
			eventType:     string(deep.Type),
			wantIDs:       []string{deep.ID},
		},
		{
			name:          "segment glob matches canonical runtime semantics",
			subscriptions: []events.EventType{"*.ready"},
			eventType:     string(segment.Type),
			wantIDs:       []string{segment.ID},
		},
		{
			name:          "single segment wildcard does not span multiple segments",
			subscriptions: []events.EventType{"scoring/*"},
			eventType:     string(tooDeep.Type),
			wantIDs:       nil,
		},
		{
			name:          "invalid pattern fails closed",
			subscriptions: []events.EventType{"["},
			eventType:     string(invalidPattern.Type),
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
				gotIDs = append(gotIDs, evt.ID)
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
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('retry-policy-entity', 'test', 'static', '{"instance_kind":"entity","workflow_version":"v1"}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES ($1::uuid, 'retry-policy-entity', 'default', 'retry-policy', 'Store Retry Policy Test', 'approved',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now())
	`, entityID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}

	agentID = "agent-" + uuid.NewString()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       agentID,
			Type:     "test",
			Role:     "test",
			Mode:     "worker",
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
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: "store-test",
		Payload:     payload,
		CreatedAt:   time.Now().Add(-1 * time.Hour),
	}).WithEntityID(entityID)
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
