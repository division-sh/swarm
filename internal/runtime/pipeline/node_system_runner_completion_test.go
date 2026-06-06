package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type systemNodeCompletionBus struct {
	converged []string
}

func (*systemNodeCompletionBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (*systemNodeCompletionBus) Publish(context.Context, events.Event) error { return nil }

func (b *systemNodeCompletionBus) ConvergeNormalRunCompletionForEvent(_ context.Context, eventID string) error {
	b.converged = append(b.converged, eventID)
	return nil
}

func TestSystemNodeRunner_MarkProcessedSettlesNodeDeliveryAndTriggersNormalRunCompletion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID)
	bus := &systemNodeCompletionBus{}
	handlerCalled := false
	handlerObservedStatus := ""
	handlerObservedReason := ""
	runner := newSystemNodeRunner("terminal-node", bus, db, func() []events.EventType {
		return []events.EventType{"example.started"}
	}, func(ctx context.Context, evt events.Event) error {
		handlerCalled = true
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), COALESCE(reason_code, '')
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = 'terminal-node'
		`, eventID).Scan(&handlerObservedStatus, &handlerObservedReason); err != nil {
			t.Fatalf("load node delivery during handler: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE entity_state
			SET current_state = 'done',
			    updated_at = now()
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
		`, runID, entityID); err != nil {
			t.Fatalf("mark entity terminal: %v", err)
		}
		return nil
	}, func(context.Context) (bool, error) { return true, nil })

	runner.ProcessEventForTest(ctx, (events.NewProjectionEvent(eventID,
		"example.started", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID(entityID))

	if !handlerCalled {
		t.Fatal("handler was not called")
	}
	if handlerObservedStatus != "in_progress" || handlerObservedReason != "node_processing" {
		t.Fatalf("handler observed node delivery = %s/%s, want in_progress/node_processing", handlerObservedStatus, handlerObservedReason)
	}
	if len(bus.converged) != 1 || bus.converged[0] != eventID {
		t.Fatalf("normal run convergence events = %#v, want %s", bus.converged, eventID)
	}
	var (
		status      string
		reason      string
		deliveredAt sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), delivered_at
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&status, &reason, &deliveredAt); err != nil {
		t.Fatalf("load node delivery: %v", err)
	}
	if status != "delivered" || reason != "node_processed" || !deliveredAt.Valid {
		t.Fatalf("node delivery = status:%q reason:%q delivered:%v, want delivered node_processed with delivered_at", status, reason, deliveredAt.Valid)
	}
	var receiptOutcome string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(outcome, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&receiptOutcome); err != nil {
		t.Fatalf("load node receipt: %v", err)
	}
	if receiptOutcome != "no_op" {
		t.Fatalf("node receipt outcome = %q, want no_op", receiptOutcome)
	}
}

func TestSystemNodeRunner_RetryableFailureWritesFailedBeforeRetry(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID)
	bus := &systemNodeCompletionBus{}
	attempts := 0
	var (
		backoffStatus string
		backoffReason string
		backoffError  string
		backoffRetry  int
	)
	runner := newSystemNodeRunner("terminal-node", bus, db, func() []events.EventType {
		return []events.EventType{"example.started"}
	}, func(context.Context, events.Event) error {
		attempts++
		if attempts == 1 {
			return errors.New("temporary node failure")
		}
		return nil
	}, func(context.Context) (bool, error) { return true, nil })
	runner.SetRetryPolicyForTest(2, func(int) time.Duration {
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(last_error, ''), COALESCE(retry_count, 0)
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = 'terminal-node'
		`, eventID).Scan(&backoffStatus, &backoffReason, &backoffError, &backoffRetry); err != nil {
			t.Fatalf("load node delivery during retry backoff: %v", err)
		}
		return 0
	})

	runner.ProcessEventForTest(ctx, (events.NewProjectionEvent(eventID,
		"example.started", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID(entityID))

	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if backoffStatus != "failed" || backoffReason != "handler_error" || backoffRetry != 1 || backoffError == "" {
		t.Fatalf("retry backoff delivery = %s/%s retry=%d err=%q, want failed/handler_error retry=1 with error", backoffStatus, backoffReason, backoffRetry, backoffError)
	}
	var finalStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&finalStatus); err != nil {
		t.Fatalf("load final node delivery: %v", err)
	}
	if finalStatus != "delivered" {
		t.Fatalf("final node delivery status = %q, want delivered", finalStatus)
	}
}

func TestSystemNodeRunner_RetryableFailureExhaustsConfiguredRetryLimit(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID)
	bus := &systemNodeCompletionBus{}
	attempts := 0
	runner := newSystemNodeRunner("terminal-node", bus, db, func() []events.EventType {
		return []events.EventType{"example.started"}
	}, func(context.Context, events.Event) error {
		attempts++
		return errors.New("temporary node failure")
	}, func(context.Context) (bool, error) { return true, nil })
	runner.SetRetryPolicyForTest(DefaultSystemNodeRetryLimit, func(int) time.Duration { return 0 })

	runner.ProcessEventForTest(ctx, (events.NewProjectionEvent(eventID,
		"example.started", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID(entityID))

	if attempts != DefaultSystemNodeRetryLimit {
		t.Fatalf("attempts = %d, want configured retry limit %d", attempts, DefaultSystemNodeRetryLimit)
	}
	var (
		deliveryStatus string
		deliveryReason string
		deliveryRetry  int
		receiptOutcome string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(d.status, ''), COALESCE(d.reason_code, ''), COALESCE(d.retry_count, 0), COALESCE(r.outcome, '')
		FROM event_deliveries d
		LEFT JOIN event_receipts r
		  ON r.event_id = d.event_id
		 AND r.subscriber_type = d.subscriber_type
		 AND r.subscriber_id = d.subscriber_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = 'terminal-node'
	`, eventID).Scan(&deliveryStatus, &deliveryReason, &deliveryRetry, &receiptOutcome); err != nil {
		t.Fatalf("load exhausted node delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryReason != "retry_exhausted" || deliveryRetry != DefaultSystemNodeRetryLimit || receiptOutcome != "dead_letter" {
		t.Fatalf("exhausted node delivery = %s/%s retry=%d receipt=%q, want dead_letter/retry_exhausted retry=%d receipt dead_letter", deliveryStatus, deliveryReason, deliveryRetry, receiptOutcome, DefaultSystemNodeRetryLimit)
	}
}

func TestSystemNodeRunner_PipelineNamedNodeDoesNotMaskPlatformReceipt(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID, "pipeline")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		) VALUES (
			$1::uuid, 'platform', 'pipeline', 'success', 'processed', '{}'::jsonb, now()
		)
	`, eventID); err != nil {
		t.Fatalf("seed platform pipeline receipt: %v", err)
	}

	sideEffects := systemNodeProcessedReceiptSideEffects("pipeline", eventID)
	if err := persistSystemNodeProcessedReceiptAndSettleDelivery(ctx, db, "pipeline", eventID, sideEffects); err != nil {
		t.Fatalf("persistSystemNodeProcessedReceiptAndSettleDelivery: %v", err)
	}

	var platformReceipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&platformReceipts); err != nil {
		t.Fatalf("count platform receipt: %v", err)
	}
	if platformReceipts != 1 {
		t.Fatalf("platform pipeline receipts = %d, want 1", platformReceipts)
	}
	var nodeReceipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&nodeReceipts); err != nil {
		t.Fatalf("count node receipt: %v", err)
	}
	if nodeReceipts != 1 {
		t.Fatalf("node pipeline receipts = %d, want 1", nodeReceipts)
	}
	var deliveryStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&deliveryStatus); err != nil {
		t.Fatalf("load pipeline node delivery: %v", err)
	}
	if deliveryStatus != "delivered" {
		t.Fatalf("pipeline node delivery status = %q, want delivered", deliveryStatus)
	}
}

func TestSystemNodeProcessedSettlementFailsWithoutNodeDeliveryAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)

	sideEffects := systemNodeProcessedReceiptSideEffects("terminal-node", eventID)
	err := persistSystemNodeProcessedReceiptAndSettleDelivery(ctx, db, "terminal-node", eventID, sideEffects)
	if !errors.Is(err, ErrSystemNodeDeliveryAuthorityMissing) {
		t.Fatalf("persistSystemNodeProcessedReceiptAndSettleDelivery error = %v, want ErrSystemNodeDeliveryAuthorityMissing", err)
	}
	var deliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&deliveries); err != nil {
		t.Fatalf("count node deliveries: %v", err)
	}
	if deliveries != 0 {
		t.Fatalf("node deliveries = %d, want 0", deliveries)
	}
	var receipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&receipts); err != nil {
		t.Fatalf("count node receipts: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("node receipts = %d, want 0", receipts)
	}
}

func TestSystemNodeProcessedSettlementFailsWithTerminalNodeDeliveryAuthority(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     string
		retryCount int
	}{
		{name: "dead_letter", status: "dead_letter", retryCount: 2},
		{name: "retry_exhausted_failed", status: "failed", retryCount: DefaultSystemNodeRetryLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			ctx := context.Background()
			runID := uuid.NewString()
			eventID := uuid.NewString()
			entityID := uuid.NewString()
			seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)
			if _, err := db.ExecContext(ctx, `
				INSERT INTO event_deliveries (
					run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
				) VALUES (
					$1::uuid, $2::uuid, 'node', 'terminal-node', $3, $4, 'terminal_test', now()
				)
			`, runID, eventID, tc.status, tc.retryCount); err != nil {
				t.Fatalf("seed terminal node delivery: %v", err)
			}

			sideEffects := systemNodeProcessedReceiptSideEffects("terminal-node", eventID)
			err := persistSystemNodeProcessedReceiptAndSettleDelivery(ctx, db, "terminal-node", eventID, sideEffects)
			if !errors.Is(err, ErrSystemNodeDeliveryAuthorityMissing) {
				t.Fatalf("persistSystemNodeProcessedReceiptAndSettleDelivery error = %v, want ErrSystemNodeDeliveryAuthorityMissing", err)
			}
			var status string
			var retryCount int
			if err := db.QueryRowContext(ctx, `
				SELECT COALESCE(status, ''), COALESCE(retry_count, 0)
				FROM event_deliveries
				WHERE event_id = $1::uuid
				  AND subscriber_type = 'node'
				  AND subscriber_id = 'terminal-node'
			`, eventID).Scan(&status, &retryCount); err != nil {
				t.Fatalf("load terminal delivery: %v", err)
			}
			if status != tc.status || retryCount != tc.retryCount {
				t.Fatalf("terminal delivery = %s/%d, want %s/%d", status, retryCount, tc.status, tc.retryCount)
			}
			var receipts int
			if err := db.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM event_receipts
				WHERE event_id = $1::uuid
				  AND subscriber_type = 'node'
				  AND subscriber_id = 'terminal-node'
			`, eventID).Scan(&receipts); err != nil {
				t.Fatalf("count node receipts: %v", err)
			}
			if receipts != 0 {
				t.Fatalf("node receipts = %d, want 0", receipts)
			}
		})
	}
}

func seedSystemNodeCompletionRun(t *testing.T, db *sql.DB, runID, eventID, entityID string, nodeIDs ...string) {
	t.Helper()
	nodeID := "terminal-node"
	if len(nodeIDs) > 0 && nodeIDs[0] != "" {
		nodeID = nodeIDs[0]
	}
	seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'node', $3, 'pending', 'matched_node_subscription', now()
		)
	`, runID, eventID, nodeID); err != nil {
		t.Fatalf("seed node delivery: %v", err)
	}
}

func seedSystemNodeCompletionEventWithoutDelivery(t *testing.T, db *sql.DB, runID, eventID, entityID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', now())
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'example.started', $3::uuid, 'example', 'entity', '{}'::jsonb,
			0, 'test', 'external', now()
		)
	`, eventID, runID, entityID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET trigger_event_id = $2::uuid,
		    trigger_event_type = 'example.started'
		WHERE run_id = $1::uuid
	`, runID, eventID); err != nil {
		t.Fatalf("seed run trigger: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'example', 'default', 'example', 'Example', 'working',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, runID, entityID); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
}
