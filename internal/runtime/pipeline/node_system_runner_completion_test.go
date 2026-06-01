package pipeline

import (
	"context"
	"database/sql"
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
	runner := newSystemNodeRunner("terminal-node", bus, db, func() []events.EventType {
		return []events.EventType{"example.started"}
	}, func(ctx context.Context, evt events.Event) error {
		handlerCalled = true
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

	runner.ProcessEventForTest(ctx, (events.Event{
		ID:        eventID,
		Type:      "example.started",
		RunID:     runID,
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(entityID))

	if !handlerCalled {
		t.Fatal("handler was not called")
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

func seedSystemNodeCompletionRun(t *testing.T, db *sql.DB, runID, eventID, entityID string, nodeIDs ...string) {
	t.Helper()
	nodeID := "terminal-node"
	if len(nodeIDs) > 0 && nodeIDs[0] != "" {
		nodeID = nodeIDs[0]
	}
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
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'node', $3, 'pending', 'matched_node_subscription', now()
		)
	`, runID, eventID, nodeID); err != nil {
		t.Fatalf("seed node delivery: %v", err)
	}
}
