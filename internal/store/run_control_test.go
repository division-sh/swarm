package store

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_RunControlTransitionsAndStopAbandonsPendingWork(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := seedPostgresSemanticEventRecordFixture(
		t, ctx, db, eventID, runID, events.EventType("custom.stop"),
		events.EventProducerPlatform, "test", "", "", time.Now().UTC(),
	)
	for _, route := range []events.DeliveryRoute{
		{SubscriberType: "agent", SubscriberID: "agent-pending"},
		{SubscriberType: "node", SubscriberID: "node-pending"},
	} {
		if err := commitDeliveryObligationFixture(ctx, pg, event, route); err != nil {
			t.Fatalf("seed pending %s delivery: %v", route.SubscriberType, err)
		}
	}

	if _, err := pg.PauseRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test", Now: time.Now().UTC()}); err != nil {
		t.Fatalf("PauseRunControl: %v", err)
	}
	if _, err := pg.ContinueRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test", Now: time.Now().UTC()}); err != nil {
		t.Fatalf("ContinueRunControl: %v", err)
	}
	state, err := pg.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test", Now: time.Now().UTC()})
	if err != nil {
		t.Fatalf("StopRunControl: %v", err)
	}
	if state.Status != "cancelled" || state.ControlStatus != "stopped" || state.AbandonedDeliveries != 2 {
		t.Fatalf("stop state = %+v, want cancelled/stopped/2", state)
	}

	var deliveryStatus, reasonCode string
	if err := db.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_id = 'agent-pending'
	`, eventID).Scan(&deliveryStatus, &reasonCode); err != nil {
		t.Fatalf("load stopped delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || reasonCode != "run_stopped" {
		t.Fatalf("stopped delivery = %s/%s, want dead_letter/run_stopped", deliveryStatus, reasonCode)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node-pending'
	`, eventID).Scan(&deliveryStatus, &reasonCode); err != nil {
		t.Fatalf("load stopped node delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || reasonCode != "run_stopped" {
		t.Fatalf("stopped node delivery = %s/%s, want dead_letter/run_stopped", deliveryStatus, reasonCode)
	}
	var nodeReceiptOutcome, nodeReceiptReason string
	if err := db.QueryRowContext(ctx, `
		SELECT o.outcome, COALESCE(o.reason_code, '')
		FROM event_delivery_outcomes o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = 'node-pending'
	`, eventID).Scan(&nodeReceiptOutcome, &nodeReceiptReason); err != nil {
		t.Fatalf("load stopped node outcome: %v", err)
	}
	if nodeReceiptOutcome != "terminalized" || nodeReceiptReason != "run_stopped" {
		t.Fatalf("stopped node outcome = %s/%s, want terminalized/run_stopped", nodeReceiptOutcome, nodeReceiptReason)
	}
	var receiptCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
		  AND outcome = 'dead_letter'
	`, eventID).Scan(&receiptCount); err != nil {
		t.Fatalf("count stopped pipeline receipts: %v", err)
	}
	if receiptCount != 1 {
		t.Fatalf("stopped pipeline receipts = %d, want 1", receiptCount)
	}

	if _, err := pg.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID}); !errors.Is(err, runtimeruncontrol.ErrAlreadyTerminal) {
		t.Fatalf("repeat StopRunControl err = %v, want ErrAlreadyTerminal", err)
	}
}

func TestPostgresStore_RunControlContinueRequiresOperatorPauseOwner(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'paused')`, runID); err != nil {
		t.Fatalf("seed paused run: %v", err)
	}
	if _, err := pg.ContinueRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID}); !errors.Is(err, runtimeruncontrol.ErrNotPaused) {
		t.Fatalf("ContinueRunControl without operator pause owner err = %v, want ErrNotPaused", err)
	}
}
