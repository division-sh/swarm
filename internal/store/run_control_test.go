package store

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_RunControlTransitionsAndStopAbandonsPendingWork(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, run_id, event_name, payload, created_at)
		VALUES ($1::uuid, $2::uuid, 'custom.stop', '{}'::jsonb, now())
	`, eventID, runID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, status, created_at)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-pending', 'pending', now())
	`, runID, eventID); err != nil {
		t.Fatalf("seed pending delivery: %v", err)
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
	if state.Status != "cancelled" || state.ControlStatus != "stopped" || state.AbandonedDeliveries != 1 {
		t.Fatalf("stop state = %+v, want cancelled/stopped/1", state)
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'paused')`, runID); err != nil {
		t.Fatalf("seed paused run: %v", err)
	}
	if _, err := pg.ContinueRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: runID}); !errors.Is(err, runtimeruncontrol.ErrNotPaused) {
		t.Fatalf("ContinueRunControl without operator pause owner err = %v, want ErrNotPaused", err)
	}
}
