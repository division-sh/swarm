package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeruncontrol "swarm/internal/runtime/runcontrol"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestEventBusRunControlPauseQueuesOnlyTargetRunAndContinueReleases(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, eb, runtimeruncontrol.Options{})
	eb.SetRunDispatchGate(controller)

	agentID := "agent-run-control"
	eventType := events.EventType("custom.run_control")
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := eb.Subscribe(agentID, eventType)
	defer eb.Unsubscribe(agentID)

	pausedRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	for _, runID := range []string{pausedRunID, otherRunID} {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
			t.Fatalf("seed run %s: %v", runID, err)
		}
	}
	if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: pausedRunID, Reason: "test", ControlledBy: "test"}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	pausedEventID := uuid.NewString()
	if err := eb.Publish(ctx, events.Event{
		ID:          pausedEventID,
		RunID:       pausedRunID,
		Type:        eventType,
		SourceAgent: "api.v1",
		Payload:     []byte(`{"entity_id":"21000000-0000-0000-0000-000000000002"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("21000000-0000-0000-0000-000000000002")); err != nil {
		t.Fatalf("Publish paused run event: %v", err)
	}
	select {
	case got := <-ch:
		t.Fatalf("paused run delivered event %s before continue", got.ID)
	case <-time.After(150 * time.Millisecond):
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, pausedEventID); got != 0 {
		t.Fatalf("paused run pipeline receipts = %d, want 0", got)
	}

	otherEventID := uuid.NewString()
	if err := eb.Publish(ctx, events.Event{
		ID:          otherEventID,
		RunID:       otherRunID,
		Type:        eventType,
		SourceAgent: "api.v1",
		Payload:     []byte(`{"entity_id":"21000000-0000-0000-0000-000000000003"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("21000000-0000-0000-0000-000000000003")); err != nil {
		t.Fatalf("Publish other run event: %v", err)
	}
	select {
	case got := <-ch:
		if got.ID != otherEventID {
			t.Fatalf("delivered event = %s, want other run %s", got.ID, otherEventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for other run dispatch")
	}

	result, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: pausedRunID, Reason: "test", ControlledBy: "test"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if result.ReleasedDeliveries != 1 {
		t.Fatalf("released deliveries = %d, want 1", result.ReleasedDeliveries)
	}
	select {
	case got := <-ch:
		if got.ID != pausedEventID {
			t.Fatalf("released event = %s, want paused run %s", got.ID, pausedEventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for paused run release")
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, pausedEventID); got != 1 {
		t.Fatalf("paused run pipeline receipts after continue = %d, want 1", got)
	}
}
