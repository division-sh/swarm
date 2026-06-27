package store

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestRuntimeIngressStatePersistsTypedTransitions(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)

	state, err := pg.EnsureRuntimeIngressState(ctx, now)
	if err != nil {
		t.Fatalf("EnsureRuntimeIngressState: %v", err)
	}
	if state.Status != runtimeingress.StatusRunning {
		t.Fatalf("initial status = %q, want running", state.Status)
	}

	state, changed, err := pg.TransitionRuntimeIngressState(ctx, runtimeingress.StatusPaused, "operator_request", "api.v1", now.Add(time.Second))
	if err != nil {
		t.Fatalf("TransitionRuntimeIngressState(paused): %v", err)
	}
	if !changed || state.Status != runtimeingress.StatusPaused || state.Reason != "operator_request" || state.ControlledBy != "api.v1" {
		t.Fatalf("paused transition = %#v changed=%v", state, changed)
	}
	pausedAt := state.UpdatedAt
	pausedEventID := "11111111-1111-1111-1111-111111111111"
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(pausedEventID,
		events.EventType("platform.paused"),
		"runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, pausedAt)); err != nil {
		t.Fatalf("AppendEvent(paused): %v", err)
	}
	if ok, err := pg.SetRuntimeIngressTransitionEvent(ctx, runtimeingress.StatusPaused, pausedEventID, pausedAt); err != nil {
		t.Fatalf("SetRuntimeIngressTransitionEvent(paused): %v", err)
	} else if !ok {
		t.Fatal("SetRuntimeIngressTransitionEvent(paused) ok = false, want true")
	}
	if state, err := pg.LoadRuntimeIngressState(ctx); err != nil {
		t.Fatalf("LoadRuntimeIngressState(paused event): %v", err)
	} else if state.TransitionEventID != pausedEventID {
		t.Fatalf("paused transition event id = %q, want %q", state.TransitionEventID, pausedEventID)
	}

	state, changed, err = pg.TransitionRuntimeIngressState(ctx, runtimeingress.StatusPaused, "operator_request", "api.v1", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("TransitionRuntimeIngressState(paused no-op): %v", err)
	}
	if changed || state.Status != runtimeingress.StatusPaused {
		t.Fatalf("paused no-op = %#v changed=%v", state, changed)
	}

	state, changed, err = pg.TransitionRuntimeIngressState(ctx, runtimeingress.StatusRunning, "operator_request", "api.v1", now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("TransitionRuntimeIngressState(running): %v", err)
	}
	if !changed || state.Status != runtimeingress.StatusRunning {
		t.Fatalf("running transition = %#v changed=%v", state, changed)
	}
	runningAt := state.UpdatedAt
	stalePausedEventID := "22222222-2222-2222-2222-222222222222"
	if ok, err := pg.SetRuntimeIngressTransitionEvent(ctx, runtimeingress.StatusPaused, stalePausedEventID, pausedAt); err != nil {
		t.Fatalf("SetRuntimeIngressTransitionEvent(stale paused): %v", err)
	} else if ok {
		t.Fatal("SetRuntimeIngressTransitionEvent(stale paused) ok = true, want false")
	}
	if state, err := pg.LoadRuntimeIngressState(ctx); err != nil {
		t.Fatalf("LoadRuntimeIngressState(after stale event): %v", err)
	} else if state.Status != runtimeingress.StatusRunning || state.TransitionEventID == stalePausedEventID {
		t.Fatalf("state after stale event update = %#v", state)
	}
	runningEventID := "33333333-3333-3333-3333-333333333333"
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(runningEventID,
		events.EventType("platform.resumed"),
		"runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, runningAt)); err != nil {
		t.Fatalf("AppendEvent(running): %v", err)
	}
	if ok, err := pg.SetRuntimeIngressTransitionEvent(ctx, runtimeingress.StatusRunning, runningEventID, runningAt); err != nil {
		t.Fatalf("SetRuntimeIngressTransitionEvent(running): %v", err)
	} else if !ok {
		t.Fatal("SetRuntimeIngressTransitionEvent(running) ok = false, want true")
	}

	if _, _, err := pg.TransitionRuntimeIngressState(ctx, runtimeingress.Status("stopped"), "", "", now); err == nil {
		t.Fatal("unsupported runtime ingress status error = nil")
	}
}
