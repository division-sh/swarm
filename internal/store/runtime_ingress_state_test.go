package store

import (
	"context"
	"testing"
	"time"

	runtimeingress "swarm/internal/runtime/ingress"
	"swarm/internal/testutil"
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

	if _, _, err := pg.TransitionRuntimeIngressState(ctx, runtimeingress.Status("stopped"), "", "", now); err == nil {
		t.Fatal("unsupported runtime ingress status error = nil")
	}
}
