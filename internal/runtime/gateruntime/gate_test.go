package gateruntime

import (
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func testRoutesJSON(t *testing.T) string {
	t.Helper()
	raw, err := FreezeRoutes(map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}}, gateTestCompiledTransitions(t))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestActivationLifecycleIsFencedAndDurable(t *testing.T) {
	now := time.Date(2026, time.July, 12, 10, 0, 0, 0, time.UTC)
	activation, err := New("run-1", "review/instance-1", "entity-1", "review", "waiting", "review_decision", "bundle-hash", testRoutesJSON(t), "stage.entered", now)
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := Load(buckets, "review", "review_decision")
	if err != nil || !found {
		t.Fatalf("Load = %#v, %v, %v", loaded, found, err)
	}
	if err := loaded.CommitDecision("event-1", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if loaded.Supersede("stage_exited", now.Add(2*time.Minute)) || loaded.Status != StatusDecisionCommitted {
		t.Fatalf("committed activation supersession = %#v, want committed decision preserved", loaded)
	}
	if err := loaded.Route("event-1", now.Add(3*time.Minute)); err != nil {
		t.Fatalf("committed activation route: %v", err)
	}
}

func TestActivationRouteRequiresCommittedEventIdentity(t *testing.T) {
	now := time.Date(2026, time.July, 12, 10, 0, 0, 0, time.UTC)
	activation, err := New("run-1", "review/instance-1", "entity-1", "review", "waiting", "review_decision", "bundle-hash", testRoutesJSON(t), "stage.entered", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitDecision("event-1", now); err != nil {
		t.Fatal(err)
	}
	if err := activation.Route("event-2", now); err == nil {
		t.Fatal("activation accepted a different decision event")
	}
	if err := activation.Route("event-1", now); err != nil {
		t.Fatal(err)
	}
	if activation.Status != StatusRouted {
		t.Fatalf("status = %q, want routed", activation.Status)
	}
}
