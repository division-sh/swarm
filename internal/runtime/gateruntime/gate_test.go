package gateruntime

import (
	"testing"
	"time"
)

func TestActivationLifecycleIsFencedAndDurable(t *testing.T) {
	now := time.Date(2026, time.July, 12, 10, 0, 0, 0, time.UTC)
	activation, err := New("run-1", "root", "entity-1", "", "awaiting_review", "launch_review", "bundle-hash", "stage.entered", now)
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := Load(buckets, "", "launch_review")
	if err != nil || !found {
		t.Fatalf("Load = %#v, %v, %v", loaded, found, err)
	}
	if err := loaded.CommitDecision("event-1", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if loaded.Supersede("stage_exited", now.Add(2*time.Minute)) != true || loaded.Status != StatusSuperseded {
		t.Fatalf("committed activation supersession = %#v, want terminal fence", loaded)
	}
	if err := loaded.Route("event-1", now.Add(3*time.Minute)); err == nil {
		t.Fatal("superseded activation routed")
	}
}

func TestActivationRouteRequiresCommittedEventIdentity(t *testing.T) {
	now := time.Date(2026, time.July, 12, 10, 0, 0, 0, time.UTC)
	activation, err := New("run-1", "root", "entity-1", "", "awaiting_review", "launch_review", "bundle-hash", "stage.entered", now)
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
