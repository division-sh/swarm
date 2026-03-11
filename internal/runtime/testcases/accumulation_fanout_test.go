package testcases

import "testing"

func TestGenericBundle_AccumulationFanoutPatterns(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	created := mustHandler(t, bundle, "intake-router", "item.created")
	if created.Guard == nil || created.Guard.ID != "accept_item" {
		t.Fatalf("expected inline guard id on item.created, got %+v", created.Guard)
	}
	if created.FanOut == nil {
		t.Fatal("expected fan_out on item.created")
	}
	if created.FanOut.ItemsFrom != "payload.items" || created.FanOut.Target != "worker-a" || created.FanOut.EmitPerItem != "item.processed" {
		t.Fatalf("unexpected fan_out spec: %+v", created.FanOut)
	}
	if len(created.Branch) != 1 || created.Branch[0].Then == nil || created.Branch[0].Else == nil {
		t.Fatalf("expected urgent/non-urgent branch on item.created, got %+v", created.Branch)
	}

	processed := mustHandler(t, bundle, "intake-router", "item.processed")
	if processed.Accumulate == nil || processed.Accumulate.OnComplete == nil {
		t.Fatal("expected accumulate.on_complete on item.processed")
	}
	outcome := simulateAccumulation(processed, 3, 3)
	if outcome.nextState != "ready" {
		t.Fatalf("expected ready state after accumulation completion, got %q", outcome.nextState)
	}
	if !hasAll(outcome.emitted, "item.review_requested") {
		t.Fatalf("expected intake completion to emit item.review_requested, got %v", outcome.emitted)
	}
	if outcome.setsGate != "intake_ready" {
		t.Fatalf("expected intake_ready gate, got %q", outcome.setsGate)
	}
	if !hasAll(outcome.clearGates, "needs_revision") {
		t.Fatalf("expected needs_revision gate cleared, got %v", outcome.clearGates)
	}
}
