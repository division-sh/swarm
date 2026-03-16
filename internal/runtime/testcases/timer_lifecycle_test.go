package testcases

import "testing"

func TestGenericBundle_TimerLifecyclePatterns(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	timer, ok := bundle.WorkflowTimerByID("item_timeout")
	if !ok {
		t.Fatal("expected item_timeout timer")
	}
	if timer.Owner != "delivery-node" || timer.Event != "timer.item.timeout" {
		t.Fatalf("unexpected timer contract: %+v", timer)
	}
	if timer.StartOn != "state:approved" || timer.CancelOn != "state:completed" {
		t.Fatalf("unexpected timer lifecycle hooks: %+v", timer)
	}

	handler := mustHandler(t, bundle, "delivery-node", "timer.item.timeout")
	if handler.AdvancesTo != "completed" {
		t.Fatalf("expected timeout to force completion, got %q", handler.AdvancesTo)
	}
	if !hasAll(handler.Emits.Values(), "delivery/item.completed") {
		t.Fatalf("expected timeout completion emission, got %v", handler.Emits.Values())
	}
	if fields := handler.DataAccumulation.TargetFields(); !hasAll(fields, "timed_out", "status") {
		t.Fatalf("expected timed_out/status writes, got %v", fields)
	}
}
