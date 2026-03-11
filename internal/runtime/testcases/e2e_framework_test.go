package testcases

import "testing"

func TestGenericBundle_E2EFrameworkShape(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	if bundle.WorkflowName() != "test-platform" || bundle.WorkflowVersion() != "1.0.0" {
		t.Fatalf("unexpected workflow identity: %s %s", bundle.WorkflowName(), bundle.WorkflowVersion())
	}
	if len(bundle.FlowSchemas) != 3 {
		t.Fatalf("expected 3 generic flows, got %d", len(bundle.FlowSchemas))
	}
	if !hasAll(bundle.FlowInputEvents("intake"), "item.created") {
		t.Fatalf("expected intake input events, got %v", bundle.FlowInputEvents("intake"))
	}
	if !hasAll(bundle.FlowOutputEvents("processing"), "item.completed", "item.rejected") {
		t.Fatalf("expected processing output events, got %v", bundle.FlowOutputEvents("processing"))
	}
	if _, ok := bundle.NodeEventHandler("delivery-node", "item.completed"); !ok {
		t.Fatal("expected delivery handler to support publish-and-wait style assertions")
	}
}
