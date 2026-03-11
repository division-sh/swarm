package testcases

import "testing"

func TestGenericBundle_SystemNodeReliabilityPatterns(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	for _, nodeID := range []string{"intake-router", "processing-node", "delivery-node"} {
		node := bundle.Nodes[nodeID]
		if node.IdempotencyTable == "" {
			t.Fatalf("expected idempotency table for %s", nodeID)
		}
		if node.StateTable == "" {
			t.Fatalf("expected state table for %s", nodeID)
		}
	}

	if owners := bundle.RuntimeEventOwners("item.review_requested"); !hasAll(owners, "processing-node") {
		t.Fatalf("expected processing-node to own item.review_requested, got %v", owners)
	}
	if owners := bundle.RuntimeEventOwners("item.completed"); !hasAll(owners, "delivery-node") {
		t.Fatalf("expected delivery-node to own item.completed, got %v", owners)
	}

	handler := mustHandler(t, bundle, "processing-node", "item.review_requested")
	if len(handler.Branch) == 0 || handler.Branch[0].Else == nil {
		t.Fatalf("expected fallback branch for review handler, got %+v", handler.Branch)
	}
}
