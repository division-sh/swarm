package testcases

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestGenericBundle_SystemNodeReliabilityPatterns(t *testing.T) {
	bundle := loadGenericSwarmBundle(t)
	for _, nodeID := range []string{"intake-router", "processing-node", "delivery-node"} {
		node := bundle.Nodes[nodeID]
		if node.StateTable == "" {
			t.Fatalf("expected state table for %s", nodeID)
		}
	}

	source := semanticview.Wrap(bundle)
	if owners := source.RuntimeEventOwners("item.review_requested"); !containsExecutableNode(owners, genericExecutableNode(t, bundle, "processing-node")) {
		t.Fatalf("expected processing-node to own item.review_requested, got %v", owners)
	}
	if owners := source.RuntimeEventOwners("item.completed"); !containsExecutableNode(owners, genericExecutableNode(t, bundle, "delivery-node")) {
		t.Fatalf("expected delivery-node to own item.completed, got %v", owners)
	}

	handler := mustHandler(t, bundle, "processing-node", "item.review_requested")
	if len(handler.Rules) != 2 || handler.Rules[1].Condition != "else" {
		t.Fatalf("expected approve/reject rules with fallback, got %+v", handler.Rules)
	}
}

func containsExecutableNode(nodes []runtimeidentity.ExecutableNode, want runtimeidentity.ExecutableNode) bool {
	for _, node := range nodes {
		if node.Equal(want) {
			return true
		}
	}
	return false
}
