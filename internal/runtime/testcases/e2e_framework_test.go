package testcases

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestGenericBundle_E2EFrameworkShape(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/runtime/testdata/generic-swarm-bundle"))
	bundle := loadGenericSwarmBundle(t)
	if bundle.WorkflowName() != "." || bundle.SourceArtifact == nil || bundle.WorkflowVersion() != bundle.SourceArtifact.BundleHash() {
		t.Fatalf("unexpected workflow identity: %s %s", bundle.WorkflowName(), bundle.WorkflowVersion())
	}
	if len(bundle.FlowSchemas) != 3 {
		t.Fatalf("expected 3 generic flows, got %d", len(bundle.FlowSchemas))
	}
	if !hasAll(bundle.FlowInputEvents("intake"), "item.created") {
		t.Fatalf("expected intake input events, got %v", bundle.FlowInputEvents("intake"))
	}
	source := semanticview.Wrap(bundle)
	if _, ok := source.ExecutableNodeEventHandler(genericExecutableNode(t, bundle, "processing-node"), "item.review_requested"); !ok {
		t.Fatal("expected processing handler to support rule-outcome assertions")
	}
	if _, ok := source.ExecutableNodeEventHandler(genericExecutableNode(t, bundle, "delivery-node"), "item.completed"); !ok {
		t.Fatal("expected delivery handler to support publish-and-wait style assertions")
	}
}
