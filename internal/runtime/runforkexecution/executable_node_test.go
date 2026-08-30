package runforkexecution

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func mustRunForkRootNode(nodeID string) runtimeidentity.ExecutableNode {
	return mustRunForkNode(".", nodeID)
}

func mustRunForkNode(flowPath, nodeID string) runtimeidentity.ExecutableNode {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(flowPath, nodeID)
	if err != nil {
		panic(err)
	}
	return node
}

func runForkSourceNode(t testing.TB, source semanticview.Source, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	var match runtimeidentity.ExecutableNode
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil || node.NodeID() != nodeID {
			continue
		}
		if !match.Empty() {
			t.Fatalf("selected-contract node %q is flow-path ambiguous", nodeID)
		}
		match = node
	}
	if match.Empty() {
		t.Fatalf("selected-contract node %q is missing", nodeID)
	}
	return match
}
