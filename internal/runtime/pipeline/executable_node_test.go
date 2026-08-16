package pipeline

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func pipelineNode(t testing.TB, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	if flowID == "" {
		return identitytest.RootNode(t, nodeID)
	}
	return identitytest.FlowNode(t, flowID, nodeID)
}

func mustPipelineNode(flowID, nodeID string) runtimeidentity.ExecutableNode {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, flowID, nodeID)
	if err != nil {
		panic(err)
	}
	return node
}

func pipelinePackageNode(t testing.TB, packageKey, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return identitytest.ExecutableNode(t, packageKey, flowID, nodeID)
}

func pipelineSourceNode(t testing.TB, source semanticview.Source, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	var match runtimeidentity.ExecutableNode
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil || node.FlowID() != flowID || node.NodeID() != nodeID {
			continue
		}
		if !match.Empty() {
			t.Fatalf("executable node %s/%s is package-ambiguous", flowID, nodeID)
		}
		match = node
	}
	if match.Empty() {
		t.Fatalf("executable node %s/%s is missing", flowID, nodeID)
	}
	return match
}

func pipelineOnlySourceNode(t testing.TB, source semanticview.Source, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	var match runtimeidentity.ExecutableNode
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil || node.NodeID() != nodeID {
			continue
		}
		if !match.Empty() {
			t.Fatalf("executable node %s is scope-ambiguous", nodeID)
		}
		match = node
	}
	if match.Empty() {
		t.Fatalf("executable node %s is missing", nodeID)
	}
	return match
}
