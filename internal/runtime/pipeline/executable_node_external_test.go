package pipeline_test

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func externalPipelineNode(t testing.TB, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	if flowID == "" {
		return identitytest.RootNode(t, nodeID)
	}
	return identitytest.FlowNode(t, flowID, nodeID)
}

func externalPipelineSourceNode(t testing.TB, source semanticview.Source, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	var match runtimeidentity.ExecutableNode
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		wantFlowPath := flowID
		if wantFlowPath == "" {
			wantFlowPath = "."
		}
		if err != nil || node.FlowPath() != wantFlowPath || node.NodeID() != nodeID {
			continue
		}
		if !match.Empty() {
			t.Fatalf("executable node %s/%s is flow-path ambiguous", flowID, nodeID)
		}
		match = node
	}
	if match.Empty() {
		t.Fatalf("executable node %s/%s is missing", flowID, nodeID)
	}
	return match
}
