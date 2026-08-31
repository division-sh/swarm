package identitytest

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func ExecutableNode(t testing.TB, flowPath, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(flowPath, nodeID)
	if err != nil {
		t.Fatalf("admit executable node: %v", err)
	}
	return node
}

func RootNode(t testing.TB, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return ExecutableNode(t, ".", nodeID)
}

func FlowNode(t testing.TB, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return ExecutableNode(t, flowID, nodeID)
}
