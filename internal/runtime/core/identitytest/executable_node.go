package identitytest

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func ExecutableNode(t testing.TB, packageKey, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(packageKey, flowID, nodeID)
	if err != nil {
		t.Fatalf("admit executable node: %v", err)
	}
	return node
}

func RootNode(t testing.TB, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return ExecutableNode(t, runtimeidentity.RootPackageKey, "", nodeID)
}

func FlowNode(t testing.TB, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return ExecutableNode(t, runtimeidentity.RootPackageKey, flowID, nodeID)
}
