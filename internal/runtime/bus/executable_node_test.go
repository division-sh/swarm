package bus

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func testRootNode(t testing.TB, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return identitytest.RootNode(t, nodeID)
}

func testFlowNode(t testing.TB, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return identitytest.FlowNode(t, flowID, nodeID)
}

func testPackageNode(t testing.TB, packageKey, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return identitytest.ExecutableNode(t, packageKey, flowID, nodeID)
}
