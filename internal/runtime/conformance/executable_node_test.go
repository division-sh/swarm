package conformance

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func conformanceNode(t testing.TB, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	if flowID == "" {
		return identitytest.RootNode(t, nodeID)
	}
	return identitytest.FlowNode(t, flowID, nodeID)
}

func conformancePackageNode(t testing.TB, packageKey, flowID, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return identitytest.ExecutableNode(t, packageKey, flowID, nodeID)
}
