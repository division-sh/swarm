package cataloge2e

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func catalogRootNode(t testing.TB, nodeID string) runtimeidentity.ExecutableNode {
	t.Helper()
	return identitytest.RootNode(t, nodeID)
}
