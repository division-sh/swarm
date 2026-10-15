package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

// RequireMalformedOperatorAgentRoute preserves the defensive reader's hostile
// present-route/empty-scope cut on a disposable native fixture only.
func RequireMalformedOperatorAgentRoute(t testing.TB, ctx context.Context, selected any, identity agentidentity.Identity) {
	t.Helper()
	private.RequireMalformedOperatorAgentRouteForTest(t, ctx, selected, identity)
}

func RequireUnknownOperatorAgentStatus(t testing.TB, ctx context.Context, selected any, identity agentidentity.Identity) {
	t.Helper()
	private.RequireUnknownOperatorAgentStatusForTest(t, ctx, selected, identity)
}
