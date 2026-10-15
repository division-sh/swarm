package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func InstallTerminalPanicMutationFailure(ctx context.Context, selected any, agentID string, failFinalizer bool) error {
	return private.InstallTerminalPanicMutationFailureForTest(ctx, selected, agentID, failFinalizer)
}
