package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

// InstallNodeRecoveryPoisonMutation scopes a real entity UPDATE failure to the
// selected test store/run/entity. The returned operation removes only that fault.
func InstallNodeRecoveryPoisonMutation(ctx context.Context, selected any, runID, entityID string) (func(context.Context) error, error) {
	return private.InstallNodeRecoveryPoisonMutationForTest(ctx, selected, runID, entityID)
}
