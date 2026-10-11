package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SeedReplayDeliverySession(ctx context.Context, selected any, sessionID string, identity agentidentity.Identity) error {
	return private.SeedReplayDeliverySessionForTest(ctx, selected, sessionID, identity)
}
