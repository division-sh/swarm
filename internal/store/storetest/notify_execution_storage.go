package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadNotifyAgentDeliveryStatus(ctx context.Context, selected any, runID, agentID, instance string) (string, error) {
	return private.ReadNotifyAgentDeliveryStatusForTest(ctx, selected, runID, agentID, instance)
}
