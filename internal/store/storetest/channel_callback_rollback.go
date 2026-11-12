package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ChannelCallbackRollbackFault = private.ChannelCallbackRollbackFault

func OpenChannelCallbackRollbackFault(ctx context.Context, selected any, event string) (ChannelCallbackRollbackFault, error) {
	return private.OpenChannelCallbackRollbackFaultForTest(ctx, selected, event)
}
