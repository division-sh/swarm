package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ChannelCurrentRenderFault = private.ChannelCurrentRenderFault

func CaptureChannelCurrentRenderFaultForTest(ctx context.Context, selected any, card string) (*ChannelCurrentRenderFault, error) {
	return private.CaptureChannelCurrentRenderFaultForTest(ctx, selected, card)
}
