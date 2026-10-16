package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ChannelInputBindingFault = private.ChannelInputBindingFault

func CaptureChannelInputBindingFaultForTest(ctx context.Context, selected any, run string) (*ChannelInputBindingFault, error) {
	return private.CaptureChannelInputBindingFaultForTest(ctx, selected, run)
}
