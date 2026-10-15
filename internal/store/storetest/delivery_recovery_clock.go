package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DeliveryConformanceClockInput = private.DeliveryConformanceClockInput

func ProveDeliveryConformanceClock(ctx context.Context, selected any, input DeliveryConformanceClockInput) error {
	return private.ProveDeliveryConformanceClockForTest(ctx, selected, input)
}
