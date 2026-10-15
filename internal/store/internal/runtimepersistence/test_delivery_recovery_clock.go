package runtimepersistence

import (
	"context"
	"fmt"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type DeliveryConformanceClockInput = delivery.DeliveryConformanceClockInput

func ProveDeliveryConformanceClockForTest(ctx context.Context, selected any, input DeliveryConformanceClockInput) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	owner, ok := selected.(*PostgresStore)
	if !ok {
		return fmt.Errorf("long transaction clock proof requires original PostgreSQL owner")
	}
	return owner.deliveryPostgresOwner.ProveDeliveryConformanceClockForTest(ctx, input)
}
