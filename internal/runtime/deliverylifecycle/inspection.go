package deliverylifecycle

import "context"

// ActiveSnapshotReader observes durable membership, not live execution permission.
type ActiveSnapshotReader interface {
	ReadActiveDeliverySnapshots(context.Context) ([]Snapshot, error)
}
