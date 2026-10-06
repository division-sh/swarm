package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ReceiverDeliveryStorageEvidence = private.ReceiverDeliveryStorageEvidence

func ReadReceiverDeliveryStorage(ctx context.Context, selected any, runID string) ([]ReceiverDeliveryStorageEvidence, error) {
	return private.ReadReceiverDeliveryStorageForTest(ctx, selected, runID)
}
