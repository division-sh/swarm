package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DeploymentEventDeliveryCutStorage = private.DeploymentEventDeliveryCutStorage
type DeploymentRunEventDeliveryCutStorage = private.DeploymentRunEventDeliveryCutStorage
type SelectedDeploymentClaimStorage = private.SelectedDeploymentClaimStorage
type SelectedDeliveryExecutionStorage = private.SelectedDeliveryExecutionStorage

func ReadDeploymentPreReceiptDeliveryCutStorage(ctx context.Context, selected any, event string) (DeploymentEventDeliveryCutStorage, error) {
	return private.ReadDeploymentPreReceiptDeliveryCutStorageForTest(ctx, selected, event)
}

func ReadDeploymentPendingReceiverCutStorage(ctx context.Context, selected any, run, event string) (DeploymentEventDeliveryCutStorage, error) {
	return private.ReadDeploymentPendingReceiverCutStorageForTest(ctx, selected, run, event)
}

func ReadDeploymentRunEventDeliveryCutStorage(ctx context.Context, selected any, run, event string) (DeploymentRunEventDeliveryCutStorage, error) {
	return private.ReadDeploymentRunEventDeliveryCutStorageForTest(ctx, selected, run, event)
}

func CountDeploymentRunDeliveryAttemptsStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountDeploymentRunDeliveryAttemptsStorageForTest(ctx, selected, run)
}

func ReadSelectedDeploymentClaimStorage(ctx context.Context, selected any, id, run string) (SelectedDeploymentClaimStorage, error) {
	return private.ReadSelectedDeploymentClaimStorageForTest(ctx, selected, id, run)
}

func ReadSelectedDeliveryExecutionStorage(ctx context.Context, selected any, id string) (SelectedDeliveryExecutionStorage, error) {
	return private.ReadSelectedDeliveryExecutionStorageForTest(ctx, selected, id)
}

func CountDeliveredClaimVersionStorage(ctx context.Context, selected any, id string, version int64) (int, error) {
	return private.CountDeliveredClaimVersionStorageForTest(ctx, selected, id, version)
}

func CountDeliveredSuccessorClaimVersionsStorage(ctx context.Context, selected any, id string, version int64) (int, error) {
	return private.CountDeliveredSuccessorClaimVersionsStorageForTest(ctx, selected, id, version)
}
