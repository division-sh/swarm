package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type DeploymentEventDeliveryCutStorage = delivery.DeploymentEventDeliveryCutStorage
type DeploymentRunEventDeliveryCutStorage = delivery.DeploymentRunEventDeliveryCutStorage
type SelectedDeploymentClaimStorage = delivery.SelectedDeploymentClaimStorage
type SelectedDeliveryExecutionStorage = delivery.SelectedDeliveryExecutionStorage

func ReadDeploymentPreReceiptDeliveryCutStorageForTest(ctx context.Context, selected any, event string) (DeploymentEventDeliveryCutStorage, error) {
	if err := validateStandaloneStorageEvent(selected, event); err != nil {
		return DeploymentEventDeliveryCutStorage{}, err
	}
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (DeploymentEventDeliveryCutStorage, error) {
		return delivery.ReadDeploymentPreReceiptDeliveryCutStorageTx(ctx, tx, event)
	})
}

func ReadDeploymentPendingReceiverCutStorageForTest(ctx context.Context, selected any, run, event string) (DeploymentEventDeliveryCutStorage, error) {
	if err := validateStandaloneStorageEvent(selected, run); err != nil {
		return DeploymentEventDeliveryCutStorage{}, err
	}
	if err := validateStandaloneStorageEvent(selected, event); err != nil {
		return DeploymentEventDeliveryCutStorage{}, err
	}
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (DeploymentEventDeliveryCutStorage, error) {
		return delivery.ReadDeploymentPendingReceiverCutStorageTx(ctx, tx, run, event)
	})
}

func ReadDeploymentRunEventDeliveryCutStorageForTest(ctx context.Context, selected any, run, event string) (DeploymentRunEventDeliveryCutStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (DeploymentRunEventDeliveryCutStorage, error) {
		return delivery.ReadDeploymentRunEventDeliveryCutStorageTx(ctx, tx, run, event)
	})
}

func CountDeploymentRunDeliveryAttemptsStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, run, delivery.CountDeploymentRunDeliveryAttemptsStorageTx)
}

func ReadSelectedDeploymentClaimStorageForTest(ctx context.Context, selected any, id, run string) (SelectedDeploymentClaimStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{id, run}, func(ctx context.Context, tx *sql.Tx) (SelectedDeploymentClaimStorage, error) {
		return delivery.ReadSelectedDeploymentClaimStorageTx(ctx, tx, id, run)
	})
}

func ReadSelectedDeliveryExecutionStorageForTest(ctx context.Context, selected any, id string) (SelectedDeliveryExecutionStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, id, delivery.ReadSelectedDeliveryExecutionStorageTx)
}

func CountDeliveredClaimVersionStorageForTest(ctx context.Context, selected any, id string, version int64) (int, error) {
	if version <= 0 {
		return 0, fmt.Errorf("delivered claim evidence requires a positive exact claim version")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{id}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountDeliveredClaimVersionStorageTx(ctx, tx, id, version)
	})
}

func CountDeliveredSuccessorClaimVersionsStorageForTest(ctx context.Context, selected any, id string, version int64) (int, error) {
	if version <= 0 {
		return 0, fmt.Errorf("successor claim evidence requires a positive predecessor claim version")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{id}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountDeliveredSuccessorClaimVersionsStorageTx(ctx, tx, id, version)
	})
}
