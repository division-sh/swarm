package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type DeploymentNullableText = delivery.WorkloadNullableText
type DeploymentIntentProgressStorage = pipelinepersistence.DeploymentIntentProgressStorage
type DeploymentIntentMetadataStorage = pipelinepersistence.DeploymentIntentMetadataStorage
type DeploymentFeedStorage = pipelinepersistence.DeploymentFeedStorage
type DeploymentIntentDiagnosticStorage = pipelinepersistence.DeploymentIntentDiagnosticStorage
type DeploymentIntentCardinalityStorage = pipelinepersistence.DeploymentIntentCardinalityStorage
type PinnedDeploymentFeedStorage = pipelinepersistence.PinnedDeploymentFeedStorage
type DeploymentSettlementDiagnosticStorage = pipelinepersistence.DeploymentSettlementDiagnosticStorage
type PinnedPortfolioDeliveryDiagnosticStorage = pipelinepersistence.PinnedPortfolioDeliveryDiagnosticStorage
type DeploymentReceiptStorage = pipelinepersistence.DeploymentReceiptStorage
type DeploymentDeliveryStorage = delivery.DeploymentDeliveryStorage
type DeploymentAttemptStorage = delivery.DeploymentAttemptStorage
type DeploymentDeliveryStatusStorage = delivery.DeploymentDeliveryStatusStorage
type SelectedDeploymentOperationStorage = runforkpersistence.SelectedDeploymentOperationStorage
type SelectedDeploymentExecutionStorage = runforkpersistence.SelectedDeploymentExecutionStorage
type SelectedForkKeyStorage = runforkpersistence.SelectedForkKeyStorage
type SelectedDeploymentExecutionIdentityStorage = runforkpersistence.SelectedDeploymentExecutionIdentityStorage
type SelectedDeploymentPointStorage = runforkpersistence.SelectedDeploymentPointStorage
type DeploymentRunStatusStorage = runlifecycle.DeploymentRunStatusStorage
type DeploymentRunControlStorage = runlifecycle.DeploymentRunControlStorage

// The callback remains private between bounded row owners. Every public bridge
// selects one fixed projection, and each original query keeps its own read cut.
func readDeploymentStorageForTest[T any](ctx context.Context, selected any, runID string, read func(context.Context, *sql.Tx, string) (T, error)) (T, error) {
	var out T
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = read(ctx, tx, runID)
		return err
	})
	if err != nil {
		var empty T
		return empty, err
	}
	return out, nil
}

func ReadRootReadyResourcePinStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, storedurabledata.ReadRootReadyResourcePinStorageTx)
}

func ReadPortfolioAccountResourcePinStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, storedurabledata.ReadPortfolioAccountResourcePinStorageTx)
}

func CountAccountDeploymentReceiversStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.CountAccountDeploymentReceiversStorageTx)
}

func CountConsumerDeploymentReceiversStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.CountConsumerDeploymentReceiversStorageTx)
}

func CountDeploymentRouteObligationsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.CountDeploymentRouteObligationsStorageTx)
}

func CountPortfolioDeliveredDeploymentRowsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountPortfolioDeliveredDeploymentRowsStorageTx)
}

func CountPortfolioAllDeploymentRowsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountPortfolioAllDeploymentRowsStorageTx)
}

func ReadDeploymentIntentVersionStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentIntentVersionStorageTx)
}

func ReadDeploymentIntentCursorStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentIntentCursorStorageTx)
}

func ReadDeploymentOriginIntentVersionStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentOriginIntentVersionStorageTx)
}

func CountDeploymentOriginIntentsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.CountDeploymentOriginIntentsStorageTx)
}

func ReadDeploymentIntentCardinalityStorageForTest(ctx context.Context, selected any, runID string) (DeploymentIntentCardinalityStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentIntentCardinalityStorageTx)
}

func ReadDeploymentOriginIntentStateStorageForTest(ctx context.Context, selected any, runID string) (DeploymentIntentDiagnosticStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentOriginIntentStateStorageTx)
}

func ReadDeploymentCommittedOrdinalsStorageForTest(ctx context.Context, selected any, runID string) ([]CommittedFanOutOrdinalStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentCommittedOrdinalsStorageTx)
}

func ReadPinnedDeploymentFeedsStorageForTest(ctx context.Context, selected any, run string) ([]PinnedDeploymentFeedStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, run, pipelinepersistence.ReadPinnedDeploymentFeedsStorageTx)
}

func ReadDeploymentSettlementDiagnosticsStorageForTest(ctx context.Context, selected any, run string) ([]DeploymentSettlementDiagnosticStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, run, pipelinepersistence.ReadDeploymentSettlementDiagnosticsStorageTx)
}

func CountPinnedPortfolioSuccessfulPipelineReceiptsStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, run, pipelinepersistence.CountPinnedPortfolioSuccessfulPipelineReceiptsStorageTx)
}

func CountPinnedPortfolioSettledDeliveriesStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, run, delivery.CountPinnedPortfolioSettledDeliveriesStorageTx)
}

func ReadPinnedPortfolioDeliveryDiagnosticsStorageForTest(ctx context.Context, selected any, run string) ([]PinnedPortfolioDeliveryDiagnosticStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, run, pipelinepersistence.ReadPinnedPortfolioDeliveryDiagnosticsStorageTx)
}

func ReadDeploymentIntentProgressStorageForTest(ctx context.Context, selected any, runID string) (DeploymentIntentProgressStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentIntentProgressStorageTx)
}

func ReadDeploymentIntentMetadataStorageForTest(ctx context.Context, selected any, runID string) (DeploymentIntentMetadataStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentIntentMetadataStorageTx)
}

func ReadDeploymentFeedStorageForTest(ctx context.Context, selected any, runID string) ([]DeploymentFeedStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentFeedStorageTx)
}

func ReadDeploymentIntentDiagnosticsStorageForTest(ctx context.Context, selected any, runID string) ([]DeploymentIntentDiagnosticStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentIntentDiagnosticsStorageTx)
}

func CountDeploymentCommittedOutcomesStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.CountDeploymentCommittedOutcomesStorageTx)
}

func CountDeploymentPipelineReceiptsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.CountDeploymentPipelineReceiptsStorageTx)
}

func CountRootReadySuccessfulPipelineReceiptsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.CountRootReadySuccessfulPipelineReceiptsStorageTx)
}

func ReadDeploymentReceiptStorageForTest(ctx context.Context, selected any, runID string) ([]DeploymentReceiptStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.ReadDeploymentReceiptStorageTx)
}

func CountDeploymentPendingDeliveriesStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountDeploymentPendingDeliveriesStorageTx)
}

func CountDeploymentFailedDeliveriesStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountDeploymentFailedDeliveriesStorageTx)
}

func CountDeploymentStampedHandoffsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountDeploymentStampedHandoffsStorageTx)
}

func CountRunDeliveredRowsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountRunDeliveredRowsStorageTx)
}

func CountRootReadyDeliveredStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountRootReadyDeliveredStorageTx)
}

func CountRootReadyStampedHandoffsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountRootReadyStampedHandoffsStorageTx)
}

func ReadDeploymentDeliveryStorageForTest(ctx context.Context, selected any, runID string) ([]DeploymentDeliveryStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.ReadDeploymentDeliveryStorageTx)
}

func ReadDeploymentAttemptStorageForTest(ctx context.Context, selected any, runID string) ([]DeploymentAttemptStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.ReadDeploymentAttemptStorageTx)
}

func ReadDeploymentDeliveryStatusStorageForTest(ctx context.Context, selected any, runID string) ([]DeploymentDeliveryStatusStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.ReadDeploymentDeliveryStatusStorageTx)
}

func ReadSelectedDeploymentOperationStorageForTest(ctx context.Context, selected any, runID string) (SelectedDeploymentOperationStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, runforkpersistence.ReadSelectedDeploymentOperationStorageTx)
}

func ReadSelectedForkKeyStorageForTest(ctx context.Context, selected any, key string) (SelectedForkKeyStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, key, runforkpersistence.ReadSelectedForkKeyStorageTx)
}

func ReadSelectedForkKeyRunStateStorageForTest(ctx context.Context, selected any, key, run string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, key); err != nil {
		return "", err
	}
	return readDeploymentStorageForTest(ctx, selected, run, func(ctx context.Context, tx *sql.Tx, run string) (string, error) {
		return runforkpersistence.ReadSelectedForkKeyRunStateStorageTx(ctx, tx, key, run)
	})
}

func CountSelectedForkKeyOperationsStorageForTest(ctx context.Context, selected any, key string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, key, runforkpersistence.CountSelectedForkKeyOperationsStorageTx)
}

func CountSelectedForkKeyActivatedOperationsStorageForTest(ctx context.Context, selected any, key string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, key, runforkpersistence.CountSelectedForkKeyActivatedOperationsStorageTx)
}

func CountSelectedForkSourceOperationsStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, run, runforkpersistence.CountSelectedForkSourceOperationsStorageTx)
}

func ReadLatestSelectedDeploymentExecutionStateStorageForTest(ctx context.Context, selected any, run string) (string, error) {
	return readDeploymentStorageForTest(ctx, selected, run, runforkpersistence.ReadLatestSelectedDeploymentExecutionStateStorageTx)
}

func ReadLatestSelectedDeploymentExecutionIdentityStorageForTest(ctx context.Context, selected any, run string) (SelectedDeploymentExecutionIdentityStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, run, runforkpersistence.ReadLatestSelectedDeploymentExecutionIdentityStorageTx)
}

func ReadSelectedDeploymentPointStorageForTest(ctx context.Context, selected any, run string) (SelectedDeploymentPointStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, run, runforkpersistence.ReadSelectedDeploymentPointStorageTx)
}

func ReadSelectedDeploymentExecutionsStorageForTest(ctx context.Context, selected any, runID string) ([]SelectedDeploymentExecutionStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, runforkpersistence.ReadSelectedDeploymentExecutionsStorageTx)
}

func CountClosedSelectedDeploymentExecutionsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, runforkpersistence.CountClosedSelectedDeploymentExecutionsStorageTx)
}

func ReadDeploymentRunStatusStorageForTest(ctx context.Context, selected any, runID string) (DeploymentRunStatusStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, runlifecycle.ReadDeploymentRunStatusStorageTx)
}

func ReadDeploymentRunControlStorageForTest(ctx context.Context, selected any, runID string) (DeploymentRunControlStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, runlifecycle.ReadDeploymentRunControlStorageTx)
}

func ReadDeploymentParentRunStorageForTest(ctx context.Context, selected any, runID string) (DeploymentNullableText, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, runlifecycle.ReadDeploymentParentRunStorageTx)
}

func CountAdmittedUnboundBundleGrantsStorageForTest(ctx context.Context, selected any, bundleHash string) (int, error) {
	if bundleHash == "" {
		return 0, fmt.Errorf("deployment grant observation requires an exact bundle hash")
	}
	return readSelectedPreflightGlobalCount(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return generationauthority.CountAdmittedUnboundBundleGrantsStorageTx(ctx, tx, bundleHash)
	})
}
