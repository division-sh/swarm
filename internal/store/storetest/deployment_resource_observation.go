package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DeploymentNullableText = private.DeploymentNullableText
type DeploymentIntentProgressStorage = private.DeploymentIntentProgressStorage
type DeploymentIntentMetadataStorage = private.DeploymentIntentMetadataStorage
type DeploymentFeedStorage = private.DeploymentFeedStorage
type DeploymentIntentDiagnosticStorage = private.DeploymentIntentDiagnosticStorage
type DeploymentIntentCardinalityStorage = private.DeploymentIntentCardinalityStorage
type PinnedDeploymentFeedStorage = private.PinnedDeploymentFeedStorage
type DeploymentSettlementDiagnosticStorage = private.DeploymentSettlementDiagnosticStorage
type PinnedPortfolioDeliveryDiagnosticStorage = private.PinnedPortfolioDeliveryDiagnosticStorage
type DeploymentReceiptStorage = private.DeploymentReceiptStorage
type DeploymentDeliveryStorage = private.DeploymentDeliveryStorage
type DeploymentAttemptStorage = private.DeploymentAttemptStorage
type DeploymentDeliveryStatusStorage = private.DeploymentDeliveryStatusStorage
type SelectedDeploymentOperationStorage = private.SelectedDeploymentOperationStorage
type SelectedDeploymentExecutionStorage = private.SelectedDeploymentExecutionStorage
type SelectedForkKeyStorage = private.SelectedForkKeyStorage
type SelectedDeploymentExecutionIdentityStorage = private.SelectedDeploymentExecutionIdentityStorage
type SelectedDeploymentPointStorage = private.SelectedDeploymentPointStorage
type DeploymentRunStatusStorage = private.DeploymentRunStatusStorage
type DeploymentRunControlStorage = private.DeploymentRunControlStorage
type DeploymentRuntimeLogStorage = private.DeploymentRuntimeLogStorage
type DeploymentRecentEventStorage = private.DeploymentRecentEventStorage

func ReadRootReadyResourcePinStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadRootReadyResourcePinStorageForTest(ctx, selected, runID)
}

func ReadPortfolioAccountResourcePinStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPortfolioAccountResourcePinStorageForTest(ctx, selected, runID)
}

func CountAccountDeploymentReceiversStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountAccountDeploymentReceiversStorageForTest(ctx, selected, runID)
}

func CountConsumerDeploymentReceiversStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountConsumerDeploymentReceiversStorageForTest(ctx, selected, runID)
}

func CountDeploymentRouteObligationsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountDeploymentRouteObligationsStorageForTest(ctx, selected, runID)
}

func CountPortfolioDeliveredDeploymentRowsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountPortfolioDeliveredDeploymentRowsStorageForTest(ctx, selected, runID)
}

func CountPortfolioAllDeploymentRowsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountPortfolioAllDeploymentRowsStorageForTest(ctx, selected, runID)
}

func ReadDeploymentIntentVersionStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadDeploymentIntentVersionStorageForTest(ctx, selected, runID)
}

func ReadDeploymentIntentCursorStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.ReadDeploymentIntentCursorStorageForTest(ctx, selected, runID)
}

func ReadDeploymentOriginIntentVersionStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadDeploymentOriginIntentVersionStorageForTest(ctx, selected, runID)
}

func CountDeploymentOriginIntentsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountDeploymentOriginIntentsStorageForTest(ctx, selected, runID)
}

func ReadDeploymentIntentCardinalityStorage(ctx context.Context, selected any, runID string) (DeploymentIntentCardinalityStorage, error) {
	return private.ReadDeploymentIntentCardinalityStorageForTest(ctx, selected, runID)
}

func ReadDeploymentOriginIntentStateStorage(ctx context.Context, selected any, runID string) (DeploymentIntentDiagnosticStorage, error) {
	return private.ReadDeploymentOriginIntentStateStorageForTest(ctx, selected, runID)
}

func ReadDeploymentCommittedOrdinalsStorage(ctx context.Context, selected any, runID string) ([]CommittedFanOutOrdinalStorage, error) {
	return private.ReadDeploymentCommittedOrdinalsStorageForTest(ctx, selected, runID)
}

func ReadPinnedDeploymentFeedsStorage(ctx context.Context, selected any, run string) ([]PinnedDeploymentFeedStorage, error) {
	return private.ReadPinnedDeploymentFeedsStorageForTest(ctx, selected, run)
}

func ReadDeploymentSettlementDiagnosticsStorage(ctx context.Context, selected any, run string) ([]DeploymentSettlementDiagnosticStorage, error) {
	return private.ReadDeploymentSettlementDiagnosticsStorageForTest(ctx, selected, run)
}

func CountPinnedPortfolioSuccessfulPipelineReceiptsStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountPinnedPortfolioSuccessfulPipelineReceiptsStorageForTest(ctx, selected, run)
}

func CountPinnedPortfolioSettledDeliveriesStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountPinnedPortfolioSettledDeliveriesStorageForTest(ctx, selected, run)
}

func ReadPinnedPortfolioDeliveryDiagnosticsStorage(ctx context.Context, selected any, run string) ([]PinnedPortfolioDeliveryDiagnosticStorage, error) {
	return private.ReadPinnedPortfolioDeliveryDiagnosticsStorageForTest(ctx, selected, run)
}

func CountPinnedPortfolioSourceEventsStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountPinnedPortfolioSourceEventsStorageForTest(ctx, selected, run)
}

func ReadDeploymentIntentProgressStorage(ctx context.Context, selected any, runID string) (DeploymentIntentProgressStorage, error) {
	return private.ReadDeploymentIntentProgressStorageForTest(ctx, selected, runID)
}

func ReadDeploymentIntentMetadataStorage(ctx context.Context, selected any, runID string) (DeploymentIntentMetadataStorage, error) {
	return private.ReadDeploymentIntentMetadataStorageForTest(ctx, selected, runID)
}

func ReadDeploymentFeedStorage(ctx context.Context, selected any, runID string) ([]DeploymentFeedStorage, error) {
	return private.ReadDeploymentFeedStorageForTest(ctx, selected, runID)
}

func ReadDeploymentIntentDiagnosticsStorage(ctx context.Context, selected any, runID string) ([]DeploymentIntentDiagnosticStorage, error) {
	return private.ReadDeploymentIntentDiagnosticsStorageForTest(ctx, selected, runID)
}

func CountDeploymentCommittedOutcomesStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountDeploymentCommittedOutcomesStorageForTest(ctx, selected, runID)
}

func CountDeploymentPipelineReceiptsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountDeploymentPipelineReceiptsStorageForTest(ctx, selected, runID)
}

func CountRootReadySuccessfulPipelineReceiptsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRootReadySuccessfulPipelineReceiptsStorageForTest(ctx, selected, runID)
}

func ReadDeploymentReceiptStorage(ctx context.Context, selected any, runID string) ([]DeploymentReceiptStorage, error) {
	return private.ReadDeploymentReceiptStorageForTest(ctx, selected, runID)
}

func CountDeploymentPendingDeliveriesStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountDeploymentPendingDeliveriesStorageForTest(ctx, selected, runID)
}

func CountDeploymentFailedDeliveriesStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountDeploymentFailedDeliveriesStorageForTest(ctx, selected, runID)
}

func CountDeploymentStampedHandoffsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountDeploymentStampedHandoffsStorageForTest(ctx, selected, runID)
}

func CountRunDeliveredRowsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunDeliveredRowsStorageForTest(ctx, selected, runID)
}

func CountRootReadyDeliveredStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRootReadyDeliveredStorageForTest(ctx, selected, runID)
}

func CountRootReadyStampedHandoffsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRootReadyStampedHandoffsStorageForTest(ctx, selected, runID)
}

func ReadDeploymentDeliveryStorage(ctx context.Context, selected any, runID string) ([]DeploymentDeliveryStorage, error) {
	return private.ReadDeploymentDeliveryStorageForTest(ctx, selected, runID)
}

func ReadDeploymentAttemptStorage(ctx context.Context, selected any, runID string) ([]DeploymentAttemptStorage, error) {
	return private.ReadDeploymentAttemptStorageForTest(ctx, selected, runID)
}

func ReadDeploymentDeliveryStatusStorage(ctx context.Context, selected any, runID string) ([]DeploymentDeliveryStatusStorage, error) {
	return private.ReadDeploymentDeliveryStatusStorageForTest(ctx, selected, runID)
}

func ReadSelectedDeploymentOperationStorage(ctx context.Context, selected any, runID string) (SelectedDeploymentOperationStorage, error) {
	return private.ReadSelectedDeploymentOperationStorageForTest(ctx, selected, runID)
}

func ReadSelectedForkKeyStorage(ctx context.Context, selected any, key string) (SelectedForkKeyStorage, error) {
	return private.ReadSelectedForkKeyStorageForTest(ctx, selected, key)
}

func ReadSelectedForkKeyRunStateStorage(ctx context.Context, selected any, key, run string) (string, error) {
	return private.ReadSelectedForkKeyRunStateStorageForTest(ctx, selected, key, run)
}

func CountSelectedForkKeyOperationsStorage(ctx context.Context, selected any, key string) (int, error) {
	return private.CountSelectedForkKeyOperationsStorageForTest(ctx, selected, key)
}

func CountSelectedForkKeyActivatedOperationsStorage(ctx context.Context, selected any, key string) (int, error) {
	return private.CountSelectedForkKeyActivatedOperationsStorageForTest(ctx, selected, key)
}

func CountSelectedForkSourceOperationsStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountSelectedForkSourceOperationsStorageForTest(ctx, selected, run)
}

func ReadLatestSelectedDeploymentExecutionStateStorage(ctx context.Context, selected any, run string) (string, error) {
	return private.ReadLatestSelectedDeploymentExecutionStateStorageForTest(ctx, selected, run)
}

func ReadLatestSelectedDeploymentExecutionIdentityStorage(ctx context.Context, selected any, run string) (SelectedDeploymentExecutionIdentityStorage, error) {
	return private.ReadLatestSelectedDeploymentExecutionIdentityStorageForTest(ctx, selected, run)
}

func ReadSelectedDeploymentPointStorage(ctx context.Context, selected any, run string) (SelectedDeploymentPointStorage, error) {
	return private.ReadSelectedDeploymentPointStorageForTest(ctx, selected, run)
}

func ReadSelectedDeploymentExecutionsStorage(ctx context.Context, selected any, runID string) ([]SelectedDeploymentExecutionStorage, error) {
	return private.ReadSelectedDeploymentExecutionsStorageForTest(ctx, selected, runID)
}

func CountClosedSelectedDeploymentExecutionsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountClosedSelectedDeploymentExecutionsStorageForTest(ctx, selected, runID)
}

func ReadDeploymentRunStatusStorage(ctx context.Context, selected any, runID string) (DeploymentRunStatusStorage, error) {
	return private.ReadDeploymentRunStatusStorageForTest(ctx, selected, runID)
}

func ReadDeploymentRunControlStorage(ctx context.Context, selected any, runID string) (DeploymentRunControlStorage, error) {
	return private.ReadDeploymentRunControlStorageForTest(ctx, selected, runID)
}

func ReadDeploymentParentRunStorage(ctx context.Context, selected any, runID string) (DeploymentNullableText, error) {
	return private.ReadDeploymentParentRunStorageForTest(ctx, selected, runID)
}

func CountAdmittedUnboundBundleGrantsStorage(ctx context.Context, selected any, bundleHash string) (int, error) {
	return private.CountAdmittedUnboundBundleGrantsStorageForTest(ctx, selected, bundleHash)
}

func ReadFirstRootReadyEventStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadFirstRootReadyEventStorageForTest(ctx, selected, runID)
}

func ReadDeploymentRuntimeLogStorage(ctx context.Context, selected any, runID string) ([]DeploymentRuntimeLogStorage, error) {
	return private.ReadDeploymentRuntimeLogStorageForTest(ctx, selected, runID)
}

func ReadDeploymentRecentEventStorage(ctx context.Context, selected any, runID string) ([]DeploymentRecentEventStorage, error) {
	return private.ReadDeploymentRecentEventStorageForTest(ctx, selected, runID)
}
