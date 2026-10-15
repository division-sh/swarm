package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PublicationResultSourceStorage = private.PublicationResultSourceStorage
type PublicationTemplateIdentityStorage = private.PublicationTemplateIdentityStorage
type PublicationRuleSelectionStorage = private.PublicationRuleSelectionStorage
type NumericRunEntityStorage = private.NumericRunEntityStorage
type ForkReceiverSettlementStorage = private.ForkReceiverSettlementStorage
type ReceiverFieldMutationStorage = private.ReceiverFieldMutationStorage
type ReceiverFinishedEffectStorage = private.ReceiverFinishedEffectStorage
type LoopReceiverSettlementStorage = private.LoopReceiverSettlementStorage
type ReceiverAgentIdentityStorage = private.ReceiverAgentIdentityStorage
type ActivityPublicationSourceStorage = private.ActivityPublicationSourceStorage

func ReadActivityPublicationTemplatePathStorage(ctx context.Context, selected any, runID, flow string) (string, error) {
	return private.ReadActivityPublicationTemplatePathStorageForTest(ctx, selected, runID, flow)
}

func ReadActivityPublicationTemplateEntityStorage(ctx context.Context, selected any, runID, path, flow string) (string, error) {
	return private.ReadActivityPublicationTemplateEntityStorageForTest(ctx, selected, runID, path, flow)
}

func ReadLoopObservedRevisionMutationStorage(ctx context.Context, selected any, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	return private.ReadLoopObservedRevisionMutationStorageForTest(ctx, selected, runID, entityID, eventID)
}

func CountLoopObservedRevisionMutationsStorage(ctx context.Context, selected any, runID, entityID, eventID string) (int, error) {
	return private.CountLoopObservedRevisionMutationsStorageForTest(ctx, selected, runID, entityID, eventID)
}

func ReadLoopReceiverSettlementStorage(ctx context.Context, selected any, runID, deliveryID string) (LoopReceiverSettlementStorage, error) {
	return private.ReadLoopReceiverSettlementStorageForTest(ctx, selected, runID, deliveryID)
}

func CountReceiverFirstClaimAgentDeliveriesStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountReceiverFirstClaimAgentDeliveriesStorageForTest(ctx, selected, runID)
}

func ReadReceiverAgentIdentityStorage(ctx context.Context, selected any, runID, instance string) (ReceiverAgentIdentityStorage, error) {
	return private.ReadReceiverAgentIdentityStorageForTest(ctx, selected, runID, instance)
}

func CountReceiverLiveParsedTurnsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountReceiverLiveParsedTurnsStorageForTest(ctx, selected, runID)
}

func CountReceiverRunTurnsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountReceiverRunTurnsStorageForTest(ctx, selected, runID)
}

func ReadActivityPublicationSourceStorage(ctx context.Context, selected any, runID, eventID string) (ActivityPublicationSourceStorage, error) {
	return private.ReadActivityPublicationSourceStorageForTest(ctx, selected, runID, eventID)
}

func CountActivityPublicationResultArmsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountActivityPublicationResultArmsStorageForTest(ctx, selected, runID)
}

func ReadReceiverParentEntityIdentityStorage(ctx context.Context, selected any, runID, instance string) (string, error) {
	return private.ReadReceiverParentEntityIdentityStorageForTest(ctx, selected, runID, instance)
}

func ReadReceiverFinishedEffectStorage(ctx context.Context, selected any, runID, name, sourceEventID string) (ReceiverFinishedEffectStorage, error) {
	return private.ReadReceiverFinishedEffectStorageForTest(ctx, selected, runID, name, sourceEventID)
}

func ReadReceiverBusinessTokenMutationStorage(ctx context.Context, selected any, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	return private.ReadReceiverBusinessTokenMutationStorageForTest(ctx, selected, runID, entityID, eventID)
}

func ReadReceiverProcessedCountMutationStorage(ctx context.Context, selected any, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	return private.ReadReceiverProcessedCountMutationStorageForTest(ctx, selected, runID, entityID, eventID)
}

func ReadReceiverMarkerMutationStorage(ctx context.Context, selected any, runID, entityID, eventID string) ([]ReceiverFieldMutationStorage, error) {
	return private.ReadReceiverMarkerMutationStorageForTest(ctx, selected, runID, entityID, eventID)
}

func CountReceiverBusinessTokenMutationsStorage(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return private.CountReceiverBusinessTokenMutationsStorageForTest(ctx, selected, runID, eventID)
}

func CountReceiverMarkerMutationsStorage(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return private.CountReceiverMarkerMutationsStorageForTest(ctx, selected, runID, eventID)
}

func CountReceiverProcessedCountMutationsStorage(ctx context.Context, selected any, runID, entityID, eventID string) (int, error) {
	return private.CountReceiverProcessedCountMutationsStorageForTest(ctx, selected, runID, entityID, eventID)
}

func CountReceiverEntityCausedMutationsStorage(ctx context.Context, selected any, runID, entityID, eventID string) (int, error) {
	return private.CountReceiverEntityCausedMutationsStorageForTest(ctx, selected, runID, entityID, eventID)
}

func ReadForkReceiverSettlementStorage(ctx context.Context, selected any, runID, deliveryID string) (ForkReceiverSettlementStorage, error) {
	return private.ReadForkReceiverSettlementStorageForTest(ctx, selected, runID, deliveryID)
}

func ReadForkReceiverBusinessSettlementStorage(ctx context.Context, selected any, runID, deliveryID string) (ForkReceiverSettlementStorage, error) {
	return private.ReadForkReceiverBusinessSettlementStorageForTest(ctx, selected, runID, deliveryID)
}

func CountForkReceiverSettledAttemptsStorage(ctx context.Context, selected any, deliveryID string) (int, error) {
	return private.CountForkReceiverSettledAttemptsStorageForTest(ctx, selected, deliveryID)
}

func ReadForkReceiverRuleSelectionStorage(ctx context.Context, selected any, deliveryID string) (PublicationRuleSelectionStorage, error) {
	return private.ReadForkReceiverRuleSelectionStorageForTest(ctx, selected, deliveryID)
}

func ReadNumericCompletionPayloadStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadNumericCompletionPayloadStorageForTest(ctx, selected, runID)
}

func CountNumericRunNamedEventStorage(ctx context.Context, selected any, runID, name string) (int, error) {
	return private.CountNumericRunNamedEventStorageForTest(ctx, selected, runID, name)
}

func ReadNumericRunEntityStorage(ctx context.Context, selected any, runID string) (NumericRunEntityStorage, error) {
	return private.ReadNumericRunEntityStorageForTest(ctx, selected, runID)
}

func ReadNumericRunEntityIdentityStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadNumericRunEntityIdentityStorageForTest(ctx, selected, runID)
}

func ReadNumericMockActivityResultStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadNumericMockActivityResultStorageForTest(ctx, selected, runID)
}

func ReadPublicationResultSourcesStorage(ctx context.Context, selected any, runID string) ([]PublicationResultSourceStorage, error) {
	return private.ReadPublicationResultSourcesStorageForTest(ctx, selected, runID)
}

func CountOpenPublicationFanOutIntentsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountOpenPublicationFanOutIntentsStorageForTest(ctx, selected, runID)
}

func CountNoncommittedPublicationFanOutOutcomesStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountNoncommittedPublicationFanOutOutcomesStorageForTest(ctx, selected, runID)
}

func ReadPublicationFanOutFailureStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPublicationFanOutFailureStorageForTest(ctx, selected, runID)
}

func ReadPublicationTemplateIdentityStorage(ctx context.Context, selected any, runID, entityID string) (PublicationTemplateIdentityStorage, error) {
	return private.ReadPublicationTemplateIdentityStorageForTest(ctx, selected, runID, entityID)
}

func ReadPublicationRuleSelectionStorage(ctx context.Context, selected any, runID, eventID, subscriberID string) (PublicationRuleSelectionStorage, error) {
	return private.ReadPublicationRuleSelectionStorageForTest(ctx, selected, runID, eventID, subscriberID)
}

func ReadConnectionWorkerInstancePathsStorage(ctx context.Context, selected any, runID string) ([]string, error) {
	return private.ReadConnectionWorkerInstancePathsStorageForTest(ctx, selected, runID)
}

func ReadOrderedConnectionReadyEventIDsStorage(ctx context.Context, selected any, runID string) ([]string, error) {
	return private.ReadOrderedConnectionReadyEventIDsStorageForTest(ctx, selected, runID)
}

func ReadDeliveryPayloadProjectionTextStorage(ctx context.Context, selected any, deliveryID string) (string, error) {
	return private.ReadDeliveryPayloadProjectionTextStorageForTest(ctx, selected, deliveryID)
}
