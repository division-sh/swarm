package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"

	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type PublicationTemplateIdentityStorage = pipelinepersistence.PublicationTemplateIdentityStorage
type PublicationRuleSelectionStorage = delivery.PublicationRuleSelectionStorage
type NumericRunEntityStorage = entitystore.NumericRunEntityStorage
type ForkReceiverSettlementStorage = delivery.ForkReceiverSettlementStorage
type ReceiverFieldMutationStorage = mutationlog.ReceiverFieldMutationStorage
type LoopReceiverSettlementStorage = delivery.LoopReceiverSettlementStorage
type ReceiverAgentIdentityStorage = agentpersistence.ReceiverAgentIdentityStorage

func ReadActivityPublicationTemplatePathStorageForTest(ctx context.Context, selected any, runID, flow string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadActivityPublicationTemplatePathStorageTx(ctx, tx, runID, flow)
	})
}

func ReadActivityPublicationTemplateEntityStorageForTest(ctx context.Context, selected any, runID, path, flow string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadActivityPublicationTemplateEntityStorageTx(ctx, tx, runID, path, flow)
	})
}

func ReadLoopObservedRevisionMutationStorageForTest(ctx context.Context, selected any, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ReceiverFieldMutationStorage, error) {
		return mutationlog.ReadLoopObservedRevisionMutationStorageTx(ctx, tx, runID, entityID, eventID)
	})
}

func CountLoopObservedRevisionMutationsStorageForTest(ctx context.Context, selected any, runID, entityID, eventID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountLoopObservedRevisionMutationsStorageTx(ctx, tx, runID, entityID, eventID)
	})
}

func ReadLoopReceiverSettlementStorageForTest(ctx context.Context, selected any, runID, deliveryID string) (LoopReceiverSettlementStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (LoopReceiverSettlementStorage, error) {
		return delivery.ReadLoopReceiverSettlementStorageTx(ctx, tx, runID, deliveryID)
	})
}

func CountReceiverFirstClaimAgentDeliveriesStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountReceiverFirstClaimAgentDeliveriesStorageTx(ctx, tx, runID)
	})
}

func ReadReceiverAgentIdentityStorageForTest(ctx context.Context, selected any, runID, instance string) (ReceiverAgentIdentityStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ReceiverAgentIdentityStorage, error) {
		return agentpersistence.ReadReceiverAgentIdentityStorageTx(ctx, tx, runID, instance)
	})
}

func CountReceiverLiveParsedTurnsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return llmpersistence.CountReceiverLiveParsedTurnsStorageTx(ctx, tx, runID)
	})
}

func CountReceiverRunTurnsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return llmpersistence.CountReceiverRunTurnsStorageTx(ctx, tx, runID)
	})
}

func ReadReceiverParentEntityIdentityStorageForTest(ctx context.Context, selected any, runID, instance string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return entitystore.ReadReceiverParentEntityIdentityStorageTx(ctx, tx, runID, instance)
	})
}

func ReadReceiverBusinessTokenMutationStorageForTest(ctx context.Context, selected any, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ReceiverFieldMutationStorage, error) {
		return mutationlog.ReadReceiverBusinessTokenMutationStorageTx(ctx, tx, runID, entityID, eventID)
	})
}

func ReadReceiverProcessedCountMutationStorageForTest(ctx context.Context, selected any, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ReceiverFieldMutationStorage, error) {
		return mutationlog.ReadReceiverProcessedCountMutationStorageTx(ctx, tx, runID, entityID, eventID)
	})
}

func ReadReceiverMarkerMutationStorageForTest(ctx context.Context, selected any, runID, entityID, eventID string) ([]ReceiverFieldMutationStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]ReceiverFieldMutationStorage, error) {
		return mutationlog.ReadReceiverMarkerMutationStorageTx(ctx, tx, runID, entityID, eventID)
	})
}

func CountReceiverBusinessTokenMutationsStorageForTest(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountReceiverBusinessTokenMutationsStorageTx(ctx, tx, runID, eventID)
	})
}

func CountReceiverMarkerMutationsStorageForTest(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountReceiverMarkerMutationsStorageTx(ctx, tx, runID, eventID)
	})
}

func CountReceiverProcessedCountMutationsStorageForTest(ctx context.Context, selected any, runID, entityID, eventID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountReceiverProcessedCountMutationsStorageTx(ctx, tx, runID, entityID, eventID)
	})
}

func CountReceiverEntityCausedMutationsStorageForTest(ctx context.Context, selected any, runID, entityID, eventID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountReceiverEntityCausedMutationsStorageTx(ctx, tx, runID, entityID, eventID)
	})
}

func ReadForkReceiverSettlementStorageForTest(ctx context.Context, selected any, runID, deliveryID string) (ForkReceiverSettlementStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ForkReceiverSettlementStorage, error) {
		return delivery.ReadForkReceiverSettlementStorageTx(ctx, tx, runID, deliveryID)
	})
}

func ReadForkReceiverBusinessSettlementStorageForTest(ctx context.Context, selected any, runID, deliveryID string) (ForkReceiverSettlementStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ForkReceiverSettlementStorage, error) {
		return delivery.ReadForkReceiverBusinessSettlementStorageTx(ctx, tx, runID, deliveryID)
	})
}

func CountForkReceiverSettledAttemptsStorageForTest(ctx context.Context, selected any, deliveryID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountForkReceiverSettledAttemptsStorageTx(ctx, tx, deliveryID)
	})
}

func ReadForkReceiverRuleSelectionStorageForTest(ctx context.Context, selected any, deliveryID string) (PublicationRuleSelectionStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (PublicationRuleSelectionStorage, error) {
		return delivery.ReadForkReceiverRuleSelectionStorageTx(ctx, tx, deliveryID)
	})
}

func ReadNumericRunEntityStorageForTest(ctx context.Context, selected any, runID string) (NumericRunEntityStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (NumericRunEntityStorage, error) {
		return entitystore.ReadNumericRunEntityStorageTx(ctx, tx, runID)
	})
}

func ReadNumericRunEntityIdentityStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return entitystore.ReadNumericRunEntityIdentityStorageTx(ctx, tx, runID)
	})
}

func ReadNumericMockActivityResultStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return activityjournal.ReadNumericMockActivityResultStorageTx(ctx, tx, runID)
	})
}

func CountOpenPublicationFanOutIntentsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountOpenPublicationFanOutIntentsStorageTx(ctx, tx, runID)
	})
}

func CountNoncommittedPublicationFanOutOutcomesStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountNoncommittedPublicationFanOutOutcomesStorageTx(ctx, tx, runID)
	})
}

func ReadPublicationFanOutFailureStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadPublicationFanOutFailureStorageTx(ctx, tx, runID)
	})
}

func ReadPublicationTemplateIdentityStorageForTest(ctx context.Context, selected any, runID, entityID string) (PublicationTemplateIdentityStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (PublicationTemplateIdentityStorage, error) {
		return pipelinepersistence.ReadPublicationTemplateIdentityStorageTx(ctx, tx, runID, entityID)
	})
}

func ReadPublicationRuleSelectionStorageForTest(ctx context.Context, selected any, runID, eventID, subscriberID string) (PublicationRuleSelectionStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (PublicationRuleSelectionStorage, error) {
		return delivery.ReadPublicationRuleSelectionStorageTx(ctx, tx, runID, eventID, subscriberID)
	})
}

func ReadConnectionWorkerInstancePathsStorageForTest(ctx context.Context, selected any, runID string) ([]string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return pipelinepersistence.ReadConnectionWorkerInstancePathsStorageTx(ctx, tx, runID)
	})
}

func ReadDeliveryPayloadProjectionTextStorageForTest(ctx context.Context, selected any, deliveryID string) (string, error) {
	_, postgres := selected.(*PostgresStore)
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadDeliveryPayloadProjectionTextStorageTx(ctx, tx, postgres, deliveryID)
	})
}
