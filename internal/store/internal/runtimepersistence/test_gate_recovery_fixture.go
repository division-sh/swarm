package runtimepersistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

func CountGateRunProposedEffectContinuationsForTest(ctx context.Context, selected any, run string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return decisionpersistence.CountGateRunProposedEffectContinuationsTx(ctx, tx, run)
	})
}

func ReadGateNodeDeliveryStatusForTest(ctx context.Context, selected any, event, node string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateNodeDeliveryStatusTx(ctx, tx, event, node)
	})
}

func CountRunNonterminalDeliveryStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountRunNonterminalDeliveryStorageTx(ctx, tx, runID)
	})
}

type A2EarlyRefusalStorage = delivery.A2EarlyRefusalStorage

type CanonicalAccumulatorClaimStorage = delivery.CanonicalAccumulatorClaimStorage
type CanonicalAccumulatorRefusalStorage = delivery.CanonicalAccumulatorRefusalStorage
type ReceiverAppearanceSettlementStorage = delivery.ReceiverAppearanceSettlementStorage

func ReadCanonicalAccumulatorClaimStorageForTest(ctx context.Context, selected any, eventID string) (CanonicalAccumulatorClaimStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (CanonicalAccumulatorClaimStorage, error) {
		return delivery.ReadCanonicalAccumulatorClaimStorageTx(ctx, tx, eventID)
	})
}

func ReadCanonicalAccumulatorRefusalStorageForTest(ctx context.Context, selected any, eventID string) (CanonicalAccumulatorRefusalStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (CanonicalAccumulatorRefusalStorage, error) {
		return delivery.ReadCanonicalAccumulatorRefusalStorageTx(ctx, tx, eventID)
	})
}

func ReadReceiverAppearanceSettlementStorageForTest(ctx context.Context, selected any, eventID string) (ReceiverAppearanceSettlementStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (ReceiverAppearanceSettlementStorage, error) {
		return delivery.ReadReceiverAppearanceSettlementStorageTx(ctx, tx, eventID)
	})
}

func ReadA2EarlyRefusalForTest(ctx context.Context, selected any, eventID, nodeID string) (A2EarlyRefusalStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (A2EarlyRefusalStorage, error) {
		return delivery.ReadA2EarlyRefusalTx(ctx, tx, postgres, eventID, nodeID)
	})
}

func ReadGateNodeDeliveryIdentityForTest(ctx context.Context, selected any, eventID, nodeID string) (string, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateNodeDeliveryIdentityTx(ctx, tx, postgres, eventID, nodeID)
	})
}

func CountGateNodeSettledAttemptsForTest(ctx context.Context, selected any, eventID, nodeID string) (int, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountGateNodeSettledAttemptsTx(ctx, tx, postgres, eventID, nodeID)
	})
}

func CountGateEventNodeDeliveriesForTest(ctx context.Context, selected any, eventID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountGateEventNodeDeliveriesTx(ctx, tx, eventID)
	})
}

func ReadGateEventNodeDeliveryIdentityForTest(ctx context.Context, selected any, eventID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateEventNodeDeliveryIdentityTx(ctx, tx, eventID)
	})
}

func CountGateEventDeliveredRowsForTest(ctx context.Context, selected any, eventID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountGateEventDeliveredRowsTx(ctx, tx, eventID)
	})
}

func ReadGateEventFailureForTest(ctx context.Context, selected any, eventID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateEventFailureTx(ctx, tx, eventID)
	})
}

func ReadGateSubscriberFailureForTest(ctx context.Context, selected any, eventID, subscriberID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateSubscriberFailureTx(ctx, tx, eventID, subscriberID)
	})
}

func ReadGateNodeFailureForTest(ctx context.Context, selected any, eventID, nodeID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateNodeFailureTx(ctx, tx, eventID, nodeID)
	})
}

type WorkflowTimerNodeFailureStorage = delivery.WorkflowTimerNodeFailureStorage
type WorkflowTimerDeliveryTraceStorage = delivery.WorkflowTimerDeliveryTraceStorage

type ExactJoinNodeDeliveryStorage = delivery.ExactJoinNodeDeliveryStorage

func ReadExactJoinNodeDeliveryStorageForTest(ctx context.Context, selected any, eventID, subscriberKey string) (ExactJoinNodeDeliveryStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (ExactJoinNodeDeliveryStorage, error) {
		return delivery.ReadExactJoinNodeDeliveryStorageTx(ctx, tx, postgres, eventID, subscriberKey)
	})
}

func ReadWorkflowTimerNodeFailureForTest(ctx context.Context, selected any, eventID, nodeID string) (WorkflowTimerNodeFailureStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (WorkflowTimerNodeFailureStorage, error) {
		return delivery.ReadWorkflowTimerNodeFailureTx(ctx, tx, postgres, eventID, nodeID)
	})
}

func ReadWorkflowTimerDeliveryTraceForTest(ctx context.Context, selected any, runID string) (WorkflowTimerDeliveryTraceStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (WorkflowTimerDeliveryTraceStorage, error) {
		return delivery.ReadWorkflowTimerDeliveryTraceTx(ctx, tx, postgres, runID)
	})
}

func ReadGateSubscriberFailureOrEmptyObjectForTest(ctx context.Context, selected any, eventID, subscriberID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateSubscriberFailureOrEmptyObjectTx(ctx, tx, eventID, subscriberID)
	})
}

func ReadGateDeadLetterFailureForTest(ctx context.Context, selected any, eventID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateDeadLetterFailureTx(ctx, tx, eventID)
	})
}

func CountGateEventSubscriberDeliveriesForTest(ctx context.Context, selected any, event, subscriber string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountGateEventSubscriberDeliveriesTx(ctx, tx, event, subscriber)
	})
}

func ReadGateLatestDeadLetterFailureForTest(ctx context.Context, selected any, event string) (string, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadGateLatestDeadLetterFailureTx(ctx, tx, postgres, event)
	})
}

func InstallGateProposedEffectCreationFailureForTest(ctx context.Context, selected any) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return decisionpersistence.InstallGateProposedEffectCreationFailureTx(ctx, tx, postgres)
	})
}

func SetGateDecisionRouteAttemptFaultForTest(ctx context.Context, selected any, event string, attempt int, due time.Time) error {
	if err := validateStandaloneStorageEvent(selected, event); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return decisionpersistence.SetGateDecisionRouteAttemptFaultTx(ctx, tx, event, attempt, due)
	})
}

func SetGateDecisionRouteDueFaultForTest(ctx context.Context, selected any, event string, due time.Time) error {
	if err := validateStandaloneStorageEvent(selected, event); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return decisionpersistence.SetGateDecisionRouteDueFaultTx(ctx, tx, event, due)
	})
}
