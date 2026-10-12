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
