package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
)

type BusAncestorDeliveryDiagnostic = delivery.BusAncestorDeliveryDiagnostic

type BusRoutingDeliveryDiagnostic = delivery.BusRoutingDeliveryDiagnostic

func CountBusValidationDeadLetterNodeForTest(ctx context.Context, selected any, node string) (int, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.CountBusValidationDeadLetterNodeTx(ctx, tx, node)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountUndeclaredBusChildEntitiesForTest(ctx context.Context, selected any) (int, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = entitystore.CountUndeclaredBusChildEntitiesTx(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadBusRoutingDeliveryDiagnosticsForTest(ctx context.Context, selected any) ([]BusRoutingDeliveryDiagnostic, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var out []BusRoutingDeliveryDiagnostic
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadBusRoutingDeliveryDiagnosticsTx(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadBusAncestorDeliveryDiagnosticsForTest(ctx context.Context, selected any) ([]BusAncestorDeliveryDiagnostic, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var out []BusAncestorDeliveryDiagnostic
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadBusAncestorDeliveryDiagnosticsTx(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
