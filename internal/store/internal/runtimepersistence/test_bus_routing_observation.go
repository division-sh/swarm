package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type BusAncestorDeliveryDiagnostic = delivery.BusAncestorDeliveryDiagnostic

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
