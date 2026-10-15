package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type StandingDeliveryDiagnostic = delivery.StandingDeliveryDiagnostic

func ReadStandingDeliveryDiagnosticsForTest(ctx context.Context, selected any) ([]StandingDeliveryDiagnostic, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) ([]StandingDeliveryDiagnostic, error) {
		return delivery.ReadStandingDeliveryDiagnosticsTx(ctx, tx, postgres)
	})
}
