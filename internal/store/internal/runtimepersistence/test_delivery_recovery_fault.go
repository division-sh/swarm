package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"time"
)

func InstallDeliveryDiagnosticFaultForTest(ctx context.Context, selected any) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return delivery.InstallConformanceDiagnosticFaultTx(ctx, tx, postgres)
	})
}
func RemoveDeliveryDiagnosticFaultForTest(ctx context.Context, selected any) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return delivery.RemoveConformanceDiagnosticFaultTx(ctx, tx, postgres)
	})
}
func InstallNodeRecoveryPoisonFaultForTest(ctx context.Context, selected any, id string) error {
	if err := validateStandaloneStorageEvent(selected, id); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return entitystore.InstallNodeRecoveryPoisonFaultTx(ctx, tx, postgres, id)
	})
}
func RemoveNodeRecoveryPoisonFaultForTest(ctx context.Context, selected any) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return entitystore.RemoveNodeRecoveryPoisonFaultTx(ctx, tx, postgres)
	})
}
func TerminateDeliveryConformanceSessionForTest(ctx context.Context, selected any, id string, at time.Time) error {
	if err := validateStandaloneStorageEvent(selected, id); err != nil {
		return err
	}
	if at.IsZero() {
		return fmt.Errorf("session termination requires the original exact clock")
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return llmpersistence.TerminateDeliveryConformanceSessionTx(ctx, tx, id, at)
	})
}
