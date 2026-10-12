package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

type SelectedForkCleanupStorageCounts struct {
	Events, Deliveries, GlobalReceipts, State, Mutations, Bindings, Lineage, RouteRecoveries int
}

func ReadPostgresSelectedForkCleanupStorageForTest(ctx context.Context, selected any, run string) (SelectedForkCleanupStorageCounts, error) {
	if err := validatePostgresPublicationIdentities(selected, run); err != nil {
		return SelectedForkCleanupStorageCounts{}, err
	}
	var out SelectedForkCleanupStorageCounts
	var err error
	out.Events, err = CountPhysicalRunEventsForTest(ctx, selected, run)
	if err != nil {
		return SelectedForkCleanupStorageCounts{}, fmt.Errorf("count fork events: %w", err)
	}
	// Preserve all eight independently timed reads, including the unscoped receipt
	// count. A later failure cannot expose provisional cleanup evidence.
	steps := []struct {
		label string
		read  func(context.Context, *sql.Tx) (int, error)
		count *int
	}{
		{"fork deliveries", func(ctx context.Context, tx *sql.Tx) (int, error) {
			return delivery.CountSelectedForkDeliveryStorageTx(ctx, tx, run)
		}, &out.Deliveries},
		{"global event receipts", pipelinepersistence.CountGlobalPipelineReceiptStorageTx, &out.GlobalReceipts},
		{"fork state", func(ctx context.Context, tx *sql.Tx) (int, error) {
			return pipelinepersistence.CountWorkflowFieldRowsForRun(ctx, tx, run)
		}, &out.State},
		{"fork mutations", func(ctx context.Context, tx *sql.Tx) (int, error) {
			return mutationlog.CountRunMutationStorageTx(ctx, tx, run)
		}, &out.Mutations},
		{"fork bindings", func(ctx context.Context, tx *sql.Tx) (int, error) {
			return runforkpersistence.CountSelectedForkBindingStorageTx(ctx, tx, run)
		}, &out.Bindings},
		{"fork lineage", func(ctx context.Context, tx *sql.Tx) (int, error) {
			return runforkpersistence.CountSelectedForkExecutionStorageTx(ctx, tx, run)
		}, &out.Lineage},
		{"fork route recoveries", func(ctx context.Context, tx *sql.Tx) (int, error) {
			return runforkpersistence.CountSelectedForkRouteRecoveryStorageTx(ctx, tx, run)
		}, &out.RouteRecoveries},
	}
	for _, step := range steps {
		if err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			value, err := step.read(ctx, tx)
			*step.count = value
			return err
		}); err != nil {
			return SelectedForkCleanupStorageCounts{}, fmt.Errorf("count %s: %w", step.label, err)
		}
	}
	return out, nil
}

func SetSelectedReplayScopeCollisionFaultForTest(ctx context.Context, selected any, enabled bool) error {
	if err := validatePostgresPublicationIdentities(selected); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.SetSelectedReplayScopeCollisionFaultTx(ctx, tx, enabled)
	})
}

func SetSelectedExecutionLineageInsertFaultForTest(ctx context.Context, selected any, enabled bool) error {
	if err := validatePostgresPublicationIdentities(selected); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return runforkpersistence.SetSelectedExecutionLineageInsertFaultTx(ctx, tx, enabled)
	})
}
