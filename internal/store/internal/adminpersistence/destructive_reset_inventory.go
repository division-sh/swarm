package adminpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

func (s *DestructiveResetPostgresOwner) ReadResetInventory(ctx context.Context) (destructivereset.Inventory, error) {
	if s == nil || s.backend == nil {
		return destructivereset.Inventory{}, fmt.Errorf("postgres store is required")
	}
	var out destructivereset.Inventory
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readResetInventoryTx(ctx, tx, s.deliveries)
		return err
	})
	return out, err
}

func readResetInventoryTx(ctx context.Context, tx *sql.Tx, deliveries *deliveryadapter.Adapter) (destructivereset.Inventory, error) {
	runs, err := readDestructiveResetInventoryRuns(ctx, tx)
	if err != nil {
		return destructivereset.Inventory{}, err
	}
	activeDeliveries, err := readDestructiveResetInventoryDeliveries(ctx, tx, deliveries)
	if err != nil {
		return destructivereset.Inventory{}, err
	}
	out := destructivereset.Inventory{
		CleanupRuns:        append([]destructivereset.RunRef(nil), runs...),
		CleanupRunSetKnown: true,
		ActiveDeliveries:   activeDeliveries,
		Preserved:          destructivereset.DefaultPreservedResources(),
	}
	for _, run := range runs {
		if resetRunStatusActive(run.Status) {
			out.ActiveRuns = append(out.ActiveRuns, run)
		}
	}
	for _, entry := range destructivereset.DefaultPlatformCleanupCatalog() {
		switch entry.Classification {
		case destructivereset.CleanupPreserve, destructivereset.CleanupSplitPreserve, destructivereset.CleanupRequestScopedSourceArtifacts:
			continue
		default:
			out.RunScopedTables = append(out.RunScopedTables, destructivereset.TableRef{
				Name:   entry.Table,
				Owner:  destructivereset.ContractRunScopedTruncation,
				Action: entry.Classification,
			})
		}
	}
	return out, nil
}

func readDestructiveResetInventoryRuns(ctx context.Context, tx *sql.Tx) ([]destructivereset.RunRef, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT CAST(run_id AS TEXT), COALESCE(status, '')
		FROM runs
		ORDER BY CAST(run_id AS TEXT)
	`)
	if err != nil {
		return nil, fmt.Errorf("read destructive reset inventory runs: %w", err)
	}
	defer rows.Close()
	var out []destructivereset.RunRef
	for rows.Next() {
		var run destructivereset.RunRef
		if err := rows.Scan(&run.RunID, &run.Status); err != nil {
			return nil, fmt.Errorf("scan destructive reset inventory run: %w", err)
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read destructive reset inventory run rows: %w", err)
	}
	return out, nil
}

func readDestructiveResetInventoryDeliveries(ctx context.Context, tx *sql.Tx, adapter *deliveryadapter.Adapter) ([]destructivereset.DeliveryRef, error) {
	snapshots, err := adapter.ActiveSnapshots(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("read destructive reset inventory deliveries: %w", err)
	}
	out := make([]destructivereset.DeliveryRef, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, destructivereset.DeliveryRef{DeliveryID: snapshot.DeliveryID, RunID: snapshot.RunID, Status: string(snapshot.Status)})
	}
	return out, nil
}

func resetRunStatusActive(raw string) bool {
	state, err := runtimerunlifecycle.ParseState(raw)
	return err == nil && state.Active()
}
