package adminpersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

func (s *DestructiveResetPostgresOwner) ReadResetInventory(ctx context.Context) (destructivereset.Inventory, error) {
	if s == nil || s.backend == nil {
		return destructivereset.Inventory{}, fmt.Errorf("postgres store is required")
	}
	runs, err := s.readDestructiveResetInventoryRuns(ctx)
	if err != nil {
		return destructivereset.Inventory{}, err
	}
	deliveries, err := s.readDestructiveResetInventoryDeliveries(ctx)
	if err != nil {
		return destructivereset.Inventory{}, err
	}
	out := destructivereset.Inventory{
		CleanupRuns:        append([]destructivereset.RunRef(nil), runs...),
		CleanupRunSetKnown: true,
		ActiveDeliveries:   deliveries,
		Preserved:          destructivereset.DefaultPreservedResources(),
	}
	for _, run := range runs {
		if resetRunStatusActive(run.Status) {
			out.ActiveRuns = append(out.ActiveRuns, run)
		}
	}
	for _, entry := range destructivereset.DefaultPlatformCleanupCatalog() {
		switch entry.Classification {
		case destructivereset.CleanupPreserve, destructivereset.CleanupSplitPreserve, destructivereset.CleanupRequestScopedBundles:
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

func (s *DestructiveResetPostgresOwner) readDestructiveResetInventoryRuns(ctx context.Context) ([]destructivereset.RunRef, error) {
	rows, err := s.backend.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(status, '')
		FROM runs
		ORDER BY run_id::text
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

func (s *DestructiveResetPostgresOwner) readDestructiveResetInventoryDeliveries(ctx context.Context) ([]destructivereset.DeliveryRef, error) {
	adapter, err := deliveryadapter.NewAdapter(deliveryadapter.DialectPostgres)
	if err != nil {
		return nil, err
	}
	snapshots, err := adapter.ActiveSnapshots(ctx, s.backend)
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
