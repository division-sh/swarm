package store

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
)

func (s *PostgresStore) ReadResetInventory(ctx context.Context) (destructivereset.Inventory, error) {
	if s == nil || s.DB == nil {
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
		if activeRunQuiescenceRunStatusActive(run.Status) {
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

func (s *PostgresStore) readDestructiveResetInventoryRuns(ctx context.Context) ([]destructivereset.RunRef, error) {
	rows, err := s.DB.QueryContext(ctx, `
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

func (s *PostgresStore) readDestructiveResetInventoryDeliveries(ctx context.Context) ([]destructivereset.DeliveryRef, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT delivery_id::text, run_id::text, COALESCE(status, '')
		FROM event_deliveries
		WHERE subscriber_type IN ('agent', 'node')
		  AND `+activeRunQuiescenceDeliveryPredicateSQL("")+`
		ORDER BY run_id::text, event_id::text, subscriber_type, subscriber_id
	`)
	if err != nil {
		return nil, fmt.Errorf("read destructive reset inventory deliveries: %w", err)
	}
	defer rows.Close()
	var out []destructivereset.DeliveryRef
	for rows.Next() {
		var delivery destructivereset.DeliveryRef
		if err := rows.Scan(&delivery.DeliveryID, &delivery.RunID, &delivery.Status); err != nil {
			return nil, fmt.Errorf("scan destructive reset inventory delivery: %w", err)
		}
		out = append(out, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read destructive reset inventory delivery rows: %w", err)
	}
	return out, nil
}
