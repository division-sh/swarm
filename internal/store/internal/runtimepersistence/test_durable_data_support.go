package runtimepersistence

import (
	"context"
	"fmt"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

// RegisterDurableDataCatalogForTest preserves fixture catalogs without creating
// a second durable-data owner or bypassing the selected writer coordinator.
func RegisterDurableDataCatalogForTest(ctx context.Context, selected any, catalog runtimedata.Catalog) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil || store.durableDataOwner == nil {
			return fmt.Errorf("postgres durable data fixture store is required")
		}
		if err := store.requireCurrentSchema(); err != nil {
			return err
		}
		return store.durableDataOwner.RegisterCatalogForTest(ctx, catalog)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil || store.durableDataOwner == nil {
			return fmt.Errorf("sqlite durable data fixture store is required")
		}
		if err := store.requireCurrentSchema(); err != nil {
			return err
		}
		return store.durableDataOwner.RegisterCatalogForTest(ctx, catalog)
	default:
		return fmt.Errorf("durable data catalog fixture store %T is unsupported", selected)
	}
}

// MaterializeDataForkPinsForTest invokes the same pin owner and transaction
// coordinator used by run creation. No transaction capability leaves this seam.
func MaterializeDataForkPinsForTest(ctx context.Context, selected any, sourceRunID, forkRunID, targetBundleHash string, overrides []runtimedata.ExplicitPin, replay bool) ([]runtimedata.Pin, error) {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil || store.durableDataOwner == nil {
			return nil, fmt.Errorf("postgres durable data fixture store is required")
		}
		if err := store.requireCurrentSchema(); err != nil {
			return nil, err
		}
		return store.durableDataOwner.MaterializeForkPinsForTest(ctx, sourceRunID, forkRunID, targetBundleHash, overrides, replay)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil || store.durableDataOwner == nil {
			return nil, fmt.Errorf("sqlite durable data fixture store is required")
		}
		if err := store.requireCurrentSchema(); err != nil {
			return nil, err
		}
		return store.durableDataOwner.MaterializeForkPinsForTest(ctx, sourceRunID, forkRunID, targetBundleHash, overrides, replay)
	default:
		return nil, fmt.Errorf("durable data fork pin fixture store %T is unsupported", selected)
	}
}
