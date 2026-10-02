package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
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
		return store.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			return storedurabledata.RegisterCatalogTx(store.durableDataOwner, txctx, tx, catalog, time.Now().UTC())
		})
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil || store.durableDataOwner == nil {
			return fmt.Errorf("sqlite durable data fixture store is required")
		}
		if err := store.requireCurrentSchema(); err != nil {
			return err
		}
		return store.backend.RunTransaction(ctx, "durable data catalog fixture", func(txctx context.Context, tx *sql.Tx) error {
			return storedurabledata.RegisterCatalogTx(store.durableDataOwner, txctx, tx, catalog, time.Now().UTC())
		})
	default:
		return fmt.Errorf("durable data catalog fixture store %T is unsupported", selected)
	}
}

// MaterializeDataForkPinsForTest invokes the same pin owner and transaction
// coordinator used by run creation. No transaction capability leaves this seam.
func MaterializeDataForkPinsForTest(ctx context.Context, selected any, sourceRunID, forkRunID, targetBundleHash string, overrides []runtimedata.ExplicitPin, replay bool) ([]runtimedata.Pin, error) {
	var pins []runtimedata.Pin
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil || store.durableDataOwner == nil {
			return nil, fmt.Errorf("postgres durable data fixture store is required")
		}
		if err := store.requireCurrentSchema(); err != nil {
			return nil, err
		}
		err := store.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			pins, err = storedurabledata.MaterializeForkPinsTx(store.durableDataOwner, txctx, tx, sourceRunID, forkRunID, targetBundleHash, overrides, replay, time.Now().UTC())
			return err
		})
		if err != nil {
			return nil, err
		}
		return pins, nil
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil || store.durableDataOwner == nil {
			return nil, fmt.Errorf("sqlite durable data fixture store is required")
		}
		if err := store.requireCurrentSchema(); err != nil {
			return nil, err
		}
		err := store.backend.RunTransaction(ctx, "durable data fork pin fixture", func(txctx context.Context, tx *sql.Tx) error {
			var err error
			pins, err = storedurabledata.MaterializeForkPinsTx(store.durableDataOwner, txctx, tx, sourceRunID, forkRunID, targetBundleHash, overrides, replay, time.Now().UTC())
			return err
		})
		if err != nil {
			return nil, err
		}
		return pins, nil
	default:
		return nil, fmt.Errorf("durable data fork pin fixture store %T is unsupported", selected)
	}
}
