package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
)

// NewPostgresStoreForTest binds an externally managed test database to the
// selected-store facade. Production construction must use NewPostgresStore.
func NewPostgresStoreForTest(db *sql.DB) *PostgresStore {
	return newPostgresStoreWithBackend(mustPostgresBackend(db))
}

func mustPostgresBackend(db *sql.DB) *postgresbackend.Backend {
	backend, err := postgresbackend.New(db)
	if err != nil {
		panic(err)
	}
	return backend
}

// NewSQLiteRuntimeStoreForTest binds an externally managed test database to
// the selected-store facade. Production construction must use
// NewSQLiteRuntimeStore.
func NewSQLiteRuntimeStoreForTest(db *sql.DB) *SQLiteRuntimeStore {
	backend, err := sqlitebackend.New(db)
	if err != nil {
		panic(err)
	}
	schema, err := storeschema.NewSQLiteWithBackend(backend, "")
	if err != nil {
		panic(err)
	}
	store, err := newSQLiteStoreComposition(schema, backend, nil)
	if err != nil {
		panic(err)
	}
	return store
}

// DatabaseForTest exposes a fixture-owned database for exact persistence
// readback without adding a raw capability to either selected-store facade.
func DatabaseForTest(selected any) *sql.DB {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil && store.backend != nil {
			return store.backend.ConstructionHandle()
		}
	case *SQLiteRuntimeStore:
		if store != nil && store.backend != nil {
			return store.backend.ConstructionHandle()
		}
	}
	return nil
}

// CommitPersistedEventDeliveryFixtureForTest is the named test-only operation
// for adding executable routes to an event that is already persisted.
func CommitPersistedEventDeliveryFixtureForTest(ctx context.Context, selected any, eventID, runID string, routes []events.DeliveryRoute) error {
	routes = events.NormalizeDeliveryRoutes(routes)
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres fixture store is required")
		}
		adapter, err := storedelivery.NewAdapter(storedelivery.DialectPostgres)
		if err != nil {
			return fmt.Errorf("construct postgres delivery fixture adapter: %w", err)
		}
		return store.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			authority, err := persistedEventDeliveryFixtureAuthority(txctx, tx, runID, false)
			if err != nil {
				return err
			}
			_, err = adapter.CommitInitial(txctx, tx, eventID, runID, routes, authority)
			return err
		})
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite fixture store is required")
		}
		adapter, err := storedelivery.NewAdapter(storedelivery.DialectSQLite)
		if err != nil {
			return fmt.Errorf("construct sqlite delivery fixture adapter: %w", err)
		}
		return store.backend.RunTransaction(ctx, "persisted event delivery fixture", func(txctx context.Context, tx *sql.Tx) error {
			authority, err := persistedEventDeliveryFixtureAuthority(txctx, tx, runID, true)
			if err != nil {
				return err
			}
			_, err = adapter.CommitInitial(txctx, tx, eventID, runID, routes, authority)
			return err
		})
	default:
		return fmt.Errorf("persisted event delivery fixture store %T is unsupported", selected)
	}
}

func persistedEventDeliveryFixtureAuthority(ctx context.Context, tx *sql.Tx, runID string, sqlite bool) (runtimedelivery.ExecutionAuthority, error) {
	query := `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`
	if sqlite {
		query = `SELECT bundle_hash FROM runs WHERE run_id=?`
	}
	var bundleHash string
	if err := tx.QueryRowContext(ctx, query, runID).Scan(&bundleHash); err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("load delivery fixture run source artifact: %w", err)
	}
	source, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("construct delivery fixture source: %w", err)
	}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "storetest:"+runID, 1)
	if err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("construct delivery fixture authority: %w", err)
	}
	return authority, nil
}
