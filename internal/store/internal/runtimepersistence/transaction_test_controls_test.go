package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	storeownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	_ "github.com/lib/pq"
)

const sqliteRuntimeMutationRetryBudget = 5 * time.Second

func NewPostgresStore(dsn string, native ...driver.Connector) (*PostgresStore, error) {
	if len(native) > 1 || (len(native) == 1 && native[0] == nil) {
		return nil, errors.New("native PostgreSQL fixture accepts one nonnil connector")
	}
	var db *sql.DB
	var err error
	if len(native) == 0 {
		db, err = sql.Open("postgres", dsn)
	} else {
		db = sql.OpenDB(native[0])
	}
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	backend, err := postgresbackend.New(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store, err := newPostgresStoreComposition(backend)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

type nativeSQLiteFixtureConstruction struct {
	connector  driver.Connector
	possession *storeownership.SQLiteConstructionGuard
}

func NewSQLiteRuntimeStore(path string, native ...nativeSQLiteFixtureConstruction) (*SQLiteRuntimeStore, error) {
	if len(native) > 1 {
		return nil, errors.New("native SQLite fixture accepts one construction input")
	}
	var schema *storeschema.SQLite
	var backend *sqlitebackend.Backend
	var identity *storeownership.SQLiteBackendIdentity
	var err error
	if len(native) == 0 {
		schema, backend, err = storeschema.OpenSQLiteForConstruction(path)
	} else {
		if native[0].connector == nil || native[0].possession == nil {
			return nil, errors.New("native SQLite fault fixture requires connector and retained possession")
		}
		backend, err = sqlitebackend.New(sql.OpenDB(native[0].connector))
		if err == nil {
			schema, err = storeschema.NewSQLiteWithBackend(backend, path)
		}
		if err == nil {
			identity, err = native[0].possession.BackendIdentity(context.Background())
		}
		if err != nil && backend != nil {
			err = errors.Join(err, backend.Close())
		}
	}
	if err != nil {
		return nil, err
	}
	store, err := newSQLiteStoreComposition(schema, backend, identity)
	if err != nil {
		_ = schema.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) runPostgresRuntimeMutation(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, operation)
}

func (s *SQLiteRuntimeStore) runRuntimeMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, operation)
}

func (s *PostgresStore) runPrivateAuthorActivityMutation(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	return s.runPostgresRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *SQLiteRuntimeStore) runPrivateAuthorActivityMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	return s.runRuntimeMutation(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func sqliteRuntimeMutationBusyError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "sqlite_busy") || strings.Contains(text, "sqlite_locked") ||
		strings.Contains(text, "database is locked") || strings.Contains(text, "database table is locked") ||
		strings.Contains(text, "database is busy")
}
