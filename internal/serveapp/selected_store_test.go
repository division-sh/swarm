package serveapp

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type selectedStoreOwner = storeselected.Owner
type selectedStoreBootCheck = storeselected.BootCheckInput

var selectedStoreTestDatabases sync.Map

func openSelectedPostgresOwner(t testing.TB, dsn string, db *sql.DB, cfg *config.Config, bootCheck ...*selectedStoreBootCheck) *selectedStoreOwner {
	t.Helper()
	owner, err := storeselected.OpenRuntime(context.Background(), storeselected.RuntimeRequest{
		Selection:      storebackend.Selection{Backend: storebackend.BackendPostgres},
		PostgresDSN:    dsn,
		SessionLockTTL: runtimeSessionLockTTL(cfg),
		BootCheckInput: firstBootCheckInput(bootCheck),
	})
	if err != nil {
		t.Fatalf("open PostgreSQL selected store: %v", err)
	}
	if _, err := initializeServePlatformStateStores(context.Background(), owner.Schema()); err != nil {
		_ = owner.CloseUnactivated()
		t.Fatalf("admit PostgreSQL selected store: %v", err)
	}
	registerSelectedStoreTestDatabase(t, owner, db)
	return owner
}

func openSelectedSQLiteOwner(t testing.TB, path string, cfg *config.Config, bootCheck ...*selectedStoreBootCheck) *selectedStoreOwner {
	t.Helper()
	owner, err := storeselected.OpenRuntime(context.Background(), storeselected.RuntimeRequest{
		Selection:      storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: path},
		SessionLockTTL: runtimeSessionLockTTL(cfg),
		BootCheckInput: firstBootCheckInput(bootCheck),
	})
	if err != nil {
		t.Fatalf("open SQLite selected store: %v", err)
	}
	db, _, _ := selectedRuntimeStoreForTest(t, projectServeRuntimePersistence(owner))
	registerSelectedStoreTestDatabase(t, owner, db)
	return owner
}

func firstBootCheckInput(input []*selectedStoreBootCheck) *selectedStoreBootCheck {
	if len(input) == 0 {
		return nil
	}
	return input[0]
}

// Only the checked open consumes the opt-in; unchecked opens keep the allocated DSN.
func firstCheckedPostgresFixtureDSN(ctx context.Context, cfg *config.Config, allocated string, first *bool, checks []*selectedStoreBootCheck) (string, error) {
	if !*first || firstBootCheckInput(checks) == nil {
		return allocated, nil
	}
	*first = false
	return postgresDSNFromConfig(ctx, cfg.Database)
}

func registerSelectedStoreTestDatabase(t testing.TB, owner *selectedStoreOwner, db *sql.DB) {
	t.Helper()
	if owner == nil || db == nil {
		t.Fatal("selected-store privileged test database requires owner and database")
	}
	selectedStoreTestDatabases.Store(owner, db)
	t.Cleanup(func() { selectedStoreTestDatabases.Delete(owner) })
}

func selectedStoreDatabaseForTest(t testing.TB, owner *selectedStoreOwner) *sql.DB {
	t.Helper()
	db, ok := selectedStoreTestDatabases.Load(owner)
	if !ok {
		db, _, _ := selectedRuntimeStoreForTest(t, projectServeRuntimePersistence(owner))
		return db
	}
	return db.(*sql.DB)
}

func captureSelectedRuntimePersistence(t *testing.T, capture func(serveRuntimePersistence)) {
	t.Helper()
	previous := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		persistence := previous(owner)
		capture(persistence)
		return persistence
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
}

func selectedRuntimeStoreForTest(t testing.TB, persistence serveRuntimePersistence) (*sql.DB, *store.PostgresStore, *store.SQLiteRuntimeStore) {
	t.Helper()
	switch selected := persistence.deps.EventStore.(type) {
	case *store.PostgresStore:
		return storetest.DatabaseForTest(selected), selected, nil
	case *store.SQLiteRuntimeStore:
		return storetest.DatabaseForTest(selected), nil, selected
	default:
		t.Fatalf("unsupported selected runtime test store %T", persistence.deps.EventStore)
		return nil, nil, nil
	}
}

func runtimeSessionLockTTL(cfg *config.Config) time.Duration {
	if cfg == nil {
		return 0
	}
	return cfg.LLM.Session.LockTTL
}

func closeUnactivatedSelectedStore(t testing.TB, owner *selectedStoreOwner) {
	t.Helper()
	if owner == nil {
		return
	}
	if err := owner.CloseUnactivated(); err != nil {
		t.Errorf("close unactivated selected store: %v", err)
	}
}
