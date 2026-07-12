package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/platform"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testutil"
	"gopkg.in/yaml.v3"
)

func TestSQLiteSchemaBootstrapFreshSecondBootAndDriftRejection(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	path := filepath.Join(t.TempDir(), "dev.db")
	first, err := NewSQLiteRuntimeStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	if err := first.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatalf("fresh bootstrap: %v", err)
	}
	var createdAt string
	if err := first.DB.QueryRow(`SELECT created_at FROM runtime_store_metadata WHERE id=1`).Scan(&createdAt); err != nil {
		t.Fatal(err)
	}
	request.Origin.SwarmVersion = "later-build"
	request.Origin.CreatedAt = request.Origin.CreatedAt.Add(time.Hour)
	if err := first.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatalf("compatible second boot: %v", err)
	}
	var after string
	if err := first.DB.QueryRow(`SELECT created_at FROM runtime_store_metadata WHERE id=1`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != createdAt {
		t.Fatalf("origin stamp changed on second boot: %q -> %q", createdAt, after)
	}
	if _, err := first.DB.Exec(`ALTER TABLE timers ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	err = first.BootstrapSchema(context.Background(), request)
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("drift bootstrap error = %v, want SchemaCompatibilityError", err)
	}
}

func TestSQLiteSchemaBootstrapRejectsUnstampedWithoutWALMutation(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	path := filepath.Join(t.TempDir(), "legacy.db")
	store, err := NewSQLiteRuntimeStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.DB.Exec(`CREATE TABLE legacy_state (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapSchema(context.Background(), request); err == nil {
		t.Fatal("unstamped non-empty store was accepted")
	}
	var mode string
	if err := store.DB.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode == "wal" {
		t.Fatal("incompatible preflight changed journal mode to WAL")
	}
	for _, sidecar := range []string{path + "-wal", path + "-shm"} {
		if _, err := os.Stat(sidecar); !os.IsNotExist(err) {
			t.Fatalf("incompatible preflight created sidecar %s", sidecar)
		}
	}
}

func TestSQLiteSchemaBootstrapConcurrentCreatorsConverge(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	path := filepath.Join(t.TempDir(), "concurrent.db")
	stores := make([]*SQLiteRuntimeStore, 2)
	for i := range stores {
		var err error
		stores[i], err = NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatal(err)
		}
		current := stores[i]
		t.Cleanup(func() { _ = current.Close() })
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = stores[i].BootstrapSchema(context.Background(), request)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent bootstrap: %v", err)
		}
	}
	var count int
	if err := stores[0].DB.QueryRow(`SELECT COUNT(*) FROM runtime_store_metadata`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("origin rows = %d, err=%v", count, err)
	}
}

func TestPostgresSchemaBootstrapAcceptsCanonicalTemplateAndRejectsDrift(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	if err := store.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatalf("compatible template boot: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE timers ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	err := store.BootstrapSchema(context.Background(), request)
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("drift bootstrap error = %v, want SchemaCompatibilityError", err)
	}
}

func TestPostgresSchemaBootstrapConcurrentFreshCreatorsConverge(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	dsn, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	second, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.DB.Close() })
	stores := []*PostgresStore{{DB: db}, second}
	errs := make([]error, len(stores))
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = stores[i].BootstrapSchema(context.Background(), request)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent postgres bootstrap: %v", err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runtime_store_metadata`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("origin rows = %d, err=%v", count, err)
	}
}

func TestPostgresSchemaBootstrapRejectsBeforeExtensionOrSchemaMutation(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	_, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE legacy_state (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	store := &PostgresStore{DB: db}
	if err := store.BootstrapSchema(context.Background(), request); err == nil {
		t.Fatal("unstamped non-empty PostgreSQL store was accepted")
	}
	var extensionExists bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto')`).Scan(&extensionExists); err != nil {
		t.Fatal(err)
	}
	if extensionExists {
		t.Fatal("incompatible preflight created pgcrypto")
	}
	var columns int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='legacy_state'`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if columns != 1 {
		t.Fatalf("incompatible preflight mutated legacy_state columns: %d", columns)
	}
}

func TestSchemaBootstrapRollsBackFailedFreshCreation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			request := canonicalSchemaBootstrapTestRequest(t)
			for i := range request.PlatformPlans {
				if request.PlatformPlans[i].TableName != RuntimeStoreMetadataTable {
					continue
				}
				statements := append([]string(nil), request.PlatformPlans[i].Statements...)
				statements[0] = strings.TrimSuffix(strings.TrimSpace(statements[0]), ")") + ", CHECK (swarm_version = 'allowed'))"
				request.PlatformPlans[i].Statements = statements
				break
			}
			if backend == "sqlite" {
				store, err := NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "rollback.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				if err := store.BootstrapSchema(context.Background(), request); err == nil {
					t.Fatal("invalid fresh bootstrap succeeded")
				}
				tables, err := sqliteUserTables(context.Background(), store.DB)
				if err != nil || len(tables) != 0 {
					t.Fatalf("failed SQLite bootstrap left objects %v, err=%v", tables, err)
				}
				return
			}
			_, db, cleanup := testutil.StartEmptyPostgres(t)
			t.Cleanup(cleanup)
			store := &PostgresStore{DB: db}
			if err := store.BootstrapSchema(context.Background(), request); err == nil {
				t.Fatal("invalid fresh bootstrap succeeded")
			}
			tables, err := postgresPublicTables(context.Background(), db)
			if err != nil || len(tables) != 0 {
				t.Fatalf("failed PostgreSQL bootstrap left objects %v, err=%v", tables, err)
			}
		})
	}
}

func TestSQLiteSchemaBootstrapCreatesThenValidatesGeneratedState(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	request.StatePlans = generatedProbeStatePlans()
	store, err := NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatalf("create generated state: %v", err)
	}
	if _, err := store.DB.Exec(`ALTER TABLE generated_probe_state ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapSchema(context.Background(), request); err == nil {
		t.Fatal("incompatible generated state table was accepted")
	}
}

func TestPostgresSchemaBootstrapCreatesThenValidatesGeneratedState(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	request.StatePlans = generatedProbeStatePlans()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	if err := store.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatalf("create generated state: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE generated_probe_state ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapSchema(context.Background(), request); err == nil {
		t.Fatal("incompatible generated state table was accepted")
	}
}

func generatedProbeStatePlans() []SchemaTableDDL {
	return []SchemaTableDDL{{
		TableName:  "generated_probe_state",
		SchemaKind: "state_schema",
		Statements: []string{`CREATE TABLE IF NOT EXISTS generated_probe_state (
    "entity_id" UUID NOT NULL,
    "node_id" TEXT NOT NULL,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "value" TEXT,
    PRIMARY KEY ("entity_id", "node_id")
)`},
	}}
}

func canonicalSchemaBootstrapTestRequest(t testing.TB) SchemaBootstrapRequest {
	t.Helper()
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(platform.PlatformSpecYAML(), &spec); err != nil {
		t.Fatal(err)
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	return SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin:        RuntimeStoreOrigin{SwarmVersion: "schema-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}
}
