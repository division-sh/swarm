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
	first, err := NewSQLiteRuntimeStore(testutil.SQLiteDeclaredPath(t, testutil.SQLiteFreshFile(), path))
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
	store, err := NewSQLiteRuntimeStore(testutil.SQLiteDeclaredPath(t, testutil.SQLiteFreshFile(), path))
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
		stores[i], err = NewSQLiteRuntimeStore(testutil.SQLiteDeclaredPath(t, testutil.SQLiteFreshFile(), path))
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
	_, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
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

func TestSchemaBootstrapRejectsUnexpectedIndex(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	const createUnexpectedIndex = `CREATE UNIQUE INDEX drift_probe_idx ON timers(status) WHERE status = 'pending'`

	t.Run("sqlite", func(t *testing.T) {
		store, err := NewSQLiteRuntimeStore(testutil.SQLiteDeclaredPath(t, testutil.SQLiteFreshFile(), filepath.Join(t.TempDir(), "unexpected-index.db")))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := store.BootstrapSchema(context.Background(), request); err != nil {
			t.Fatalf("fresh bootstrap: %v", err)
		}
		if _, err := store.DB.Exec(createUnexpectedIndex); err != nil {
			t.Fatal(err)
		}
		assertUnexpectedIndexRejected(t, store.BootstrapSchema(context.Background(), request))
	})

	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
		t.Cleanup(cleanup)
		store := &PostgresStore{DB: db}
		if _, err := db.Exec(createUnexpectedIndex); err != nil {
			t.Fatal(err)
		}
		assertUnexpectedIndexRejected(t, store.BootstrapSchema(context.Background(), request))
	})
}

func assertUnexpectedIndexRejected(t *testing.T, err error) {
	t.Helper()
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("unexpected-index bootstrap error = %v, want SchemaCompatibilityError", err)
	}
	if !strings.Contains(err.Error(), "unexpected index drift_probe_idx") {
		t.Fatalf("unexpected-index bootstrap error = %v, want named index drift", err)
	}
}

func TestSQLiteSchemaBootstrapRejectsMalformedStoredOrigin(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	path := filepath.Join(t.TempDir(), "malformed-origin.db")
	store, err := NewSQLiteRuntimeStore(testutil.SQLiteDeclaredPath(t, testutil.SQLiteFreshFile(), path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatalf("fresh bootstrap: %v", err)
	}
	cases := []malformedStoredOriginCase{
		{name: "blank swarm version", update: `UPDATE runtime_store_metadata SET swarm_version = ' ' WHERE id = 1`, wantDrift: "stored Swarm version is required"},
		{name: "blank platform version", update: `UPDATE runtime_store_metadata SET platform_version = ' ' WHERE id = 1`, wantDrift: "stored platform version is required"},
		{name: "zero creation time", update: `UPDATE runtime_store_metadata SET created_at = '0001-01-01T00:00:00Z' WHERE id = 1`, wantDrift: "stored creation time must be non-zero"},
		{name: "invalid creation time", update: `UPDATE runtime_store_metadata SET created_at = 'not-a-time' WHERE id = 1`, wantDrift: "parse runtime store creation time"},
	}
	assertMalformedStoredOrigins(t, cases, func(statement string) error {
		_, err := store.DB.Exec(statement)
		return err
	}, func() error {
		_, err := store.DB.Exec(`UPDATE runtime_store_metadata SET swarm_version = ?, platform_version = ?, created_at = ? WHERE id = 1`, request.Origin.SwarmVersion, request.Origin.PlatformVersion, request.Origin.CreatedAt.UTC().Format(time.RFC3339Nano))
		return err
	}, func() error {
		return store.BootstrapSchema(context.Background(), request)
	}, SchemaDialectSQLite, path, request.Origin)
}

func TestPostgresSchemaBootstrapRejectsMalformedStoredOrigin(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	_, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	var target string
	if err := db.QueryRow(`SELECT current_database()`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	cases := []malformedStoredOriginCase{
		{name: "blank swarm version", update: `UPDATE runtime_store_metadata SET swarm_version = ' ' WHERE id = 1`, wantDrift: "stored Swarm version is required"},
		{name: "blank platform version", update: `UPDATE runtime_store_metadata SET platform_version = ' ' WHERE id = 1`, wantDrift: "stored platform version is required"},
		{name: "zero creation time", update: `UPDATE runtime_store_metadata SET created_at = TIMESTAMPTZ '0001-01-01 00:00:00+00' WHERE id = 1`, wantDrift: "stored creation time must be non-zero"},
	}
	assertMalformedStoredOrigins(t, cases, func(statement string) error {
		_, err := db.Exec(statement)
		return err
	}, func() error {
		_, err := db.Exec(`UPDATE runtime_store_metadata SET swarm_version = $1, platform_version = $2, created_at = $3 WHERE id = 1`, request.Origin.SwarmVersion, request.Origin.PlatformVersion, request.Origin.CreatedAt)
		return err
	}, func() error {
		return store.BootstrapSchema(context.Background(), request)
	}, SchemaDialectPostgres, target, request.Origin)
}

type malformedStoredOriginCase struct {
	name      string
	update    string
	wantDrift string
}

func assertMalformedStoredOrigins(
	t *testing.T,
	cases []malformedStoredOriginCase,
	update func(string) error,
	restore func() error,
	bootstrap func() error,
	backend SchemaDialect,
	target string,
	current RuntimeStoreOrigin,
) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := update(tc.update); err != nil {
				t.Fatal(err)
			}
			bootstrapErr := bootstrap()
			if err := restore(); err != nil {
				t.Fatalf("restore canonical origin: %v", err)
			}
			assertSchemaCompatibilityDiagnostic(t, bootstrapErr, backend, target, current, nil, tc.wantDrift)
		})
	}
}

func TestPostgresSchemaBootstrapConcurrentFreshCreatorsConverge(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	dsn, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresEmptyPhysical())
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
	_, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresEmptyPhysical())
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
				store, err := NewSQLiteRuntimeStore(testutil.SQLiteDeclaredPath(t, testutil.SQLiteFreshFile(), filepath.Join(t.TempDir(), "rollback.db")))
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
			_, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresEmptyPhysical())
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
	store, err := NewSQLiteRuntimeStore(testutil.SQLiteDeclaredPath(t, testutil.SQLiteFreshFile(), filepath.Join(t.TempDir(), "state.db")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatalf("create generated state: %v", err)
	}
	storedOrigin, err := readRuntimeStoreOrigin(context.Background(), store.DB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec(`ALTER TABLE generated_probe_state ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	assertSchemaCompatibilityDiagnostic(t, store.BootstrapSchema(context.Background(), request), SchemaDialectSQLite, store.path, request.Origin, storedOrigin, "generated state table generated_probe_state:", "drift_probe")
}

func TestPostgresSchemaBootstrapCreatesThenValidatesGeneratedState(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	request.StatePlans = generatedProbeStatePlans()
	_, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	var target string
	if err := db.QueryRow(`SELECT current_database()`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatalf("create generated state: %v", err)
	}
	storedOrigin, err := readRuntimeStoreOrigin(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE generated_probe_state ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	assertSchemaCompatibilityDiagnostic(t, store.BootstrapSchema(context.Background(), request), SchemaDialectPostgres, target, request.Origin, storedOrigin, "generated state table generated_probe_state:", "drift_probe")
}

func assertSchemaCompatibilityDiagnostic(t *testing.T, err error, backend SchemaDialect, target string, current RuntimeStoreOrigin, wantOrigin *RuntimeStoreOrigin, wantDrift ...string) {
	t.Helper()
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("bootstrap error = %v (%T), want SchemaCompatibilityError", err, err)
	}
	if incompatible.Backend != backend || incompatible.Target != target {
		t.Fatalf("diagnostic backend/target = %s/%q, want %s/%q", incompatible.Backend, incompatible.Target, backend, target)
	}
	if incompatible.Current != current {
		t.Fatalf("diagnostic current origin = %#v, want %#v", incompatible.Current, current)
	}
	if wantOrigin != nil {
		if incompatible.Origin == nil || *incompatible.Origin != *wantOrigin {
			t.Fatalf("diagnostic stored origin = %#v, want %#v", incompatible.Origin, wantOrigin)
		}
	} else if incompatible.Origin != nil {
		t.Fatalf("malformed stored origin was exposed as valid evidence: %#v", incompatible.Origin)
	}
	drift := strings.Join(incompatible.Drift, "; ")
	for _, want := range wantDrift {
		if !strings.Contains(drift, want) {
			t.Fatalf("diagnostic drift = %v, want evidence %q", incompatible.Drift, want)
		}
	}
	wantRemediation := "create and select a fresh PostgreSQL database (for example: createdb swarm_fresh, then set database.name to swarm_fresh)"
	if backend == SchemaDialectSQLite {
		wantRemediation = "stop Swarm and remove the incompatible local store with: rm -f -- " + shellQuote(target) + " " + shellQuote(target+"-wal") + " " + shellQuote(target+"-shm")
	}
	if !strings.Contains(err.Error(), "remediation: "+wantRemediation) {
		t.Fatalf("diagnostic = %v, want remediation %q", err, wantRemediation)
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
