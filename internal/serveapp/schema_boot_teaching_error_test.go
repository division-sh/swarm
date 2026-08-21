package serveapp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	storeconstruction "github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

// TestServeBootLegacySchemaRendersTeachingError proves the #995 teaching-error
// half at the boot surface: state-store initialization against an older
// schema (legacy timers table missing run_id) fails closed with the typed
// SchemaCompatibilityError text — both versions, drift evidence, fresh-store
// remediation — and never surfaces raw driver text.
func TestServeBootLegacySchemaRendersTeachingError(t *testing.T) {
	repo := cliapp.RepoRoot()
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection)
	loaded, err := loadServeRuntimeBundle(context.Background(), repo, storeBundle{}, cliapp.CLIContractPlatformSpecPaths{
		ContractsPath: root, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	}, cliapp.ServeOptions{}, packfixture.EmbeddedBase(t))
	if err != nil {
		t.Fatalf("loadServeRuntimeBundle: %v", err)
	}
	loaded.bundleSourceFact = mustServeTestEphemeralBundleSourceFact(loaded.bootIdentity.BundleHash)
	cfg, err := cliapp.DefaultRuntimeConfig()
	if err != nil {
		t.Fatalf("DefaultRuntimeConfig: %v", err)
	}
	request := serveRuntimeBundleContextRequest{
		Ctx:              context.Background(),
		Loaded:           loaded,
		Config:           cfg,
		WorkspaceBackend: cliapp.WorkspaceBackendSelection{Backend: cliapp.WorkspaceBackendNone, NoWorkspace: true, Source: "test"},
		BootStartedAt:    time.Now().UTC(),
	}

	t.Run("postgres", func(t *testing.T) {
		_, pg, cleanup := testutil.StartEmptyPostgres(t)
		t.Cleanup(cleanup)
		if _, err := pg.Exec(`CREATE TABLE timers (timer_id TEXT PRIMARY KEY, due_at TIMESTAMPTZ NOT NULL)`); err != nil {
			t.Fatalf("create legacy timers table: %v", err)
		}
		pgStore := storetest.NewPostgresStoreForTest(pg)
		request.Stores = storeBundle{Postgres: pgStore, SQLDB: pg, Database: pgStore, SchemaBootstrapper: pgStore}

		_, err := buildServeRuntimeBundleContext(request)
		assertLegacySchemaTeachingBootError(t, "postgres", err, "create and select a fresh PostgreSQL database")
	})

	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "legacy-timers.db")
		sqliteStore, sqliteDB, err := storeconstruction.OpenSQLiteRuntime(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sqliteStore.Close() })
		if _, err := sqliteDB.Exec(`CREATE TABLE timers (timer_id TEXT PRIMARY KEY, due_at TIMESTAMP NOT NULL)`); err != nil {
			t.Fatalf("create legacy timers table: %v", err)
		}
		request.Stores = storeBundle{SQLDB: sqliteDB, Database: sqliteStore, SchemaBootstrapper: sqliteStore}

		_, err = buildServeRuntimeBundleContext(request)
		assertLegacySchemaTeachingBootError(t, "sqlite", err, "rm -f --")
	})
}

func assertLegacySchemaTeachingBootError(t *testing.T, backend string, err error, remediation string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s boot against a legacy timers table succeeded; want fail-closed incompatible-schema error", backend)
	}
	for _, want := range []string{
		"incompatible with Swarm",
		"platform",
		"missing column timers.run_id",
		"stored origin",
		remediation,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%s boot error missing %q:\n%v", backend, want, err)
		}
	}
	for _, forbidden := range []string{"pq:", "sql:", "driver:", "Syntax error"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("%s boot error surfaced raw driver text %q:\n%v", backend, forbidden, err)
		}
	}
}
