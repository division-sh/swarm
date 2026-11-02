package serveapp

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
)

func seedStandingRuntimeContextBundle(t *testing.T, ctx context.Context, writer sourceArtifactDataWriter, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	catalog, err := runtimecontracts.BuildDurableDataCatalog(bundle)
	if err != nil {
		t.Fatalf("project standing runtime-context source data: %v", err)
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.EnsureRootedSourceWithData(ctx, rooted, catalog); err != nil {
		t.Fatalf("persist standing runtime-context bundle: %v", err)
	}
}

func openStandingRuntimeContextStore(t *testing.T, backend, suffix string) *selectedStoreOwner {
	t.Helper()
	switch backend {
	case "sqlite":
		path := filepath.Join(t.TempDir(), suffix+".sqlite")
		stores := openSelectedSQLiteOwner(t, path, &config.Config{})
		t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
		spec, err := loadServePlatformSpecDocument()
		if err != nil {
			t.Fatalf("load platform spec for %s SQLite store: %v", suffix, err)
		}
		plans, err := store.GeneratePlatformTableDDLs(spec)
		if err != nil {
			t.Fatalf("generate platform schema for %s SQLite store: %v", suffix, err)
		}
		request, err := schemaBootstrapRequest(spec, plans, nil)
		if err != nil {
			t.Fatalf("build schema request for %s SQLite store: %v", suffix, err)
		}
		if err := ensureServeSchemaTables(context.Background(), stores.Schema(), request); err != nil {
			t.Fatalf("bootstrap %s SQLite store: %v", suffix, err)
		}
		return stores
	case "postgres":
		dsn, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		stores := openSelectedPostgresOwner(t, dsn, db, &config.Config{})
		t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
		return stores
	default:
		t.Fatalf("unsupported standing runtime-context backend %q", backend)
		return nil
	}
}
