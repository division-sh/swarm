package storetest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/platform"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// StartSQLiteRuntimeStore creates a file-backed SQLite runtime store with the
// canonical platform schema for backend-neutral tests.
func StartSQLiteRuntimeStore(t testing.TB, requirement testutil.DatabaseRequirement) *store.SQLiteRuntimeStore {
	t.Helper()
	return StartSQLiteRuntimeStoreWithContext(t, context.Background(), requirement)
}

func StartSQLiteRuntimeStoreWithContext(t testing.TB, ctx context.Context, requirement testutil.DatabaseRequirement) *store.SQLiteRuntimeStore {
	t.Helper()

	var platformSpec runtimecontracts.PlatformSpecDocument
	source, err := yamlsource.Load(platform.PlatformSpecYAML())
	if err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	if err := source.Decode(&platformSpec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	plans, err := store.GeneratePlatformTableDDLs(platformSpec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	dbPath := testutil.SQLitePath(t, requirement)
	sqliteStore, err := store.NewSQLiteRuntimeStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteRuntimeStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite runtime store: %v", err)
		}
	})
	if err := sqliteStore.BootstrapSchema(ctx, store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion:    "storetest",
			PlatformVersion: platformSpec.Platform.Version,
			CreatedAt:       time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", dbPath, err)
	}
	return sqliteStore
}
