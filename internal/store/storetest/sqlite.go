package storetest

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/platform"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// StartSQLiteRuntimeStore creates a file-backed SQLite runtime store with the
// canonical platform schema for backend-neutral tests.
func StartSQLiteRuntimeStore(t testing.TB) *store.SQLiteRuntimeStore {
	t.Helper()
	return StartSQLiteRuntimeStoreWithContext(t, context.Background())
}

// StartSQLiteRuntimeStorePair returns independently constructed store handles
// over one canonical file-backed database. It is used to prove behavior that
// must survive process-local store reconstruction.
func StartSQLiteRuntimeStorePair(t testing.TB) (*store.SQLiteRuntimeStore, *store.SQLiteRuntimeStore) {
	t.Helper()

	platformSpec, plans := canonicalPlatformPlans(t)
	dbPath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	open := func() *store.SQLiteRuntimeStore {
		sqliteStore, err := store.NewSQLiteRuntimeStore(dbPath)
		if err != nil {
			t.Fatalf("NewSQLiteRuntimeStore: %v", err)
		}
		if err := sqliteStore.BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
			PlatformPlans: plans,
			Origin: store.RuntimeStoreOrigin{
				SwarmVersion:    "storetest",
				PlatformVersion: platformSpec.Platform.Version,
				CreatedAt:       time.Now().UTC(),
			},
		}); err != nil {
			_ = sqliteStore.Close()
			t.Fatalf("BootstrapSchema: %v", err)
		}
		t.Cleanup(func() {
			if err := sqliteStore.Close(); err != nil {
				t.Errorf("close sqlite runtime store: %v", err)
			}
		})
		return sqliteStore
	}
	primary := open()
	reconstructed := open()
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", dbPath, err)
	}
	return primary, reconstructed
}

func StartSQLiteRuntimeStoreWithContext(t testing.TB, ctx context.Context) *store.SQLiteRuntimeStore {
	t.Helper()

	platformSpec, plans := canonicalPlatformPlans(t)
	dbPath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
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

// AdmitPostgresRuntimeStore runs the production bootstrap against a canonical
// PostgreSQL test database and returns the admitted store object.
func AdmitPostgresRuntimeStore(t testing.TB, db *sql.DB) *store.PostgresStore {
	t.Helper()
	postgresStore := &store.PostgresStore{DB: db}
	BootstrapPostgresRuntimeStore(t, postgresStore)
	return postgresStore
}

// BootstrapPostgresRuntimeStore admits an existing PostgreSQL store through
// the same production bootstrap used at serve startup.
func BootstrapPostgresRuntimeStore(t testing.TB, postgresStore *store.PostgresStore) {
	t.Helper()
	platformSpec, plans := canonicalPlatformPlans(t)
	if err := postgresStore.BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion:    "storetest",
			PlatformVersion: platformSpec.Platform.Version,
			CreatedAt:       time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
}

func canonicalPlatformPlans(t testing.TB) (runtimecontracts.PlatformSpecDocument, []store.SchemaTableDDL) {
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
	return platformSpec, plans
}
