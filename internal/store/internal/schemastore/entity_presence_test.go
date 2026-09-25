package schemastore

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestSQLiteEntityPresenceAdmission(t *testing.T) {
	for _, mode := range []string{"current", "missing_column", "wrong_value", "missing_row", "missing_check"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			spec := loadPlatformSpecForSQLiteSchemaTest(t)
			plans, err := GeneratePlatformTableDDLs(spec)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "store.db")
			s, err := NewSQLite(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			request := SchemaBootstrapRequest{PlatformPlans: plans, Origin: RuntimeStoreOrigin{
				SwarmVersion: "presence-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
			}}
			if err := s.BootstrapSchema(ctx, request); err != nil {
				t.Fatal(err)
			}
			db := s.backend.ConstructionHandle()
			var model string
			if err := db.QueryRowContext(ctx, "SELECT entity_presence_model FROM runtime_store_metadata WHERE id=1").Scan(&model); err != nil {
				t.Fatal(err)
			}
			if model != platformschema.EntityPresenceModel {
				t.Fatalf("model = %q", model)
			}
			var statements []string
			switch mode {
			case "missing_column":
				statements = []string{"ALTER TABLE runtime_store_metadata DROP COLUMN entity_presence_model"}
			case "wrong_value":
				statements = []string{"PRAGMA ignore_check_constraints=ON", "UPDATE runtime_store_metadata SET entity_presence_model='legacy'", "PRAGMA ignore_check_constraints=OFF"}
			case "missing_row":
				statements = []string{"DELETE FROM runtime_store_metadata"}
			case "missing_check":
				statements = []string{
					"ALTER TABLE runtime_store_metadata RENAME TO old_metadata",
					"CREATE TABLE runtime_store_metadata (id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id=1), swarm_version TEXT NOT NULL, platform_version TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, entity_presence_model TEXT NOT NULL)",
					"INSERT INTO runtime_store_metadata SELECT * FROM old_metadata", "DROP TABLE old_metadata",
				}
			}
			for _, statement := range statements {
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = NewSQLite(path)
			if err != nil {
				t.Fatal(err)
			}
			err = s.BootstrapSchema(ctx, request)
			if mode == "current" {
				if err != nil {
					t.Fatalf("restart current store: %v", err)
				}
			} else if err == nil {
				t.Fatal("incompatible entity presence store admitted")
			} else if !strings.Contains(err.Error(), "runtime_store_metadata") {
				t.Fatalf("wrong admission error: %v", err)
			}
		})
	}
}

func TestPostgresEntityPresenceAdmission(t *testing.T) {
	for _, mode := range []string{"current", "missing_column", "wrong_value", "missing_row", "missing_check"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			_, db, cleanup := testutil.StartEmptyPostgres(t)
			t.Cleanup(cleanup)
			backend, err := postgresbackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			s, err := NewPostgres(backend)
			if err != nil {
				t.Fatal(err)
			}
			spec := loadPlatformSpecForSQLiteSchemaTest(t)
			plans, err := GeneratePlatformTableDDLs(spec)
			if err != nil {
				t.Fatal(err)
			}
			request := SchemaBootstrapRequest{PlatformPlans: plans, Origin: RuntimeStoreOrigin{
				SwarmVersion: "presence-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
			}}
			if err := s.BootstrapSchema(ctx, request); err != nil {
				t.Fatal(err)
			}
			var model string
			if err := db.QueryRowContext(ctx, "SELECT entity_presence_model FROM runtime_store_metadata WHERE id=1").Scan(&model); err != nil {
				t.Fatal(err)
			}
			if model != platformschema.EntityPresenceModel {
				t.Fatalf("model = %q", model)
			}
			var statements []string
			switch mode {
			case "missing_column":
				statements = []string{"ALTER TABLE runtime_store_metadata DROP COLUMN entity_presence_model"}
			case "wrong_value":
				statements = []string{
					"ALTER TABLE runtime_store_metadata DROP CONSTRAINT runtime_store_metadata_entity_presence_model_check",
					"UPDATE runtime_store_metadata SET entity_presence_model='legacy'",
					"ALTER TABLE runtime_store_metadata ADD CONSTRAINT runtime_store_metadata_entity_presence_model_check CHECK (entity_presence_model='sparse-v1') NOT VALID",
				}
			case "missing_row":
				statements = []string{"DELETE FROM runtime_store_metadata"}
			case "missing_check":
				statements = []string{"ALTER TABLE runtime_store_metadata DROP CONSTRAINT runtime_store_metadata_entity_presence_model_check"}
			}
			for _, statement := range statements {
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			// A new admission owner must inspect the stored facts, not reuse the old admission.
			s, err = NewPostgres(backend)
			if err != nil {
				t.Fatal(err)
			}
			err = s.BootstrapSchema(ctx, request)
			if mode == "current" {
				if err != nil {
					t.Fatalf("reopen current store: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "runtime_store_metadata") {
				t.Fatalf("incompatible store admission = %v", err)
			}
		})
	}
}

func TestSQLiteEntityPresenceMarkerHasNoDefault(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLite(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	plans, err := GeneratePlatformTableDDLs(loadPlatformSpecForSQLiteSchemaTest(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BootstrapSchema(ctx, SchemaBootstrapRequest{PlatformPlans: plans, Origin: RuntimeStoreOrigin{
		SwarmVersion: "presence-test", PlatformVersion: "test", CreatedAt: time.Now().UTC(),
	}}); err != nil {
		t.Fatal(err)
	}
	db := s.backend.ConstructionHandle()
	if _, err := db.ExecContext(ctx, "DELETE FROM runtime_store_metadata"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO runtime_store_metadata (id, swarm_version, platform_version, created_at) VALUES (1, 'test', 'test', CURRENT_TIMESTAMP)"); err == nil {
		t.Fatal("origin insert without explicit presence marker succeeded")
	}
}
