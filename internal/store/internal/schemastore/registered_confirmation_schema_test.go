package schemastore

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/tests/support/testutil"
)

func TestRegisteredAcknowledgementSchemaRefusesUpgradeAndDowngrade(t *testing.T) {
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	old := append([]SchemaTableDDL(nil), plans...)
	changed := false
	for i, plan := range old {
		if plan.TableName != "runtime_external_effect_operations" {
			continue
		}
		old[i].Statements = append([]string(nil), plan.Statements...)
		for j, statement := range plan.Statements {
			lines := strings.Split(statement, "\n")
			var restored []string
			for _, line := range lines {
				if strings.Contains(line, "authority_evidence -> 'transport_parent'") {
					changed = true // Exact predecessor had no deployment-parent CHECK.
					continue
				}
				if strings.TrimSpace(line) == "bundle_hash        TEXT," {
					line = strings.Replace(line, "TEXT,", "TEXT NOT NULL,", 1)
				}
				restored = append(restored, line)
			}
			old[i].Statements[j] = strings.Join(restored, "\n")
		}
	}
	if !changed {
		t.Fatal("current closed deployment-parent schema was not found")
	}
	for _, dialect := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		for _, direction := range []string{"upgrade", "downgrade"} {
			t.Run(string(dialect)+"/"+direction, func(t *testing.T) {
				ctx := context.Background()
				var bootstrap func(context.Context, SchemaBootstrapRequest) error
				var db *sql.DB
				if dialect == SchemaDialectSQLite {
					s, err := NewSQLite(filepath.Join(t.TempDir(), "fresh.db"))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = s.Close() })
					db, bootstrap = s.backend.ConstructionHandle(), s.BootstrapSchema
				} else {
					_, db, _ = testutil.StartEmptyPostgres(t)
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					s, err := NewPostgres(backend)
					if err != nil {
						t.Fatal(err)
					}
					bootstrap = s.BootstrapSchema
				}
				first, second := old, plans
				if direction == "downgrade" {
					first, second = second, first
				}
				request := SchemaBootstrapRequest{PlatformPlans: first, Origin: RuntimeStoreOrigin{
					SwarmVersion: "ack-schema-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()}}
				if err := bootstrap(ctx, request); err != nil {
					t.Fatal("create exact source schema", err)
				}
				request.PlatformPlans = second
				var incompatible *SchemaCompatibilityError
				if err := bootstrap(ctx, request); !errors.As(err, &incompatible) {
					t.Fatal("incompatible schema did not refuse", err)
				}
				// A refusal is not an implicit migration, including on an empty journal.
				request.PlatformPlans = first
				if err := bootstrap(ctx, request); err != nil {
					t.Fatal("refusal changed the original schema", err)
				}
				var count int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_operations`).Scan(&count); err != nil || count != 0 {
					t.Fatal("compatibility inspection created effect evidence", count, err)
				}
			})
		}
	}
}
