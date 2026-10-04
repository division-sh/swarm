package schemastore

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestSchemaInspectionSharesBootDecisionWithoutPreparation(t *testing.T) {
	for _, dialect := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		t.Run(string(dialect), func(t *testing.T) {
			ctx := context.Background()
			spec := loadPlatformSpecForSQLiteSchemaTest(t)
			plans, err := GeneratePlatformTableDDLs(spec)
			if err != nil {
				t.Fatal(err)
			}
			request := SchemaBootstrapRequest{PlatformPlans: plans, Origin: RuntimeStoreOrigin{
				SwarmVersion: "inspection-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
			}, StatePlans: []SchemaTableDDL{{TableName: "inspection_state", SchemaKind: "state_schema", Statements: []string{
				`CREATE TABLE IF NOT EXISTS inspection_state (id TEXT PRIMARY KEY, value BIGINT NOT NULL)`,
			}}}}
			var db *sql.DB
			var inspect func(context.Context, SchemaBootstrapRequest) (SchemaInspection, error)
			var bootstrap func(context.Context, SchemaBootstrapRequest) error
			var listTables func(context.Context, schemaQueryer) (map[string]struct{}, error)
			if dialect == SchemaDialectSQLite {
				s, err := NewSQLite(filepath.Join(t.TempDir(), "store.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				db, inspect, bootstrap, listTables = s.backend.ConstructionHandle(), s.InspectSchema, s.BootstrapSchema, sqliteUserTables
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
				inspect, bootstrap, listTables = s.InspectSchema, s.BootstrapSchema, postgresPublicTables
			}
			before, err := listTables(ctx, db)
			if err != nil {
				t.Fatal(err)
			}
			result, err := inspect(ctx, request)
			if err != nil || !result.Fresh || !reflect.DeepEqual(result.MissingStateTables, []string{"inspection_state"}) {
				t.Fatalf("fresh preparation decision = %+v, %v", result, err)
			}
			after, err := listTables(ctx, db)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("fresh inspection changed tables: before=%v after=%v err=%v", before, after, err)
			}
			platformOnly := request
			platformOnly.StatePlans = nil
			if err := bootstrap(ctx, platformOnly); err != nil {
				t.Fatal(err)
			}
			result, err = inspect(ctx, request)
			if err != nil || result.Fresh || result.Origin == nil || len(result.MissingStateTables) != 1 {
				t.Fatalf("compatible platform with missing generated table = %+v, %v", result, err)
			}
			if tables, err := listTables(ctx, db); err != nil {
				t.Fatal(err)
			} else if _, exists := tables["inspection_state"]; exists {
				t.Fatal("inspection created a generated state table")
			}
			if err := bootstrap(ctx, request); err != nil {
				t.Fatal(err)
			}
			result, err = inspect(ctx, request)
			if err != nil || result.Fresh || len(result.MissingStateTables) != 0 {
				t.Fatalf("prepared schema = %+v, %v", result, err)
			}
			if _, err := db.ExecContext(ctx, `ALTER TABLE inspection_state ADD COLUMN unexpected TEXT`); err != nil {
				t.Fatal(err)
			}
			_, inspectionErr := inspect(ctx, request)
			bootErr := bootstrap(ctx, request)
			var inspectionDrift, bootDrift *SchemaCompatibilityError
			if !errors.As(inspectionErr, &inspectionDrift) || !errors.As(bootErr, &bootDrift) || !reflect.DeepEqual(inspectionDrift.Drift, bootDrift.Drift) {
				t.Fatalf("inspection/boot drift differs: inspection=%v boot=%v", inspectionErr, bootErr)
			}
		})
	}
}
