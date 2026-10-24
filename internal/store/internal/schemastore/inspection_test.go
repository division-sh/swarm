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
			}}
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
			if err != nil || !result.Fresh || !reflect.DeepEqual(result.MissingDeploymentTables, []string{"deployment_channel_operations"}) {
				t.Fatalf("fresh preparation decision = %+v, %v", result, err)
			}
			after, err := listTables(ctx, db)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("fresh inspection changed tables: before=%v after=%v err=%v", before, after, err)
			}
			platformOnly := request

			platformOnly.PlatformPlans = nil
			for _, plan := range plans {
				if plan.TableName != "deployment_channel_operations" {
					platformOnly.PlatformPlans = append(platformOnly.PlatformPlans, plan)
				}
			}
			if err := bootstrap(ctx, platformOnly); err != nil {
				t.Fatal(err)
			}
			result, err = inspect(ctx, request)
			if err != nil || result.Fresh || result.Origin == nil || !reflect.DeepEqual(result.MissingDeploymentTables, []string{"deployment_channel_operations"}) {
				t.Fatalf("compatible platform with missing generated table = %+v, %v", result, err)
			}
			originalOrigin := *result.Origin
			if tables, err := listTables(ctx, db); err != nil {
				t.Fatal(err)
			} else if _, exists := tables["deployment_channel_operations"]; exists {
				t.Fatal("inspection created the deployment registration table")
			}
			if err := bootstrap(ctx, request); err != nil {
				t.Fatal(err)
			}
			result, err = inspect(ctx, request)
			if err != nil || result.Fresh || len(result.MissingDeploymentTables) != 0 {
				t.Fatalf("prepared schema = %+v, %v", result, err)
			}
			if result.Origin == nil || *result.Origin != originalOrigin {
				t.Fatal("deployment-table preparation replaced the existing store origin")
			}
			if _, err := db.ExecContext(ctx, `ALTER TABLE deployment_channel_operations ADD COLUMN unexpected TEXT`); err != nil {
				t.Fatal(err)
			}
			var registrationDrift *SchemaCompatibilityError
			if err := bootstrap(ctx, request); !errors.As(err, &registrationDrift) {
				t.Fatalf("existing registration-table drift was not refused: %v", err)
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
