package schemastore

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestNodeStateCanonicalListsBootstrapBothStores(t *testing.T) {
	for _, dialect := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		t.Run(string(dialect), func(t *testing.T) {
			spec := loadPlatformSpecForSQLiteSchemaTest(t)
			platform, err := GeneratePlatformTableDDLs(spec)
			if err != nil {
				t.Fatal(err)
			}
			state, err := GenerateNodeStateTableDDLs([]contracts.ScopedNodeRecord{{
				LogicalID: "projection",
				Source:    contracts.ContractItemSource{FlowPath: ".", Family: "node", File: "nodes.yaml"},
				Entry: contracts.SystemNodeContract{StateTable: "canonical_list_state", StateSchema: contracts.NodeStateSchema{
					Fields: []contracts.NodeStateField{
						{Name: "labels", Type: "[text]"}, {Name: "amounts", Type: "[numeric(5,2)]"},
						{Name: "items", Type: "[DimensionScore]"}, {Name: "amount", Type: "numeric(5,2)"},
						{Name: "snapshot", Type: "jsonb"},
					},
				}},
			}})
			if err != nil {
				t.Fatal(err)
			}
			for _, column := range []string{`"labels" TEXT[]`, `"amounts" NUMERIC(5,2)[]`, `"items" JSONB`, `"amount" NUMERIC(5,2)`, `"snapshot" JSONB`} {
				if len(state) != 1 || !strings.Contains(state[0].Statements[0], column) {
					t.Fatalf("generated physical column %s missing: %+v", column, state)
				}
			}
			var bootstrap func(context.Context, SchemaBootstrapRequest) error
			var inspect func(context.Context, SchemaBootstrapRequest) (SchemaInspection, error)
			if dialect == SchemaDialectSQLite {
				s, err := NewSQLite(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				bootstrap, inspect = s.BootstrapSchema, s.InspectSchema
			} else {
				_, db, _ := testutil.StartEmptyPostgres(t)
				backend, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				s, err := NewPostgres(backend)
				if err != nil {
					t.Fatal(err)
				}
				bootstrap, inspect = s.BootstrapSchema, s.InspectSchema
			}
			ctx := context.Background()
			request := SchemaBootstrapRequest{PlatformPlans: platform, StatePlans: state, Origin: RuntimeStoreOrigin{
				SwarmVersion: "canonical-node-list-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
			}}
			if err := bootstrap(ctx, request); err != nil {
				t.Fatal(err)
			}
			result, err := inspect(ctx, request)
			if err != nil || result.Fresh || len(result.MissingStateTables) != 0 {
				t.Fatalf("generated canonical-list table not present and compatible: %+v, %v", result, err)
			}
			wrong := request
			wrong.StatePlans = append([]SchemaTableDDL(nil), state...)
			wrong.StatePlans[0].Statements = []string{strings.Replace(state[0].Statements[0], `"labels" TEXT[]`, `"labels" BIGINT`, 1)}
			_, err = inspect(ctx, wrong)
			var drift *SchemaCompatibilityError
			if !errors.As(err, &drift) {
				t.Fatalf("physical-column mismatch was not refused: %v", err)
			}
		})
	}
}
