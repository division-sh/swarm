package schemastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
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
						{Name: "integers", Type: "[integer]"}, {Name: "floats", Type: "[float]"},
						{Name: "numerics", Type: "[numeric]"}, {Name: "flags", Type: "[boolean]"},
						{Name: "identifiers", Type: "[uuid]"}, {Name: "times", Type: "[timestamp]"},
						{Name: "documents", Type: "[jsonb]"},
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
			var native *sql.DB
			if dialect == SchemaDialectSQLite {
				s, err := NewSQLite(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				bootstrap, inspect = s.BootstrapSchema, s.InspectSchema
				native = s.backend.ConstructionHandle()
			} else {
				_, db, _ := testutil.StartEmptyPostgres(t)
				native = db
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
			assertCanonicalNodeListColumnRoundTrips(t, ctx, native, dialect, false)
			assertCanonicalNodeListColumnRoundTrips(t, ctx, native, dialect, true)
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

func assertCanonicalNodeListColumnRoundTrips(t *testing.T, ctx context.Context, native *sql.DB, dialect SchemaDialect, empty bool) {
	t.Helper()
	stamp := "2026-10-13T00:00:00Z"
	identifier := "d9428888-122b-11e1-b85c-61cd3cbb3210"
	want := []any{
		[]any{"first", `quotes"\,{} unicode \u00e9`, ""},
		[]any{12.5, -0.25, 0.0}, []any{0.0, -2.0, 7.0},
		[]any{0.5, -2.25}, []any{12.5, -0.25}, []any{true, false},
		[]any{identifier}, []any{stamp},
		[]any{map[string]any{"name": "nested"}, []any{1.0, false}, "text"},
		[]any{map[string]any{"name": "named", "score": 7.25}},
	}
	node := "projection"
	if empty {
		node = "empty"
		for index := range want {
			want[index] = []any{}
		}
	}
	args := []any{identifier, node}
	for _, value := range want {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		args = append(args, string(raw))
	}
	if dialect == SchemaDialectPostgres {
		args[2] = pq.Array([]string{"first", `quotes"\,{} unicode \u00e9`, ""})
		args[3] = pq.Array([]float64{12.5, -0.25, 0})
		args[4] = pq.Array([]int64{0, -2, 7})
		args[5] = pq.Array([]float64{0.5, -2.25})
		args[6] = pq.Array([]float64{12.5, -0.25})
		args[7] = pq.Array([]bool{true, false})
		args[8] = pq.Array([]string{identifier})
		args[9] = pq.Array([]string{stamp})
		args[10] = pq.Array([]string{`{"name":"nested"}`, `[1,false]`, `"text"`})
		if empty {
			for index := 2; index < len(args)-1; index++ {
				args[index] = pq.Array([]string{})
			}
		}
	}
	if _, err := native.ExecContext(ctx, `INSERT INTO canonical_list_state
		(entity_id,node_id,labels,amounts,integers,floats,numerics,flags,identifiers,times,documents,items)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, args...); err != nil {
		t.Fatalf("write actual generated list columns: %v", err)
	}
	query := `SELECT labels,amounts,integers,floats,numerics,flags,identifiers,times,documents,items
		FROM canonical_list_state WHERE entity_id=$1 AND node_id=$2`
	if dialect == SchemaDialectPostgres {
		query = `SELECT to_json(labels),to_json(amounts),to_json(integers),to_json(floats),
			to_json(numerics),to_json(flags),to_json(identifiers),to_json(times),to_json(documents),items
			FROM canonical_list_state WHERE entity_id=$1 AND node_id=$2`
	}
	raw := make([]string, len(want))
	outputs := make([]any, len(raw))
	for index := range raw {
		outputs[index] = &raw[index]
	}
	if err := native.QueryRowContext(ctx, query, identifier, node).Scan(outputs...); err != nil {
		t.Fatalf("read actual generated list columns: %v", err)
	}
	for index := range raw {
		var got []any
		if err := json.Unmarshal([]byte(raw[index]), &got); err != nil {
			t.Fatalf("list column %d lost its encoding: %q %v", index, raw[index], err)
		}
		if index == 7 && len(got) == 1 {
			parsed, err := time.Parse(time.RFC3339, got[0].(string))
			if err != nil {
				t.Fatal(err)
			}
			got[0] = parsed.UTC().Format(time.RFC3339)
		}
		if !reflect.DeepEqual(got, want[index]) {
			t.Fatalf("list column %d changed values/order: %#v, want %#v", index, got, want[index])
		}
	}
}
