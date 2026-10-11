package tools_test

import (
	"encoding/json"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestQ10AgentReadbackAndQueryOwnershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			actor := models.AgentConfig{ExecutionMode: "live", ID: "tester", Type: "internal", Role: "operator",
				Tools: []string{"get_entity", "search_entities", "query_entities", "query_metrics"}}
			bundle := loadWave1EntityToolBundleWithInitialStage(t, actor, "review", "accounts", "", "accounts:\n  status: text\n  score: numeric\n", "queued", true)
			var selected runtimetools.EntityPersistence
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			ctx := runtimetools.WithActor(seedEntityToolSourceRun(t, selected, bundle), actor)
			executor := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
				EntityStore: selected, WorkflowSource: semanticview.Wrap(bundle), AllowInternalLegacyEntityTools: true,
			})
			constructed := seedImportedEntityForToolTest(t, ctx, map[string]any{
				"flow_instance": "review/constructed", "fields": map[string]any{"status": "constructed", "score": 40.0},
			})
			imports := []string{
				seedReadOnlyImportedEntityForToolTest(t, ctx, map[string]any{
					"flow_instance": "review/import-one", "fields": map[string]any{"status": "import-one", "score": 2.0},
				}),
				seedReadOnlyImportedEntityForToolTest(t, ctx, map[string]any{
					"flow_instance": "review/import-two", "fields": map[string]any{"status": "import-two", "score": 3.0},
				}),
			}
			read := func(tool string, input map[string]any) map[string]any {
				t.Helper()
				output, err := executor.Execute(ctx, tool, input)
				if err != nil {
					t.Fatalf("%s: %v", tool, err)
				}
				return output.(map[string]any)
			}
			for _, entityID := range append([]string{constructed}, imports...) {
				row := read("get_entity", map[string]any{"entity_id": entityID})
				for _, private := range []string{"bookkeeping", "loops"} {
					if _, exists := row[private]; exists {
						t.Fatalf("get_entity leaked %s: %v", private, row)
					}
				}
				if entityID == constructed {
					if row["current_state"] != "queued" || row["revision"] == nil {
						t.Fatalf("constructed header missing: %v", row)
					}
					continue
				}
				for _, key := range []string{"stage_defined", "current_state", "revision", "entered_state_at", "gates", "accumulator", "lifecycle_created_at", "lifecycle_updated_at"} {
					if value, present := row[key]; !present || value != nil {
						t.Fatalf("imported %s must be present and null: %v", key, row)
					}
				}
				raw, err := json.Marshal(row)
				if err != nil {
					t.Fatal(err)
				}
				var wire map[string]json.RawMessage
				if err := json.Unmarshal(raw, &wire); err != nil || string(wire["current_state"]) != "null" {
					t.Fatalf("import null changed on the wire: %s, %v", raw, err)
				}
			}

			search := read("search_entities", map[string]any{"current_state": "queued"})
			rows := search["results"].([]map[string]any)
			if len(rows) != 1 || rows[0]["entity_id"] != constructed || search["total"] != 1 {
				t.Fatalf("stage filter selected imports: %v", search)
			}
			for _, expression := range []string{`current_state == null`, `current_state != "queued"`} {
				query := read("query_entities", map[string]any{"filter": expression, "select": []string{"entity_id", "current_state"}})
				rows := query["results"].([]map[string]any)
				seen := map[string]bool{}
				for _, row := range rows {
					if row["current_state"] != nil {
						t.Fatalf("null query fabricated a stage: %v", row)
					}
					seen[row["entity_id"].(string)] = true
				}
				if len(rows) != 2 || len(seen) != 2 || !seen[imports[0]] || !seen[imports[1]] {
					t.Fatalf("query changed import membership: %v", query)
				}
			}
			shortCircuit := read("query_entities", map[string]any{"filter": `current_state != null && current_state > "a"`})
			if rows := shortCircuit["results"].([]map[string]any); len(rows) != 1 || rows[0]["entity_id"] != constructed {
				t.Fatalf("guarded comparison changed membership: %v", shortCircuit)
			}
			if _, err := executor.Execute(ctx, "query_entities", map[string]any{"filter": `current_state > "a"`}); err == nil {
				t.Fatal("ordered comparison must reject null instead of comparing a sentinel")
			}
			groups := read("query_entities", map[string]any{"group_by": "current_state"})["results"].([]map[string]any)
			if len(groups) != 2 || groups[0]["group_key"] != "queued" || groups[1]["group_key"] != nil ||
				len(groups[0]["items"].([]map[string]any)) != 1 || len(groups[1]["items"].([]map[string]any)) != 2 {
				t.Fatalf("null grouping or null-last order changed: %v", groups)
			}
			for _, tc := range []struct {
				metric string
				field  string
				want   []float64
			}{
				{"count", "", []float64{1, 2}},
				{"sum", "score", []float64{40, 5}},
			} {
				input := map[string]any{"metric": tc.metric, "group_by": "current_state"}
				if tc.field != "" {
					input["field"] = tc.field
				}
				groups := read("query_metrics", input)["groups"].([]map[string]any)
				if len(groups) != 2 || groups[0]["group_key"] != "queued" || groups[1]["group_key"] != nil ||
					testNumericValue(groups[0]["value"]) != tc.want[0] || testNumericValue(groups[1]["value"]) != tc.want[1] {
					t.Fatalf("metric %s lost count/numeric conservation: %v", tc.metric, groups)
				}
			}
			foreign := read("query_entities", map[string]any{"flow_instance": "foreign/one"})
			if rows := foreign["results"].([]map[string]any); len(rows) != 0 {
				t.Fatalf("undeclared path escaped the admitted flow scope: %v", rows)
			}

			advanceReviewedEntityToolFixture(t, ctx, "review/constructed", constructed)
			if row := read("get_entity", map[string]any{"entity_id": constructed}); row["current_state"] != "marginal_review" {
				t.Fatalf("get_entity returned declared initial instead of stored current stage: %v", row)
			}
			advanced := read("search_entities", map[string]any{"current_state": "marginal_review"})
			if rows := advanced["results"].([]map[string]any); len(rows) != 1 || rows[0]["entity_id"] != constructed || rows[0]["current_state"] != "marginal_review" {
				t.Fatalf("search_entities lost the stored-stage discriminator: %v", advanced)
			}
			advanced = read("query_entities", map[string]any{"filter": `current_state == "marginal_review"`, "select": []string{"entity_id", "current_state"}})
			if rows := advanced["results"].([]map[string]any); len(rows) != 1 || rows[0]["entity_id"] != constructed || rows[0]["current_state"] != "marginal_review" {
				t.Fatalf("query_entities lost stored-stage membership/value: %v", advanced)
			}
			if rows := read("search_entities", map[string]any{"current_state": "queued"})["results"].([]map[string]any); len(rows) != 0 {
				t.Fatalf("declaration initial resurrected after native transition: %v", rows)
			}
		})
	}
}
