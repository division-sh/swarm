package tools_test

import (
	"fmt"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestEntityToolPredicatePresenceExecution(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "tester", Role: "operator", Tools: []string{"create_entity", "query_entities", "query_metrics"}}
	bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", `
types:
  Note:
    note: text?
  Metadata:
    region: text?
    notes: list<Note>
    by_name: map[text]Note
`, `
accounts:
  status: text
  metadata: Metadata
`)
	ctx, exec := newEntityToolTestExecutorWithBundle(t, actor, bundle)
	for _, populated := range []bool{false, true} {
		if populated {
			for _, region := range []string{"", "us"} {
				metadata := map[string]any{"notes": []any{map[string]any{}}, "by_name": map[string]any{"x": map[string]any{}}}
				if region != "" {
					metadata["region"] = region
				}
				mustCreateEntityID(t, ctx, exec, map[string]any{"flow_instance": "review/inst-1", "fields": map[string]any{"status": "open", "metadata": metadata}})
			}
		}
		for _, tool := range []string{"query_entities", "query_metrics"} {
			for _, tc := range []struct {
				expression string
				invalid    bool
				count      int
			}{
				{`metadata.region != ""`, true, 0},
				{`entity.metadata.region != ""`, true, 0},
				{`entity.?metadata.hasValue()`, true, 0},
				{`fields["metadata"]["region"] != ""`, true, 0},
				{`fields.?metadata.value().region != ""`, true, 0},
				{`metadata.notes.all(n, n.note == "")`, true, 0},
				{`metadata.notes[0].?note.orValue("") == ""`, true, 0},
				{`metadata.by_name[?"x"].value().note == ""`, true, 0},
				{`has(metadata.region) && metadata.region == "us"`, false, 1},
				{`metadata.?region.orValue("") == ""`, false, 1},
				{`fields.?metadata.value().?region.orValue("") == ""`, false, 1},
				{`status == "open" && current_state != "missing"`, false, 2},
				{`metadata.notes.all(n, n.?note.orValue("") == "")`, false, 2},
				{`metadata.by_name[?"x"].value().?note.orValue("") == ""`, false, 2},
			} {
				t.Run(fmt.Sprintf("%s/populated=%t/%s", tool, populated, tc.expression), func(t *testing.T) {
					out, err := exec.Execute(ctx, tool, map[string]any{"filter": tc.expression, "metric": "count", "limit": 100})
					if tc.invalid {
						if err == nil {
							t.Fatalf("unsafe predicate reached successful result: %v", out)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					result := out.(map[string]any)
					count := 0
					if tool == "query_metrics" {
						count = int(testNumericValue(result["value"]))
					} else {
						count = len(result["results"].([]map[string]any))
					}
					want := 0
					if populated {
						want = tc.count
					}
					if count != want {
						t.Fatalf("count=%d want=%d output=%v", count, want, out)
					}
				})
			}
		}
	}
}
