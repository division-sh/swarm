package tools_test

import (
	"fmt"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"testing"
)

func TestLeadReview7StockOptionalToolExecution(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "tester", Role: "operator", Tools: []string{"create_entity", "query_entities", "query_metrics"}}
	bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", `
types:
  Metadata:
    region: text?
`, `
accounts:
  metadata: Metadata
`)
	ctx, executor := newEntityToolTestExecutorWithBundle(t, actor, bundle)
	for _, populated := range []bool{false, true} {
		if populated {
			mustCreateEntityID(t, ctx, executor, map[string]any{"flow_instance": "review/inst-1", "fields": map[string]any{"metadata": map[string]any{}}})
		}
		for _, tool := range []string{"query_entities", "query_metrics"} {
			for _, expression := range []string{
				`metadata.?region.orValue("") == ""`,
				`optional.of(metadata).value().?region.orValue("") == ""`,
				`optional.none().orValue("") == ""`,
				`optional.ofNonZeroValue("").orValue("fallback") == "fallback"`,
			} {
				t.Run(fmt.Sprintf("%s/populated=%t/%s", tool, populated, expression), func(t *testing.T) {
					result, err := executor.Execute(ctx, tool, map[string]any{"filter": expression, "metric": "count"})
					if err != nil {
						t.Fatalf("valid stock CEL predicate rejected: %v", err)
					}
					got := result.(map[string]any)
					count := 0
					if tool == "query_metrics" {
						count = int(testNumericValue(got["value"]))
					} else {
						count = len(got["results"].([]map[string]any))
					}
					want := 0
					if populated {
						want = 1
					}
					if count != want {
						t.Fatalf("got=%v want count=%d", got, want)
					}
				})
			}
		}
	}
}
