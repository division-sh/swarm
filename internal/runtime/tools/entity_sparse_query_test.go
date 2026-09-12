package tools_test

import (
	"errors"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
)

// Internal query implementations are not normal generated agent capabilities.
// Exercise their existing harness admission separately from role-scoped tools.
func TestEntityInternalQuerySparseBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			actor := models.AgentConfig{ExecutionMode: "live", ID: "tester", Role: "operator", Tools: []string{"create_entity", "query_entities", "query_metrics"}}
			bundle := loadWave1EntityToolBundle(t, actor, "review", "accounts", "", "accounts:\n  score: integer?\n  label: text?\n  enabled: boolean?\n")
			var persistence sparseEntityToolStore
			if backend == "sqlite" {
				persistence = newSQLiteRuntimeToolStoreForTest(t)
			} else {
				persistence = newPostgresHumanTaskToolStoreForTest(t)
			}
			ctx := tools.WithActor(seedEntityToolSourceRun(t, persistence, bundle), actor)
			executor := tools.NewExecutorWithOptions(nil, tools.ExecutorOptions{EntityStore: persistence, WorkflowSource: semanticview.Wrap(bundle), AllowInternalLegacyEntityTools: true})
			for _, fields := range []map[string]any{{}, {"score": 0, "label": "", "enabled": false}, {"score": 2, "label": "yes", "enabled": true}} {
				mustCreateEntityID(t, ctx, executor, map[string]any{"flow_instance": "review/inst-1", "fields": fields})
			}
			for _, tool := range []string{"query_entities", "query_metrics"} {
				for _, filter := range []string{`!has(fields.score)`, `has(fields.score) && fields.score == 0`, `fields.?score.orValue(-1) == -1`, `has(fields.label) && fields.label == "" && has(fields.enabled) && !fields.enabled`} {
					out, err := executor.Execute(ctx, tool, map[string]any{"filter": filter, "metric": "count", "limit": 100})
					if err != nil {
						t.Fatalf("%s %s: %v; cause=%v", tool, filter, err, errors.Unwrap(err))
					}
					result := out.(map[string]any)
					count := 0
					if tool == "query_entities" {
						count = len(result["results"].([]map[string]any))
					} else {
						count = int(testNumericValue(result["value"]))
					}
					if count != 1 {
						t.Fatalf("%s %s: count=%d, result=%#v", tool, filter, count, result)
					}
				}
				if out, err := executor.Execute(ctx, tool, map[string]any{"filter": "score == 0", "metric": "count", "limit": 100}); err == nil {
					t.Fatalf("unsafe %s predicate accepted: %#v", tool, out)
				}
			}
		})
	}
}
