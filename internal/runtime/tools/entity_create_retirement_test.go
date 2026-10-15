package tools_test

import (
	"context"
	"fmt"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestRetiredCreateEntityCannotMutateEitherStore(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "tester", Role: "operator", Tools: []string{"create_entity"}}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWave1EntityToolBundle(t, actor, "review", "account", "", "account:\n  status: text\n")
			var selected runtimetools.EntityPersistence
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			ctx := runtimetools.WithActor(seedEntityToolSourceRun(t, selected, bundle), actor)
			for _, allowInternal := range []bool{false, true} {
				t.Run(fmt.Sprintf("internal_legacy_%t", allowInternal), func(t *testing.T) {
					exec := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
						EntityStore: selected, WorkflowSource: semanticview.Wrap(bundle), AllowInternalLegacyEntityTools: allowInternal,
					})
					for _, input := range []map[string]any{
						{"flow_instance": "review/one", "fields": map[string]any{"status": "open"}},
						{"flow_instance": "review/one", "entity_id": uuid.NewString()},
						{"flow_instance": "review/one", "entity_type": "account"},
						{"flow_instance": "review/one", "subject_id": uuid.NewString()},
						{"flow_instance": "missing/one"},
					} {
						if out, err := exec.Execute(ctx, "create_entity", input); err == nil || out != nil {
							t.Fatalf("retired constructor executed: out=%#v err=%v", out, err)
						}
					}
					assertRetiredConstructorNoWrites(t, ctx, selected)
				})
			}
		})
	}
}

func assertRetiredConstructorNoWrites(t *testing.T, ctx context.Context, selected any) {
	t.Helper()
	for _, cut := range []struct {
		table string
		read  func(context.Context, any, string) (int, error)
	}{
		{"entity_state", storetest.CountRunEntityStorage},
		{"entity_mutations", storetest.CountRunMutationStorage},
		{"flow_instances", storetest.CountRunFlowInstanceStorage},
		{"flow_instance_runtime_readiness", storetest.CountRunFlowRuntimeReadinessStorage},
	} {
		count, err := cut.read(ctx, selected, entityToolTestRunID)
		if err != nil || count != 0 {
			t.Fatalf("retired constructor changed %s: rows=%d err=%v", cut.table, count, err)
		}
	}
}
