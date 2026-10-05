package tools_test

import (
	"context"
	"database/sql"
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
				selected = newSQLiteRuntimeToolStoreForTest(t)
			} else {
				selected = newPostgresHumanTaskToolStoreForTest(t)
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
					assertRetiredConstructorNoWrites(t, ctx, storetest.DatabaseForTest(selected), backend)
				})
			}
		})
	}
}

func assertRetiredConstructorNoWrites(t *testing.T, ctx context.Context, db *sql.DB, backend string) {
	t.Helper()
	for _, table := range []string{"entity_state", "entity_mutations", "flow_instances", "flow_instance_runtime_readiness"} {
		query := "SELECT COUNT(*) FROM " + table + " WHERE run_id = ?"
		if backend == "postgres" {
			query = "SELECT COUNT(*) FROM " + table + " WHERE run_id = $1::uuid"
		}
		var count int
		if err := db.QueryRowContext(ctx, query, entityToolTestRunID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("retired constructor changed %s: rows=%d err=%v", table, count, err)
		}
	}
}
