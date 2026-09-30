package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
)

// Nonzero budget has no authored e2e producer. This is selected-store evidence,
// separate from the real golden activation/restart's flow-data capability.
func TestPersistedAgentReadinessFieldsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected agentfixture.Store
			if backend == "sqlite" {
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				selected = newTestPostgresStore(t, db)
			}
			cfg := withRuntimePersistenceTestIntent(t, actors.AgentConfig{
				ID: "readiness-fields", Identity: testAgentIdentity(t, "readiness-fields", "review/item"),
				ExecutionMode: "live", Role: "worker", Type: "worker", Model: "regular", LLMBackend: "claude_cli",
				Memory: agentmemory.Authored(false), FlowPath: "review/item",
				FlowDataAccess: []string{"profile.md"}, BudgetEnvelope: 1.25,
				Config: json.RawMessage(`{}`),
			})
			plan, err := cfg.Identity.Plan()
			if err != nil {
				t.Fatal(err)
			}
			wantRevision, err := manager.AgentConfigPlanRevision(cfg, plan)
			if err != nil {
				t.Fatal(err)
			}
			ctx := testAuthorActivityContext()
			if err := agentfixture.UpsertStatic(t, ctx, selected, manager.PersistedAgent{Config: cfg, Status: "active"}); err != nil {
				t.Fatal(err)
			}
			for read := 0; read < 2; read++ {
				agents, err := selected.LoadAgents(ctx)
				if err != nil || len(agents) != 1 {
					t.Fatalf("read %d: agents=%d err=%v", read, len(agents), err)
				}
				got := agents[0].Config
				if !reflect.DeepEqual(got.FlowDataAccess, cfg.FlowDataAccess) || got.BudgetEnvelope != cfg.BudgetEnvelope {
					t.Fatalf("read %d lost readiness-bearing fields: data=%v budget=%v", read, got.FlowDataAccess, got.BudgetEnvelope)
				}
				revision, err := manager.AgentConfigPlanRevision(got, plan)
				if err != nil || revision != wantRevision {
					t.Fatalf("read %d changed readiness revision: got=%s want=%s err=%v", read, revision, wantRevision, err)
				}
				got.FlowDataAccess[0] = "mutated-reader-copy"
			}
		})
	}
}
