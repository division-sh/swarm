package manager_test

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestTerminalPanicFinalizationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"success", "commit_failure", "commit_and_finalizer_failure"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				var selected storetest.AgentFixtureStore
				if backend == "postgres" {
					_, db, cleanup := testutil.StartPostgres(t)
					t.Cleanup(cleanup)
					selected = storetest.AdmitPostgresRuntimeStore(t, db)
				} else {
					selected = storetest.StartSQLiteRuntimeStore(t)
				}
				rec := manager.TerminalPanicFixture(t)
				rec.Topology = agenttopology.Admission{}
				rec.Config.LLMBackend = "anthropic"
				rec.Config.ResolvedLLMBackend = "anthropic"
				storetest.RequireStaticAgentFixture(t, manager.TerminalPanicFixtureContext(), selected, rec)
				state, found, err := selected.LoadAgentLifecycleState(context.Background(), rec.Config.Identity)
				if err != nil || !found {
					t.Fatalf("admitted panic fixture: found=%t err=%v", found, err)
				}
				// Preserve the exact declaration admitted above. This is a live-loop
				// finalization proof, not a configuration hydration/recovery fixture.
				rec.Topology = state.Topology
				rec.LifecycleEpoch, rec.LifecycleGeneration = state.RuntimeEpoch, state.Generation
				rec.LifecyclePhase, rec.LifecycleRunMode = state.Phase, state.RunMode
				rec.ProcessBinding = state.ProcessBinding
				persistence := storetest.AgentLifecycleFixture(t, selected)
				if mode == "success" {
					manager.ProveTerminalPanicFinalization(t, persistence, selected, rec, false)
					return
				}
				const failure = "exact panic terminal mutation rollback"
				condition := "NEW.agent_id = '" + strings.ReplaceAll(rec.Config.ID, "'", "''") + "' AND NEW.lifecycle_phase = 'failed'"
				if mode == "commit_and_finalizer_failure" {
					condition = "NEW.agent_id = '" + strings.ReplaceAll(rec.Config.ID, "'", "''") + "' AND NEW.lifecycle_phase IN ('failed', 'registered')"
				}
				db := storetest.Database(selected)
				ddl := "CREATE TRIGGER panic_terminal_failure BEFORE UPDATE ON agents WHEN " + condition + " BEGIN SELECT RAISE(ABORT, '" + failure + "'); END"
				if backend == "postgres" {
					if _, err := db.ExecContext(context.Background(), "CREATE FUNCTION panic_terminal_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION '"+failure+"'; END $$"); err != nil {
						t.Fatal(err)
					}
					ddl = "CREATE TRIGGER panic_terminal_failure BEFORE UPDATE ON agents FOR EACH ROW WHEN (" + condition + ") EXECUTE FUNCTION panic_terminal_failure()"
				}
				if _, err := db.ExecContext(context.Background(), ddl); err != nil {
					t.Fatal(err)
				}
				manager.ProveTerminalPanicMutationFailure(t, persistence, selected, rec, failure, mode == "commit_and_finalizer_failure")
			})
		}
	}
}
