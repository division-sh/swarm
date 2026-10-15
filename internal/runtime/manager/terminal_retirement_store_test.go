package manager_test

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestTerminalPanicFinalizationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"success", "commit_failure", "commit_and_finalizer_failure"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				var selected storetest.AgentFixtureStore
				if backend == "postgres" {
					selected = storetest.StartPostgresRuntimeStore(t)
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
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				if err := storetest.InstallTerminalPanicMutationFailure(cancelled, selected, rec.Config.ID, false); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled panic fault was installed: %v", err)
				}
				if err := storetest.InstallTerminalPanicMutationFailure(context.Background(), selected, "missing-panic-agent", false); err == nil {
					t.Fatal("missing agent accepted a panic fault")
				}
				if err := storetest.InstallTerminalPanicMutationFailure(context.Background(), struct{}{}, rec.Config.ID, false); err == nil {
					t.Fatal("foreign owner accepted a panic fault")
				}
				probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
				if err := storetest.InstallTerminalPanicMutationFailure(context.Background(), selected, rec.Config.ID, mode == "commit_and_finalizer_failure"); err != nil {
					t.Fatal(err)
				}
				if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
					t.Fatalf("panic fault bypassed original writer or leaked installation: %+v", counts)
				}
				probe.Stop()
				manager.ProveTerminalPanicMutationFailure(t, persistence, selected, rec, failure, mode == "commit_and_finalizer_failure")
			})
		}
	}
}
