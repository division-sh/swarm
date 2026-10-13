package manager_test

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestAgentRowIndividualSourceSetReplacementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected storetest.AgentFixtureStore
			if backend == "postgres" {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			} else {
				selected = storetest.StartSQLiteRuntimeStore(t)
			}
			ctx := manager.TerminalPanicFixtureContext()
			record := manager.TerminalPanicFixture(t)
			record.Config.LLMBackend, record.Config.ResolvedLLMBackend = "anthropic", "anthropic"
			record.Topology = agenttopology.Admission{}
			storetest.RequireStaticAgentFixture(t, ctx, selected, record)
			view, found, err := selected.LoadAgentLifecycleState(ctx, record.Config.Identity)
			if err != nil || !found {
				t.Fatalf("native fixture row: found=%v err=%v", found, err)
			}
			record.Topology = view.Topology
			manager.ProveAgentRowIndividualSourceSetReplacement(t, storetest.AgentLifecycleFixture(t, selected), selected, record, func() (manager.AgentLifecyclePersistence, agenttopology.Admission) {
				process, err := agentfixture.ProcessCapability(t, ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				current, found, err := process.CurrentSourceSet(ctx)
				if err != nil || !found {
					t.Fatalf("native source set: found=%v err=%v", found, err)
				}
				sources := append([]agenttopology.SourceCoordinate(nil), current.Sources...)
				sources = append(sources, agenttopology.SourceCoordinate{BundleHash: "bundle-v2:sha256:" + strings.Repeat("c", 64)})
				plan, err := agenttopology.NewSourceSetPlan(sources, current.Agents)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan}); err != nil {
					t.Fatal(err)
				}
				stamp, found, err := selected.LoadAgentLifecycleState(context.Background(), record.Config.Identity)
				if err != nil || !found {
					t.Fatalf("native predecessor stamp: found=%v err=%v", found, err)
				}
				grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: stamp.ProcessBinding.BundleHash, RuntimeInstanceID: stamp.ProcessBinding.RuntimeInstanceID, RuntimeGeneration: stamp.ProcessBinding.RuntimeGeneration + 1, SourceSetRevision: plan.Revision})
				if err != nil {
					t.Fatal(err)
				}
				topology, err := agenttopology.StaticAdmission(plan.Revision, stamp.ProcessBinding.BundleHash, stamp.Topology.Lifetime)
				if err != nil {
					t.Fatal(err)
				}
				return grant, topology
			})
		})
	}
}
