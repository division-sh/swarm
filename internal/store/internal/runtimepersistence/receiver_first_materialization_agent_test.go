package runtimepersistence

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Producer-boundary probe for E's ordinary source journey. No selected-fork
// runtime or provider is involved, and no future receiver row is fabricated.
func TestReceiverFirstMaterializationNodeAndAgentAdmissionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, agent := range []string{"", "same-name", "renamed-observer"} {
				name := agent
				if name == "" {
					name = "node-only-control"
				}
				t.Run(name, func(t *testing.T) {
					root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
					if agent != "" {
						if err := os.MkdirAll(filepath.Join(root, "consumer/prompts"), 0o755); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(root, "consumer/prompts/observer.md"), []byte("Observe the admitted item."), 0o644); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(root, "consumer/agents.yaml"), []byte(agent+":\n  id: "+agent+"\n  role: observer\n  model: regular\n  intent: prompts/observer.md\n  subscriptions: [receiver.seeded]\n  emit_events: []\n"), 0o644); err != nil {
							t.Fatal(err)
						}
					}
					repo := canonicalrouting.RepoRoot(t)
					bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
					if err != nil {
						t.Fatal(err)
					}
					source := semanticview.Wrap(bundle)
					runID := uuid.NewString()
					ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID)
					fact, ok := correlation.SourceArtifactFactFromContext(ctx)
					if !ok {
						t.Fatal("missing source fact")
					}
					at := time.Now().UTC().Truncate(time.Microsecond)
					trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "start.seeded", "operator", "", []byte(`{"token":"first"}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
					if err := insertCanonicalEventRecordFixture(ctx, fixture.store, trigger); err != nil {
						t.Fatal(err)
					}
					node, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
					if err != nil {
						t.Fatal(err)
					}
					emitted := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "receiver.seeded", eventtest.Producer(events.EventProducerNode, node.Key()), "", []byte(`{"token":"first"}`), 0,
						events.LineageFromEvent(trigger), events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at.Add(time.Second))
					eventBus, err := newStoreTestEventBus(t, fixture.store.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact})
					if err != nil {
						t.Fatal(err)
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					plans, err := eventBus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: emitted}})
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatal("preparation changed database")
					}
					if err != nil || len(plans) != 1 {
						t.Fatalf("ordinary first-materialization preparation: plans=%d err=%v", len(plans), err)
					}
					routes := plans[0].(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes
					want := 1
					if agent != "" {
						want = 2
					}
					if len(routes) != want {
						t.Fatalf("prepared routes=%+v want=%d", routes, want)
					}
					for _, route := range routes {
						if !route.Target.MaterializingEntity() || route.Target.Route().FlowID != "consumer" || route.Target.Route().EntityID == runID {
							t.Fatalf("receiver authority not independent future: %+v", route)
						}
					}
				})
			}
		})
	}
}
