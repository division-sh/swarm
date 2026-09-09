package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestReceiverFlowInitializationPublicationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, consumer := range []struct {
			name  string
			kind  canonicalrouting.TemplateInstanceConsumer
			count int
		}{{"node", canonicalrouting.TemplateInstanceNodeConsumer, 1}, {"agent", canonicalrouting.TemplateInstanceAgentConsumer, 1}, {"observer_and_agent", canonicalrouting.TemplateInstanceNodeAndAgentConsumer, 2}, {"observer_and_two_agents", canonicalrouting.TemplateInstanceNodeAndTwoAgentConsumer, 3}} {
			t.Run(backend.name+"/"+consumer.name, func(t *testing.T) {
				fixture := backend.open(t)
				root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{Mode: canonicalrouting.TemplateInstanceRouteSelectOrCreate, Consumer: consumer.kind})
				repo := canonicalrouting.RepoRoot(t)
				bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
				if err != nil {
					t.Fatal(err)
				}
				source := semanticview.Wrap(bundle)
				runID := uuid.NewString()
				ctx := effects.WithExecutionMode(correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID), executionmode.Live)
				descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
				if err != nil {
					t.Fatal(err)
				}
				scope, ok := authoractivity.ScopeFromContext(ctx)
				if !ok {
					t.Fatal("missing author scope")
				}
				lease, err := fixture.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(lease.Release)
				fact, ok := correlation.SourceArtifactFactFromContext(ctx)
				if !ok {
					t.Fatal("missing source fact")
				}
				workflow := configureAgentFixtureFlowLifecycle(t, fixture.store.(agentFixtureFlowStore), &sqliteFlowActivationBus{}, bundle)
				planner := ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
					ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact,
					SemanticSource: source, WorkflowInstances: workflow, WorkOwner: storeTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(),
				}))
				eventBus, err := newStoreTestEventBus(t, fixture.store.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, TemplateInstancePlanner: planner, TemplateInstanceActivator: planner.ActivateFlowInstance})
				if err != nil {
					t.Fatal(err)
				}
				src, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()})
				if err != nil {
					t.Fatal(err)
				}
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "producer/deploy.done", "operator", "", []byte(`{"vertical_id":"first"}`), 0, runID, events.EventEnvelope{}, src, time.Now().UTC())
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				plans, err := eventBus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
				if err != nil || len(plans) != 1 {
					t.Fatalf("prepare real flow activation: %d %v", len(plans), err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("preparation mutated store")
				}
				command := plans[0].(bus.EnginePublicationPlan).PublicationCommand()
				if len(command.Commit.DeliveryRoutes) != consumer.count || len(command.Activations) != 1 {
					t.Fatalf("routes/activations: %d/%d; commit=%+v", len(command.Commit.DeliveryRoutes), len(command.Activations), command.Commit)
				}
				for _, route := range command.Commit.DeliveryRoutes {
					if !route.Initialization.FlowLifecycle() || !route.Materialization.Empty() {
						t.Fatalf("invented node dependency: %+v", route)
					}
				}
				store := fixture.store.(interface {
					CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
					LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
					deliverylifecycle.Store
				})
				if _, err := store.CommitPublication(ctx, command); err != nil {
					t.Fatal(err)
				}
				loaded, found, err := store.LoadPreparedPublishEvent(ctx, event.ID())
				if err != nil || !found {
					t.Fatalf("durable aggregate: %t %v", found, err)
				}
				if err := loaded.Validate(); err != nil {
					t.Fatal(err)
				}
				for _, route := range command.Commit.DeliveryRoutes {
					id := mustReceiverDeliveryID(t, event.ID(), route)
					snapshot, err := store.Snapshot(ctx, id)
					if err != nil || !reflect.DeepEqual(snapshot.Route, route.Normalized()) {
						t.Fatalf("durable supplier lost: %+v %v", snapshot.Route, err)
					}
					if route.Recipient.IsAgent() {
						beforeClaim := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						claim, err := store.ClaimDelivery(ctx, snapshot.Authority, command.Commit.Event.Event(), route)
						if err != nil || claim.Disposition != deliverylifecycle.ClaimDeferred {
							t.Fatalf("initialization bypassed runtime readiness: %+v %v", claim, err)
						}
						if !reflect.DeepEqual(beforeClaim, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
							t.Fatal("not-ready agent appended an attempt")
						}
					}
				}
				// Reconstruct EventBus and consume the durable publication rather
				// than electing another initializer from now-existing state.
				if err := eventBus.ReleaseEnginePublications(ctx, plans); err != nil {
					t.Fatal(err)
				}
				eventBus, err = newStoreTestEventBus(t, fixture.store.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, TemplateInstancePlanner: planner, TemplateInstanceActivator: planner.ActivateFlowInstance})
				if err != nil {
					t.Fatal(err)
				}
				recovered, err := eventBus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
				if err != nil || len(recovered) != 1 {
					t.Fatalf("recover publication: %v", err)
				}
				defer eventBus.ReleaseEnginePublications(ctx, recovered)
				recoveredRoutes := recovered[0].(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes
				indexRoutes := func(routes []events.DeliveryRoute) map[events.DeliveryRouteIdentity]events.DeliveryRoute {
					indexed := make(map[events.DeliveryRouteIdentity]events.DeliveryRoute, len(routes))
					for _, route := range routes {
						id, err := route.Identity()
						if err != nil {
							t.Fatal(err)
						}
						if _, duplicate := indexed[id]; duplicate {
							t.Fatal("duplicate route")
						}
						indexed[id] = route.Normalized()
					}
					return indexed
				}
				if !reflect.DeepEqual(indexRoutes(recoveredRoutes), indexRoutes(command.Commit.DeliveryRoutes)) {
					t.Fatal("recovery changed initialization evidence")
				}
				before = snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				if _, err := store.CommitPublication(ctx, command); err != nil {
					t.Fatalf("duplicate: %v", err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("duplicate changed history")
				}
			})
		}
	}
}
