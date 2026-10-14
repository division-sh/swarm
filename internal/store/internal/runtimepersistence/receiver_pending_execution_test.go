package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestReceiverEntitylessPendingAttachmentExecutesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, canonicalrouting.ReceiverPendingExecutionDocuments(t), nil, func(options *manager.AgentManagerOptions) {
				options.ExecutionPosture = executionposture.MockOnly
			})
			source := semanticview.Wrap(f.bundle)
			ctx := effects.WithExecutionMode(f.ctx, executionmode.Mock)
			runID := correlation.RunIDFromContext(ctx)
			event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "work.requested", "pending-receiver-proof", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC(), executionmode.Mock)
			req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
			req.Instance = flowidentity.Stored(source, ".", runID, runID, runID, "")
			req.TriggerEvent, req.OccurredAt = event, event.CreatedAt()
			plan, err := f.manager.PrepareFlowInstanceActivation(ctx, req)
			if err != nil || len(plan.Children) != 1 || len(plan.Children[0].Readiness.Agents) != 1 {
				t.Fatalf("prepare genuine static receiver: %+v %v", plan, err)
			}
			committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
			if err != nil || !committed.Acknowledged {
				t.Fatalf("commit original construction: %+v %v", committed, err)
			}
			descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
			if err != nil {
				t.Fatal(err)
			}
			scope, _ := authoractivity.ScopeFromContext(ctx)
			catalog, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			work := storeTestWorkOwner(t)
			options := bus.EventBusOptions{ExecutionPosture: executionposture.MockOnly, TemplateInstancePlanner: f.manager, WorkOwner: work}
			publisher := f.newRuntimeEventBus(t, options)
			plans, err := publisher.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
			if err != nil || len(plans) != 1 {
				t.Fatalf("prepare pending static publication: %v", err)
			}
			preparingPublisher := publisher
			t.Cleanup(func() {
				if err := preparingPublisher.ReleaseEnginePublications(context.WithoutCancel(ctx), plans); err != nil {
					t.Error(err)
				}
			})
			command := plans[0].(bus.EnginePublicationPlan).PublicationCommand()
			if len(command.Commit.DeliveryRoutes) != 1 || len(command.Activations) != 0 {
				t.Fatalf("existing constructor was replaced or consumer omitted: %+v", command)
			}
			route := command.Commit.DeliveryRoutes[0]
			if !route.Recipient.IsAgent() || !route.Target.EntitylessReceiver() || route.Initialization.Empty() || route.Initialization.ConstructionTarget().Route().EntityID != plan.Children[0].Identity.EntityID {
				t.Fatalf("pending static carrier conflates actor and constructor: %+v", route)
			}
			store := f.store.(interface {
				CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
				deliverylifecycle.Store
			})
			if _, err := store.CommitPublication(ctx, command); err != nil {
				t.Fatal(err)
			}
			id := mustReceiverDeliveryID(t, event.ID(), route)
			before, err := store.Snapshot(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			deferred, err := store.ClaimDelivery(ctx, before.Authority, command.Commit.Event.Event(), route)
			if err != nil || deferred.Disposition != deliverylifecycle.ClaimDeferred {
				t.Fatalf("pending attachment permitted execution: %+v %v", deferred, err)
			}
			if after, err := store.Snapshot(ctx, id); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("deferred claim changed durable evidence: %+v %v", after, err)
			}
			// Discard the publication return path. Recovery must consume the stored
			// receipt, not a handoff retained by the preparing Bus.
			if err := publisher.ReleaseEnginePublications(ctx, plans); err != nil {
				t.Fatal(err)
			}
			publisher = f.newRuntimeEventBus(t, options)
			if _, err := store.CommitPublication(ctx, command); err != nil {
				t.Fatalf("repeat committed publication after losing its response: %v", err)
			}
			if after, err := store.Snapshot(ctx, id); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("duplicate publication changed the pending receipt: %+v %v", after, err)
			}
			// This agent-only publication has no node handler. Finish its pipeline
			// frontier through the native owner before recovering delivery work.
			pipelineWork := f.store.(storeTestDurableEventBusStore).PipelineObligations()
			claimed, err := pipelineWork.ClaimEvent(ctx, event.ID(), pipelineobligation.PurposeRecovery)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pipelineWork.Settle(ctx, claimed.Claim, pipelineobligation.Acknowledged("processed")); err != nil {
				t.Fatal(err)
			}
			gate := &publicationGroupAgentGate{started: make(chan struct{})}
			am := ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(publisher, publicationGroupRealAgentFactory(t, f.store, publisher, source, gate), manager.AgentManagerOptions{
				SourceArtifactFact: mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), SemanticSource: source,
				ExecutionPosture: executionposture.MockOnly, LLMBackend: "anthropic", BaseContext: ctx,
				WorkflowInstances: f.workflows, DeliveryStore: f.store, WorkOwner: work, ReceiverExecution: eventreceiver.NormalExecution(),
				PersistenceRoles: manager.PersistenceRoles{AgentRoutes: publisher, CreationPublisher: publisher, DeliveryRuntime: publisher, LifecycleState: f.store.(manager.AgentLifecycleStateReader)},
			}, f.store))
			coordinate := agenttopology.SourceCoordinate{BundleHash: f.bundle.SourceArtifact.BundleHash()}
			set, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
			if err != nil {
				t.Fatal(err)
			}
			admission, err := agenttopology.StaticAdmission(set.Revision, coordinate.BundleHash, agenttopology.LifetimeDurableManaged)
			if err != nil {
				t.Fatal(err)
			}
			if err := am.InstallStartupTopology(f.grant, admission, set); err != nil {
				t.Fatal(err)
			}
			publisher.SetCommittedAgentReadinessFinalizer(bus.CommittedAgentReadinessFinalizerFunc(am.FinalizeCommittedAgentReadiness))
			binding, err := f.grant.ProcessExecutionBinding()
			if err != nil {
				t.Fatal(err)
			}
			managed, err := managedexecution.New(managedexecution.KindNormalRuntime, binding.RuntimeInstanceID, binding.RuntimeGeneration, "", "pending-static-execution", binding.BundleHash, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx = managedexecution.WithAdmission(ctx, managed)
			if err := am.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if err := am.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
				t.Fatal(err)
			}
			if err := am.FinalizeCommittedAgentReadiness(ctx, command.Commit.Event.Event(), []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			eligible, err := store.ObserveDeliveryContinuation(ctx, before.Authority, id)
			if err != nil || eligible.Disposition != deliverylifecycle.ClaimAcquired || gate.entries.Load() != 0 {
				t.Fatalf("attached actor not eligible before recovery: %+v executions=%d err=%v", eligible, gate.entries.Load(), err)
			}
			continuations, err := deliverycontinuation.New(store, f.store.(runlifecycle.StandingRestartDispositionReader), before.Authority, work, publisher, func(_ context.Context, err error) {
				t.Errorf("recover pending static carrier: %v", err)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := publisher.SetDeliveryContinuationOwner(continuations); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := continuations.Retire(join); err != nil {
					t.Error(err)
				}
			})
			if err := continuations.Start(ctx); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(15 * time.Second)
			for {
				after, err := store.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if after.Terminal() && after.Status == deliverylifecycle.StatusDelivered {
					if gate.entries.Load() != 1 || after.ClaimVersion != 1 || !reflect.DeepEqual(before.Route, after.Route) || !after.Authority.Equal(before.Authority) {
						t.Fatalf("execution replaced actor/constructor/claim ownership: before=%+v after=%+v executions=%d", before, after, gate.entries.Load())
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("attached mock actor did not settle: %+v", after)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}
