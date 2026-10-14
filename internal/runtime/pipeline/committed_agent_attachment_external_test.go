package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Hold construction of the second actor before lifecycle registration, not
// inside the lifecycle lock or by inventing a process readiness projection.
func TestCommittedAgentReadinessPartialAttachmentBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, scenario := range []struct {
			missing  string
			shutdown bool
		}{{"source", false}, {"sibling", false}, {"source", true}, {"sibling", true}} {
			t.Run(fmt.Sprintf("%s/missing_%s/shutdown_%t", backend.name, scenario.missing, scenario.shutdown), func(t *testing.T) {
				selected := backend.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected.events, runID)
				ctx, cancel := context.WithTimeout(withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)), 10*time.Second)
				defer cancel()
				missing := scenario.missing
				first := "source"
				if missing == first {
					first = "sibling"
				}
				first = "a-" + first
				missing = "z-" + missing
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, map[string]string{
					"schema.yaml":          "name: attachment-cut\nstages:\n  active: {}\n",
					"events.yaml":          "work.requested:\n  order_id: text\n",
					"orders/schema.yaml":   "name: orders\ninstance: order_id\npins:\n  inputs: [order.created, work.requested]\nstages:\n  active: {}\n",
					"orders/events.yaml":   "order.created:\n  order_id: text\nwork.requested:\n  order_id: text\n",
					"orders/entities.yaml": "work:\n  order_id: {type: text, indexed: true}\n",
					"orders/agents.yaml":   fmt.Sprintf("a-installed:\n  id: %s\n  model: regular\n  intent: {inline: Observe work.}\n  subscriptions: [work.requested]\nz-held:\n  id: %s\n  model: regular\n  intent: {inline: Observe work.}\n  subscriptions: [work.requested]\n", first, missing),
				}))
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
				if err != nil {
					t.Fatal(err)
				}
				pc := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: proposedEffectProofModule{source: source}})
				bus.SetInterceptors(pc)
				entered, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unhold := func() { releaseOnce.Do(func() { close(release) }) }
				defer unhold()
				am, grant, releaseProcess := newPartialAttachmentManager(t, ctx, selected, source, pc, bus, func(cfg actors.AgentConfig) (manager.Agent, error) {
					if cfg.ID == missing {
						close(entered)
						select {
						case <-release:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return &partialAttachmentAgent{config: cfg}, nil
				})
				parent, err := flowidentity.StandingForGeneration(source, ".", runID)
				if err != nil {
					t.Fatal(err)
				}
				instance, err := flowidentity.KeyedChild(source, parent, "orders", "one")
				if err != nil {
					t.Fatal(err)
				}
				at := time.Now().UTC()
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "order.created", "operator", "", []byte(`{"order_id":"one"}`), 0, runID, events.EventEnvelope{}, at)
				plan, err := am.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: instance,
					ConstructorInput: "order.created", ResolvedKey: "one", TriggerEvent: event, OccurredAt: at})
				if err != nil {
					t.Fatal(err)
				}
				committed, err := bus.CommitFlowInstanceActivation(ctx, plan)
				if err != nil || !committed.Acknowledged {
					t.Fatalf("native construction: acknowledged=%t err=%v", committed.Acknowledged, err)
				}
				finished := make(chan error, 1)
				go func() { finished <- am.FinalizeCommittedFlowInstanceActivation(ctx, committed) }()
				select {
				case <-entered:
				case err := <-finished:
					t.Fatalf("attachment did not reach partial cut: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				configs := am.ListAgentConfigs()
				if len(configs) != 1 || configs[0].ID != first {
					t.Fatalf("not sibling-installed/missing-actor cut: %+v", configs)
				}
				row, found, err := pc.LoadDynamicFlowRuntimeReadiness(ctx, runID, instance.Route())
				if err != nil || !found || row.AttemptState != "accepted" || row.Phase != pipeline.FlowAttachmentPlanned {
					t.Fatalf("no native accepted installation owner: row=%+v found=%t err=%v", row, found, err)
				}
				var route events.DeliveryRoute
				for _, agent := range row.Plan.Agents {
					if agent.Identity.AgentID() == missing {
						route = events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(missing), AgentIdentity: agent.Identity}
					}
				}
				if route.AgentIdentity.IsZero() {
					t.Fatal("missing actor absent from canonical census")
				}
				if err := am.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{route}); !errors.Is(err, runtimebus.ErrCommittedAgentRouteTransition) {
					t.Fatalf("live accepted partial attachment must defer exact actor: %v", err)
				}
				after, found, err := pc.LoadDynamicFlowRuntimeReadiness(ctx, runID, instance.Route())
				if err != nil || !found || !reflect.DeepEqual(row, after) {
					t.Fatalf("readiness observation mutated native row: found=%t err=%v", found, err)
				}
				foreign := route
				foreign.AgentIdentity = configs[0].Identity
				foreign.AgentIdentity.RunID = uuid.NewString()
				if err := am.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{foreign}); err == nil || errors.Is(err, runtimebus.ErrCommittedAgentRouteTransition) {
					t.Fatalf("foreign run borrowed partial attachment: %v", err)
				}
				unrelated := route
				unrelated.AgentIdentity.Name, err = agentidentity.DeclaredName("not-planned", "swarm://orders/not-planned")
				if err != nil {
					t.Fatal(err)
				}
				unrelated.Recipient = events.MustAgentDeliveryRecipient("not-planned")
				if err := am.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{unrelated}); !errors.Is(err, manager.ErrAgentNotFound) || errors.Is(err, runtimebus.ErrCommittedAgentRouteTransition) {
					t.Fatalf("unrelated actor borrowed installing census: %v", err)
				}
				if scenario.shutdown {
					provePartialAttachmentShutdownRestart(t, ctx, selected, source, pc, bus, am, grant, releaseProcess, committed, route, event, unhold, finished)
					return
				}
				stopContinuation := provePartialAttachmentContinuation(t, ctx, selected, bus, am, route, unhold, finished)
				unhold()
				if err := am.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatalf("installed actor did not become executable: %v", err)
				}
				stopContinuation()
				if err := am.Shutdown(); err != nil {
					t.Fatal(err)
				}
				if err := grant.Retire(ctx); err != nil {
					t.Fatal(err)
				}
				if err := am.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{route}); err == nil || errors.Is(err, runtimebus.ErrCommittedAgentRouteTransition) {
					t.Fatalf("retired generation remained transitional: %v", err)
				}
			})
		}
	}
}

type partialAttachmentDispatch struct {
	bus     *runtimebus.EventBus
	results chan deliverycontinuation.DispatchResult
}

func (d *partialAttachmentDispatch) DispatchDeliveryContinuation(ctx context.Context, event events.Event, route events.DeliveryRoute) deliverycontinuation.DispatchResult {
	result := d.bus.DispatchDeliveryContinuation(ctx, event, route)
	select {
	case d.results <- result:
	case <-ctx.Done():
	}
	return result
}

func provePartialAttachmentContinuation(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, bus *runtimebus.EventBus, am *manager.AgentManager, route events.DeliveryRoute, release func(), finished <-chan error) func() {
	t.Helper()
	authority, err := bus.DeliveryAuthority()
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.events.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	route.Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "orders", FlowInstance: "orders/one"})
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "work.requested", "operator", "", []byte(`{"order_id":"one"}`), 0, route.AgentIdentity.RunID,
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "orders", FlowInstance: "orders/one"}), time.Now().UTC())
	ack := pipelineobligation.Acknowledged("pipeline_persisted")
	storetest.CommitNativeDeliveryPublication(t, ctx, selected.events.(storetest.DeliveryPublicationFixtureStore), storetest.AdmitNativeDeliveryEvent(t, event), []events.DeliveryRoute{route}, authority, nil)
	publication, err := selected.events.PipelineObligations().ClaimEvent(ctx, event.ID(), pipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := selected.events.PipelineObligations().Settle(ctx, publication.Claim, ack)
	if err != nil || !outcome.DeliveryHandoffCommitted() {
		t.Fatalf("native exact handoff: %+v err=%v", outcome, err)
	}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	before, err := selected.events.Snapshot(ctx, id)
	if err != nil || before.Status != deliverylifecycle.StatusPending || before.ClaimVersion != 0 {
		t.Fatalf("exact initial delivery: %+v err=%v", before, err)
	}
	bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(am.FinalizeCommittedAgentReadiness))
	dispatch := &partialAttachmentDispatch{bus: bus, results: make(chan deliverycontinuation.DispatchResult, 8)}
	coordinator, err := deliverycontinuation.New(selected.events, selected.events, authority, pipelineExternalTestWorkOwner(t), dispatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			join, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := coordinator.Retire(join); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	if err := bus.SetDeliveryContinuationOwner(coordinator); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-dispatch.results:
		if result.Disposition() != deliverycontinuation.DispatchDeferred || result.WakeAuthority() != deliverycontinuation.DispatchWakeAgentRouteLifecycle {
			t.Fatalf("accepted partial continuation: disposition=%v failure=%v", result.Disposition(), result.Failure())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	partial, err := selected.events.Snapshot(ctx, id)
	if err != nil || !reflect.DeepEqual(before, partial) {
		t.Fatalf("deferral acquired or mutated missing actor's delivery: %+v err=%v", partial, err)
	}
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := selected.events.Snapshot(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == deliverylifecycle.StatusDelivered {
			if current.ClaimVersion != 1 {
				t.Fatalf("exact topology wake repeated execution: %+v", current)
			}
			return stop
		}
		if current.Terminal() {
			t.Fatalf("topology progress failed exact delivery: %+v", current)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("exact delivery did not settle after topology wake: %+v err=%v", current, ctx.Err())
		}
	}
}

type partialAttachmentAgent struct{ config actors.AgentConfig }

func (a *partialAttachmentAgent) ID() string   { return a.config.ID }
func (a *partialAttachmentAgent) Type() string { return "generic" }
func (a *partialAttachmentAgent) Subscriptions() []events.EventType {
	return []events.EventType{"work.requested"}
}
func (a *partialAttachmentAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func newPartialAttachmentManager(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, source semanticview.Source, pc *pipeline.PipelineCoordinator, bus *runtimebus.EventBus, factory manager.AgentFactory) (*manager.AgentManager, startupownership.LiveGenerationGrant, func()) {
	t.Helper()
	process, err := selected.events.(startupownership.Store).AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "partial-attachment", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	releaseProcess := func() {
		releaseOnce.Do(func() {
			if err := process.Release(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(releaseProcess)
	set, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: authorActivityTestSourceArtifactFact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := process.CurrentSourceSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		if current.Revision != set.Revision {
			t.Fatal("restart changed exact constructor source set")
		}
	} else if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: set}); err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: authorActivityTestSourceArtifactFact.BundleHash(),
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: set.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	admission, err := agenttopology.StaticAdmission(set.Revision, authorActivityTestSourceArtifactFact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	am := manager.NewAgentManagerWithOptions(bus, factory, manager.AgentManagerOptions{
		BaseContext: ctx, SemanticSource: source, SourceArtifactFact: authorActivityTestSourceArtifactFact,
		WorkflowInstances: pc, LifecycleStore: grant, DeliveryStore: selected.events,
		WorkOwner: pipelineExternalTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(), ExecutionPosture: executionposture.Live,
		PersistenceRoles: manager.PersistenceRoles{
			FlowActivation: bus, AgentRoutes: bus, CreationPublisher: bus, DeliveryRuntime: bus,
			LifecycleState: selected.events.(manager.AgentLifecycleStateReader), LifecycleOutcome: selected.events.(manager.AgentLifecycleOutcomeReader),
		},
	}, selected.events.(manager.ManagerPersistence))
	if err := am.InstallStartupTopology(grant, admission, set); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := am.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	execution, err := managedexecution.New(managedexecution.KindNormalRuntime, "partial-attachment", 1, "", "partial-attachment", authorActivityTestSourceArtifactFact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := am.RunAuthoritativeDeliveryOnly(managedexecution.WithAdmission(ctx, execution)); err != nil {
		t.Fatal(err)
	}
	return am, grant, releaseProcess
}

func provePartialAttachmentShutdownRestart(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, source semanticview.Source, pc *pipeline.PipelineCoordinator, bus *runtimebus.EventBus, am *manager.AgentManager, grant startupownership.LiveGenerationGrant, releaseProcess func(), committed pipeline.CommittedFlowInstanceActivation, route events.DeliveryRoute, event events.Event, release func(), finished <-chan error) {
	t.Helper()
	shutdown := make(chan error, 1)
	go func() { shutdown <- am.Shutdown() }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for am.IsRunning() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := am.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{route}); err == nil || errors.Is(err, runtimebus.ErrCommittedAgentRouteTransition) {
		t.Fatalf("shutdown installation still lent a topology wake: %v", err)
	}
	release()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("shutdown crossed partial installation as success")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-shutdown:
		if err == nil || !failures.OnlyBranches(err, func(branch error) bool { return errors.Is(branch, context.Canceled) }) {
			t.Fatalf("partial shutdown must retain only its owned cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	row, found, err := pc.LoadDynamicFlowRuntimeReadiness(ctx, event.RunID(), committed.Plan.Identity.Route())
	if err != nil || !found || row.AttemptState != "aborted" || row.Phase != pipeline.FlowAttachmentPlanned {
		t.Fatalf("partial shutdown did not settle exact failed attempt: %+v found=%t err=%v", row, found, err)
	}
	if err := am.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{route}); err == nil || errors.Is(err, runtimebus.ErrCommittedAgentRouteTransition) {
		t.Fatalf("aborted exact attempt lent missing actor a wake: %v", err)
	}
	if err := grant.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	releaseProcess()
	next, nextGrant, _ := newPartialAttachmentManager(t, ctx, selected, source, pc, bus, func(cfg actors.AgentConfig) (manager.Agent, error) { return &partialAttachmentAgent{config: cfg}, nil })
	if err := next.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
		t.Fatalf("native restart did not converge accepted construction: %v", err)
	}
	after, found, err := pc.LoadDynamicFlowRuntimeReadiness(ctx, event.RunID(), committed.Plan.Identity.Route())
	if err != nil || !found || after.AttemptOrdinal != row.AttemptOrdinal+1 || after.Phase != pipeline.FlowAttachmentReady || after.PlanHash != row.PlanHash {
		t.Fatalf("restart changed construction or reused aborted attachment: %+v found=%t err=%v", after, found, err)
	}
	if err := next.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("recovered exact actor remained absent: %v", err)
	}
	if err := next.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := nextGrant.Retire(ctx); err != nil {
		t.Fatal(err)
	}
}
