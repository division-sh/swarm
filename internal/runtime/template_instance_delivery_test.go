package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storetest "github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const templateInstanceDeliveryRunID = "99999999-9999-4999-8999-999999999901"

func templateInstanceRootNodeID(t testing.TB, nodeID string) string {
	t.Helper()
	return identitytest.RootNode(t, nodeID).Key()
}

func templateInstanceFlowNodeID(t testing.TB, flowID, nodeID string) string {
	t.Helper()
	return identitytest.FlowNode(t, flowID, nodeID).Key()
}

func TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsReceiptAndReplayScopeSeparately(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceDeliveryFixtureFiles())
	source := semanticview.Wrap(bundle)
	pg := storetest.StartPostgresRuntimeStore(t)
	ctx := seedRuntimeTestRunForSource(t, pg, source)
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle:     source,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc = newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		SourceArtifactFact:  runtimeTestSourceArtifactFact(t, source),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
	})
	constructed := seedTemplateInstanceDeliveryRouteOwner(t, ctx, pg, pc, source)

	assertRuntimeTestConstructedInstance(t, ctx, pg, source, constructed.Plan.Identity)
	eventID := "99999999-9999-4999-8999-999999999902"
	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		eventID,
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"operating",
		"",
		[]byte(`{"entity_id":"11111111-1111-4111-8111-111111111111"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EnvelopeForSourceRoute(
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-4111-8111-111111111111"), "operating/inst-1"),
			events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: "11111111-1111-4111-8111-111111111111"},
		),
		eventtest.ConcreteTemplateRoutingSource("operating", "operating/inst-1", "11111111-1111-4111-8111-111111111111"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	lifecycleNodeID := templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator")
	workflowRuntimeNodeID := templateInstanceRootNodeID(t, "workflow-runtime")
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, eventID, lifecycleNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 0, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, eventID, workflowRuntimeNodeID)
	})
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, eventID, lifecycleNodeID)
	})
	assertRuntimeCommittedReplayScope(t, ctx, pg, eventID)
	waitRuntimeStorageCount(t, ctx, pg, "CountEventNameStorage", 1, func() (int, error) {
		return storetest.CountEventNameStorage(ctx, pg, "operating/inst-1/opco.ceo_ready")
	})
}

func TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsAuthorityBeforeHandlerExecution(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceDeliveryFixtureFiles())
	source := semanticview.Wrap(bundle)
	pg := storetest.StartPostgresRuntimeStore(t)
	ctx := seedRuntimeTestRunForSource(t, pg, source)
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{ContractBundle: source, SourceArtifactFact: runtimeTestSourceArtifactFact(t, source)})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc := newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus), Module: newRuntimeTestWorkflowModule(t, source),
		Persistence: runtimepipeline.NewWorkflowPersistence(pg), RunLifecycle: pg,
		PipelineObligations: pg.PipelineObligations(), DeliveryStore: pg,
	})
	constructed := seedTemplateInstanceDeliveryRouteOwner(t, ctx, pg, pc, source)
	assertRuntimeTestConstructedInstance(t, ctx, pg, source, constructed.Plan.Identity)
	ch := runtimeInternalDeliveriesForTest(t, bus, "workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	eventID := "99999999-9999-4999-8999-999999999903"
	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		eventID,
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"operating",
		"",
		[]byte(`{"entity_id":"11111111-1111-4111-8111-111111111111"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EnvelopeForSourceRoute(
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-4111-8111-111111111111"), "operating/inst-1"),
			events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: "11111111-1111-4111-8111-111111111111"},
		),
		eventtest.ConcreteTemplateRoutingSource("operating", "operating/inst-1", "11111111-1111-4111-8111-111111111111"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-ch:
		defer func() { _ = got.Complete() }()
		if got.FlowInstance() != "operating/inst-1" || got.EntityID() != runtimeflowidentity.EntityID("operating/inst-1") {
			t.Fatalf("delivered route identity flow=%q entity=%q, want exact constructed operating/inst-1 receiver", got.FlowInstance(), got.EntityID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workflow-runtime carrier did not receive concrete template event")
	}
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, eventID, templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator"))
	})
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 0, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, eventID, templateInstanceRootNodeID(t, "workflow-runtime"))
	})
	assertRuntimeStorageCount(t, "CountTemplateNodeReceipt", 0, func() (int, error) {
		return storetest.CountTemplateNodeReceipt(ctx, pg, eventID, templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator"))
	})
}

func seedTemplateInstanceDeliveryRouteOwner(t testing.TB, ctx context.Context, selected runtimebus.FlowInstanceActivationCommitOwner, pc *runtimepipeline.PipelineCoordinator, source semanticview.Source) runtimepipeline.CommittedFlowInstanceActivation {
	t.Helper()
	seedRuntimeTestKeylessSource(t, ctx, selected, pc, semanticview.RootExecutionFlowID(source))
	return seedRuntimeTestPreparedInstance(t, ctx, selected, pc, runtimepipeline.WorkflowInstance{
		InstanceID: "inst-1", StorageRef: "operating/inst-1", EntityID: runtimeflowidentity.EntityID("operating/inst-1"),
		WorkflowName: "operating", WorkflowVersion: source.WorkflowVersion(), Mode: "template", InstanceKey: "inst-1",
		CurrentState: "initializing", StageDefined: true, EntityType: "operating_state", Fields: map[string]any{"instance_id": "inst-1"},
	})
}

func TestTemplateInstanceAutoEmitDispatchesLocalHandlerAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyTemplateInstanceEmpireStyle(t))
	source := semanticview.Wrap(bundle)
	pg := storetest.StartPostgresRuntimeStore(t)
	ctx := seedRuntimeTestRunForSource(t, pg, source)
	var manager *runtimemanager.AgentManager
	var activationCalls atomic.Int32
	activationResults := make(chan error, 4)
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, errors.New("agent manager is required")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return errors.New("agent manager is required")
			}
			activationCalls.Add(1)
			err := manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
			activationResults <- err
			return err
		}),
		ContractBundle: source, SourceArtifactFact: runtimeTestSourceArtifactFact(t, source)})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc := newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerLifecycleRoles(bus, pg),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
	constructExternalRuntimeTestRoot(t, ctx, manager, source)
	bus.SetInterceptors(pc)

	spinup := eventtest.ExistingRunRootIngress(
		"99999999-9999-4999-8999-999999999910",
		events.EventType("opco.spinup_requested"),
		"test-producer",
		"",
		[]byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-4222-8222-222222222222"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, spinup); err != nil {
		t.Fatalf("Publish spinup: %v", err)
	}
	select {
	case err := <-activationResults:
		if err != nil {
			t.Fatalf("finalize committed activation: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for committed activation finalization")
	}
	portfolioNodeID := templateInstanceRootNodeID(t, "portfolio-node")
	lifecycleNodeID := templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator")
	workflowRuntimeNodeID := templateInstanceRootNodeID(t, "workflow-runtime")
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, spinup.ID(), portfolioNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, spinup.ID(), portfolioNodeID)
	})
	assertRuntimeStorageCount(t, "CountTemplateNodeDeliveryChronology", 1, func() (int, error) {
		return storetest.CountTemplateNodeDeliveryChronology(ctx, pg, spinup.ID(), portfolioNodeID)
	})
	assertRuntimeCommittedReplayScope(t, ctx, pg, spinup.ID())
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 0, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, spinup.ID(), workflowRuntimeNodeID)
	})
	instancePath := requireRuntimeInitializedOperatingPath(t, ctx, pg, source)
	autoEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, instancePath+"/opco.product_initialization_requested")
	})
	assertRuntimeEventPayloadInitializationConfig(t, ctx, pg, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, autoEventID, lifecycleNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, autoEventID, lifecycleNodeID)
	})
	assertRuntimeCommittedReplayScope(t, ctx, pg, autoEventID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 0, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, autoEventID, workflowRuntimeNodeID)
	})
	componentEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, instancePath+"/component_scaffold.spawn_requested")
	})
	assertRuntimeEventPayloadProductOnly(t, ctx, pg, componentEventID)
	if got := activationCalls.Load(); got != 1 {
		t.Fatalf("flow activation calls = %d, want one successful activation", got)
	}
}

func TestTemplateInstanceActivationConfigSubscriberPersistsRenderedRouteAndDeliveryRows(t *testing.T) {
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyTemplateInstanceActivationConfigSubscriber(t))
	source := semanticview.Wrap(bundle)
	pg := storetest.StartPostgresRuntimeStore(t)
	ctx := seedRuntimeTestRunForSource(t, pg, source)
	durable := externalRuntimeTestDurableDependencies(pg)
	var manager *runtimemanager.AgentManager
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, errors.New("agent manager is required")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return errors.New("agent manager is required")
			}
			return manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),
		ContractBundle: source, SourceArtifactFact: runtimeTestSourceArtifactFact(t, source), Durable: durable})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc := newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerLifecycleRoles(bus, pg),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}, pg))
	bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(manager.FinalizeCommittedAgentReadiness))
	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
	constructExternalRuntimeTestRoot(t, ctx, manager, source)
	bus.SetInterceptors(pc)

	spinup := eventtest.ExistingRunRootIngress(
		"99999999-9999-4999-8999-999999999930",
		events.EventType("opco.spinup_requested"),
		"test-producer",
		"",
		[]byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-4222-8222-222222222222"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, spinup); err != nil {
		t.Fatalf("Publish spinup: %v", err)
	}
	instancePath := requireRuntimeInitializedOperatingPath(t, ctx, pg, source)
	autoEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, instancePath+"/opco.product_initialization_requested")
	})

	renderedAgentID := "ceo"
	scope, err := runtimepipeline.NewFlowInstanceLookupScope(source, runtimeTestSourceArtifactFact(t, source), templateInstanceDeliveryRunID, []string{"operating"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	instances, err := pg.ListFlowInstances(ctx, scope)
	if err != nil || len(instances) != 1 || instances[0].Identity().InstancePath != instancePath {
		t.Fatalf("constructed operating inventory = %+v, err=%v, want one exact receiver %s", instances, err, instancePath)
	}
	readiness, found := instances[0].Readiness()
	if !found || len(readiness.Plan.Agents) != 1 || readiness.Plan.Agents[0].Identity.AgentID() != renderedAgentID {
		t.Fatalf("constructed agent expectations = %+v, found=%t, want only %s", readiness.Plan.Agents, found, renderedAgentID)
	}
	waitRuntimeStorageCount(t, ctx, pg, "CountTemplateAgentDelivery", 1, func() (int, error) {
		return storetest.CountTemplateAgentDelivery(ctx, pg, autoEventID, renderedAgentID)
	})
	assertRuntimeStorageCount(t, "CountTemplatePlaceholderDelivery", 0, func() (int, error) {
		return storetest.CountTemplatePlaceholderDelivery(ctx, pg, autoEventID)
	})
}

func TestTemplateInstanceConnectLifecyclePublishRollbackDoesNotLeakInstanceOrRoute(t *testing.T) {
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyTemplateConnectRollback(t))
	source := semanticview.Wrap(bundle)
	pg := storetest.StartPostgresRuntimeStore(t)
	ctx := seedRuntimeTestRunForSource(t, pg, source)
	fault, err := storetest.NewPublicationFailure(pg, storetest.PublicationAfterMaterialization, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := fault.InstallForTest(ctx); err != nil {
		t.Fatal(err)
	}
	var manager *runtimemanager.AgentManager
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle:     source,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, errors.New("agent manager is required")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return errors.New("agent manager is required")
			}
			return manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc := newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              newRuntimeTestWorkflowModule(t, source),
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		DeliveryStore:       pg,
		PipelineObligations: pg.PipelineObligations(),
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerLifecycleRoles(bus, pg),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg), ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
	producer := seedRuntimeTestKeylessSource(t, ctx, pg, pc, "producer")
	assertRuntimeTestConstructedInstance(t, ctx, pg, source, producer)
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, pg)
	if err != nil {
		t.Fatal(err)
	}
	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		"99999999-9999-4999-8999-999999999940",
		events.EventType("producer/deploy.done"),
		"producer",
		"",
		[]byte(`{"vertical_id":"v-1"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EventEnvelope{},
		eventtest.StaticFlowRoutingSource(producer.TemplateID, producer.InstancePath, producer.EntityID),
		time.Now().UTC(),
	)

	err = bus.Publish(ctx, evt)
	if err == nil || !strings.Contains(err.Error(), "injected delivery route persistence failure") {
		t.Fatalf("Publish error = %v, want injected delivery route persistence failure", err)
	}
	assertRuntimeStorageCount(t, "CountTemplateEventIdentity", 0, func() (int, error) {
		return storetest.CountTemplateEventIdentity(ctx, pg, evt.ID())
	})
	assertRuntimeStorageCount(t, "CountTemplateEventDeliveries", 0, func() (int, error) {
		return storetest.CountTemplateEventDeliveries(ctx, pg, evt.ID())
	})
	assertRuntimeStorageCount(t, "CountTemplateInstances", 0, func() (int, error) {
		return storetest.CountTemplateInstances(ctx, pg, "consumer")
	})
	assertRuntimeStorageCount(t, "CountPhysicalFlowInstances", 2, func() (int, error) {
		snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, pg)
		return len(snapshot["flow_instances"].Rows), err
	})
	assertRuntimeStorageCount(t, "CountPhysicalEntities", 0, func() (int, error) {
		return storetest.CountPhysicalEntities(ctx, pg)
	})
	scope, err := runtimepipeline.NewFlowInstanceLookupScope(source, runtimeTestSourceArtifactFact(t, source), templateInstanceDeliveryRunID, []string{"consumer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	instances, err := pg.ListFlowInstances(ctx, scope)
	if err != nil || len(instances) != 0 {
		t.Fatalf("failed publication exposed a constructed consumer: %+v err=%v", instances, err)
	}
	assertRuntimeTestConstructedInstance(t, ctx, pg, source, producer)
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, pg)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed publication changed the prepared source tree or target storage: %v", err)
	}
}

func TestTemplateInstanceAcknowledgedPublishDispatchesRoutedSystemNodeWithoutInternalCarrierAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyTemplateInstanceEmpireOutbox(t))
	source := semanticview.Wrap(bundle)
	pg := storetest.StartPostgresRuntimeStore(t)
	ctx := seedRuntimeTestRunForSource(t, pg, source)
	var pc *runtimepipeline.PipelineCoordinator
	var manager *runtimemanager.AgentManager
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, errors.New("agent manager is required")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return errors.New("agent manager is required")
			}
			return manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),

		ContractBundle:     source,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc = newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerLifecycleRoles(bus, pg),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}))

	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
	constructExternalRuntimeTestRoot(t, ctx, manager, source)
	mailbox := eventtest.ExistingRunRootIngress(
		"99999999-9999-4999-8999-999999999913",
		events.EventType("approval.completed"),
		"approval-source",
		"",
		[]byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EventEnvelope{EntityID: "22222222-2222-4222-8222-222222222222"},
		time.Now().UTC())

	if err := bus.PublishAcknowledged(ctx, mailbox); err != nil {
		t.Fatalf("PublishAcknowledged mailbox: %v", err)
	}

	approvalNodeID := templateInstanceRootNodeID(t, "approval-router")
	portfolioNodeID := templateInstanceRootNodeID(t, "portfolio-node")
	lifecycleNodeID := templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator")
	workflowRuntimeNodeID := templateInstanceRootNodeID(t, "workflow-runtime")
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, mailbox.ID(), approvalNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, mailbox.ID(), approvalNodeID)
	})
	assertRuntimeStorageCount(t, "CountTemplateNodeDeliveryChronology", 1, func() (int, error) {
		return storetest.CountTemplateNodeDeliveryChronology(ctx, pg, mailbox.ID(), approvalNodeID)
	})
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 0, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, mailbox.ID(), workflowRuntimeNodeID)
	})

	spinupEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, "opco.spinup_requested")
	})
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, spinupEventID, portfolioNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, spinupEventID, portfolioNodeID)
	})
	assertRuntimeStorageCount(t, "CountTemplateNodeDeliveryChronology", 1, func() (int, error) {
		return storetest.CountTemplateNodeDeliveryChronology(ctx, pg, spinupEventID, portfolioNodeID)
	})
	assertRuntimeCommittedReplayScope(t, ctx, pg, spinupEventID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 0, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, spinupEventID, workflowRuntimeNodeID)
	})

	instancePath := requireRuntimeInitializedOperatingPath(t, ctx, pg, source)
	autoEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, instancePath+"/opco.product_initialization_requested")
	})
	assertRuntimeEventPayloadInitializationConfig(t, ctx, pg, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, autoEventID, lifecycleNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, autoEventID, lifecycleNodeID)
	})
	assertRuntimeCommittedReplayScope(t, ctx, pg, autoEventID)
	componentEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, instancePath+"/component_scaffold.spawn_requested")
	})
	assertRuntimeEventPayloadProductOnly(t, ctx, pg, componentEventID)
}

func TestTemplateInstanceRootOutboxEventDispatchesRoutedSystemNodeAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyTemplateInstanceEmpireOutbox(t))
	source := semanticview.Wrap(bundle)
	pg := storetest.StartPostgresRuntimeStore(t)
	ctx := seedRuntimeTestRunForSource(t, pg, source)
	var pc *runtimepipeline.PipelineCoordinator
	var manager *runtimemanager.AgentManager
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, errors.New("agent manager is required")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return errors.New("agent manager is required")
			}
			return manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),

		ContractBundle:     source,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc = newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerLifecycleRoles(bus, pg),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
	constructExternalRuntimeTestRoot(t, ctx, manager, source)

	mailbox := eventtest.ExistingRunRootIngress(
		"99999999-9999-4999-8999-999999999912",
		events.EventType("approval.completed"),
		"approval-source",
		"",
		[]byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-4222-8222-222222222222"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, mailbox); err != nil {
		t.Fatalf("Publish mailbox: %v", err)
	}

	approvalNodeID := templateInstanceRootNodeID(t, "approval-router")
	portfolioNodeID := templateInstanceRootNodeID(t, "portfolio-node")
	lifecycleNodeID := templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator")
	workflowRuntimeNodeID := templateInstanceRootNodeID(t, "workflow-runtime")
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, mailbox.ID(), approvalNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, mailbox.ID(), approvalNodeID)
	})

	spinupEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, "opco.spinup_requested")
	})
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, spinupEventID, portfolioNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, spinupEventID, portfolioNodeID)
	})
	assertRuntimeCommittedReplayScope(t, ctx, pg, spinupEventID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 0, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, spinupEventID, workflowRuntimeNodeID)
	})

	instancePath := requireRuntimeInitializedOperatingPath(t, ctx, pg, source)
	autoEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, instancePath+"/opco.product_initialization_requested")
	})
	assertRuntimeEventPayloadInitializationConfig(t, ctx, pg, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, autoEventID, lifecycleNodeID)
	assertRuntimeStorageCount(t, "CountTemplateNodeDelivery", 1, func() (int, error) {
		return storetest.CountTemplateNodeDelivery(ctx, pg, autoEventID, lifecycleNodeID)
	})
	assertRuntimeCommittedReplayScope(t, ctx, pg, autoEventID)
	componentEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateNamedEventIdentity", func() (string, error) {
		return storetest.ReadTemplateNamedEventIdentity(ctx, pg, instancePath+"/component_scaffold.spawn_requested")
	})
	assertRuntimeEventPayloadProductOnly(t, ctx, pg, componentEventID)
}

type runtimeTestWorkflowModule struct {
	source       semanticview.Source
	workflowNode []runtimepipeline.WorkflowNode
	guards       runtimepipeline.GuardRegistry
}

func newRuntimeTestWorkflowModule(t *testing.T, source semanticview.Source) runtimepipeline.WorkflowModule {
	t.Helper()
	if bundle, ok := semanticview.Bundle(source); ok && bundle != nil && bundle.PackInventory != nil && bundle.PackAdmission == nil {
		projection, err := packadmission.Admit(bundle.PackInventory, bundle.Platform)
		if err != nil {
			t.Fatalf("admit runtime test pack projection: %v", err)
		}
		bundle.PackAdmission = projection
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return &runtimeTestWorkflowModule{
		source:       source,
		workflowNode: nodes,
		guards:       runtimepipeline.NewContractGuardRegistry(source),
	}
}

func (m *runtimeTestWorkflowModule) SemanticSource() semanticview.Source { return m.source }

func (m *runtimeTestWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.workflowNode...)
}
func (m *runtimeTestWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}
func loadRuntimeTempBundle(t *testing.T, files map[string]string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repoRoot,
		root,
		platformSpec,
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
	if err != nil {
		t.Fatalf("load temp bundle: %v", err)
	}
	return bundle
}

func loadRuntimeBundleRoot(t *testing.T, root string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
	if err != nil {
		t.Fatalf("load canonical routing bundle: %v", err)
	}
	return bundle
}

func templateInstanceDeliveryFixtureFiles() map[string]string {
	return map[string]string{
		"schema.yaml": "name: test\n",
		"operating/schema.yaml": `name: operating
instance: instance_id
stages:
  initializing: {}
  ready: {final: true}
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"operating/entities.yaml": "operating_state:\n  instance_id: string\n",
		"operating/events.yaml": `opco.product_initialization_requested:
  entity_id: string?
opco.ceo_ready:
  entity_id: string?
`,
		"operating/nodes.yaml": `lifecycle-orchestrator:
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [opco.ceo_ready]
  event_handlers:
    opco.product_initialization_requested:
      emit: opco.ceo_ready
`,
	}
}

func runtimeTestSourceArtifactFact(t testing.TB, source semanticview.Source) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("native component fixture requires its admitted source artifact")
	}
	return sourceartifactfixture.FactFor(bundle.SourceArtifact)
}

func seedRuntimeTestRunForSource(t *testing.T, selected *store.PostgresStore, source semanticview.Source) context.Context {
	t.Helper()
	fact := runtimeTestSourceArtifactFact(t, source)
	ctx := runtimecorrelation.WithRunID(runtimecorrelation.WithSourceArtifactFact(context.Background(), fact), templateInstanceDeliveryRunID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
	bundle, _ := semanticview.Bundle(source)
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: templateInstanceDeliveryRunID, Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash()})
	return ctx
}

func waitRuntimeStorageCount(t *testing.T, ctx context.Context, selected any, observation string, want int, read func() (int, error)) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := read()
		if err != nil {
			t.Fatalf("query count: %v", err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("count = %d, want %d for observation %s\n%s", got, want, observation, runtimeTestEventDiagnostics(ctx, selected))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitRuntimeStoredEventID(t *testing.T, ctx context.Context, selected any, observation string, read func() (string, error)) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := read()
		if err == nil && got != "" {
			return got
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("query event id: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for event id from observation %s\n%s", observation, runtimeTestEventDiagnostics(ctx, selected))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func runtimeTestEventDiagnostics(ctx context.Context, selected any) string {
	sections, err := storetest.ReadNotifyAllChildrenDiagnosticStorage(ctx, selected)
	if err != nil {
		return "event diagnostics unavailable: " + err.Error()
	}
	var out strings.Builder
	for _, section := range sections {
		if section.Failure != "" {
			fmt.Fprintf(&out, "diagnostics unavailable: %s\n", section.Failure)
			continue
		}
		for _, row := range section.Rows {
			fmt.Fprintf(&out, "%v: %v\n", section.Columns, row)
		}
	}
	return out.String()
}

func requireRuntimeInitializedOperatingPath(t *testing.T, ctx context.Context, selected any, source semanticview.Source) string {
	t.Helper()
	waitRuntimeStorageCount(t, ctx, selected, "run operating instances", 1, func() (int, error) {
		return storetest.CountRunOperatingInstances(ctx, selected, templateInstanceDeliveryRunID)
	})
	row, err := storetest.ReadRunOperatingInstanceFields(ctx, selected, templateInstanceDeliveryRunID)
	if err != nil {
		t.Fatal(err)
	}
	path, raw := row.Path, row.Fields
	assertRuntimeInitializationConfig(t, raw)
	plans := pinrouting.CompileConnectGraph(source).Plans()
	if len(plans) != 1 || plans[0].InstanceKey() == nil {
		t.Fatalf("fixture must declare exactly one keyed creation plan: %v", plans)
	}
	digest := plans[0].ReceiverKeyDigest([]runtimecontracts.TemplateInstanceKeyValue{{
		Field: plans[0].InstanceKey().Field(), Value: "11111111-1111-4111-8111-111111111111",
	}})
	if len(digest) < 24 {
		t.Fatalf("invalid receiver key digest %q", digest)
	}
	want := plans[0].DeriveReceiverIdentity(source, "ti-"+digest[:24]).InstancePath
	if path != want {
		t.Fatalf("created instance path = %q, want exact key-derived identity %q", path, want)
	}
	return path
}

func assertRuntimeEventPayloadInitializationConfig(t *testing.T, ctx context.Context, selected any, eventID string) {
	t.Helper()
	raw, err := storetest.ReadTemplateEventPayloadText(ctx, selected, eventID)
	if err != nil {
		t.Fatal(err)
	}
	assertRuntimeInitializationConfig(t, raw)
}

func assertRuntimeInitializationConfig(t *testing.T, raw string) {
	t.Helper()
	var config map[string]any
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		t.Fatal(err)
	}
	// The declared key is business state, not injected route metadata.
	if len(config) != 2 || config["instance_id"] != "11111111-1111-4111-8111-111111111111" || config["product_id"] != "product-1" {
		t.Fatalf("receiver config = %s, want exactly the authored instance_id and product_id", raw)
	}
}

func assertRuntimeEventPayloadProductOnly(t *testing.T, ctx context.Context, selected any, eventID string) {
	t.Helper()
	raw, err := storetest.ReadTemplateEventPayloadText(ctx, selected, eventID)
	if err != nil {
		t.Fatalf("query event payload: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode event payload %s: %v", eventID, err)
	}
	if got := payload["product_id"]; got != "product-1" {
		t.Fatalf("payload product_id = %#v, want product-1: %#v", got, payload)
	}
	for _, key := range []string{"instance_id", "template_id", "flow_path", "parent_entity_id"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("payload includes hidden activation context %q: %#v", key, payload)
		}
	}
}

func assertRuntimeStorageCount(t *testing.T, observation string, want int, read func() (int, error)) {
	t.Helper()
	got, err := read()
	if err != nil {
		t.Fatalf("query count: %v", err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d for observation %s", got, want, observation)
	}
}

func waitRuntimeNodeDeliveryOutcome(t *testing.T, ctx context.Context, selected any, eventID, nodeID string) {
	t.Helper()
	waitRuntimeStorageCount(t, ctx, selected, "exact settled node attempt", 1, func() (int, error) {
		return storetest.CountTemplateNodeDeliveredAttempt(ctx, selected, eventID, nodeID)
	})
}

func assertRuntimeCommittedReplayScope(t *testing.T, ctx context.Context, selected any, eventID string) {
	t.Helper()
	assertRuntimeStorageCount(t, "subscribed replay scope", 1, func() (int, error) { return storetest.CountTemplateSubscribedReplayScope(ctx, selected, eventID) })
}
