package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storetest "github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testsql"
	"github.com/division-sh/swarm/internal/testutil"
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
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle: source,
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
	pc = newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
		FlowRoutes:          bus,
	})
	seedTemplateInstanceDeliveryRouteOwner(t, ctx, db)

	if err := bus.AddFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.RunScopedFlowInstance{
		RunID: templateInstanceDeliveryRunID,
		Route: runtimeflowidentity.DeriveRoute("operating", "inst-1"),
	}}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := "99999999-9999-4999-8999-999999999902"
	evt := eventtest.ExistingRunRootIngress(
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
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	lifecycleNodeID := templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator")
	workflowRuntimeNodeID := templateInstanceRootNodeID(t, "workflow-runtime")
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, eventID, lifecycleNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 0, eventID, workflowRuntimeNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, eventID, lifecycleNodeID)
	assertRuntimeCommittedReplayScope(t, ctx, db, eventID)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM events
		WHERE event_name = 'operating/inst-1/opco.ceo_ready'
	`, 1)
}

func TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsAuthorityBeforeHandlerExecution(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceDeliveryFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedTemplateInstanceDeliveryRouteOwner(t, ctx, db)
	if err := bus.AddFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.RunScopedFlowInstance{
		RunID: templateInstanceDeliveryRunID,
		Route: runtimeflowidentity.DeriveRoute("operating", "inst-1"),
	}}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := runtimeInternalDeliveriesForTest(t, bus, "workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	eventID := "99999999-9999-4999-8999-999999999903"
	evt := eventtest.ExistingRunRootIngress(
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
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-ch:
		defer func() { _ = got.Complete() }()
		if got.FlowInstance() != "operating/inst-1" || got.EntityID() != "" {
			t.Fatalf("delivered route identity flow=%q entity=%q, want explicit entityless operating/inst-1 receiver", got.FlowInstance(), got.EntityID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workflow-runtime carrier did not receive concrete template event")
	}
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, eventID, templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator"))
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 0, eventID, templateInstanceRootNodeID(t, "workflow-runtime"))
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 0, eventID, templateInstanceFlowNodeID(t, "operating", "lifecycle-orchestrator"))
}

func seedTemplateInstanceDeliveryRouteOwner(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()
	// This scenario proves entityless system-node routing, so it establishes the
	// exact route lifecycle owner without introducing an entity-state target.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (
			run_id, instance_path, flow_template, mode, config, status, created_at
		) VALUES ($1::uuid, $2, $3, $4, '{}'::jsonb, $5, $6)
	`, templateInstanceDeliveryRunID, "operating/inst-1", "operating", "template", "active", time.Now().UTC()); err != nil {
		t.Fatalf("seed run-scoped template route owner: %v", err)
	}
}

func TestTemplateInstanceAutoEmitDispatchesLocalHandlerAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceEmpireStyleFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
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
		ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc := newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
		FlowRoutes:          bus,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: authorActivityTestSourceArtifactFact,
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerBusRoles(bus),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
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
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, spinup.ID(), portfolioNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, spinup.ID(), portfolioNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
		  AND settled_at IS NOT NULL AND created_at < settled_at
	`, 1, spinup.ID(), portfolioNodeID)
	assertRuntimeCommittedReplayScope(t, ctx, db, spinup.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 0, spinup.ID(), workflowRuntimeNodeID)
	instancePath := requireRuntimeInitializedOperatingPath(t, ctx, db, source)
	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = $1
	`, []any{instancePath + "/opco.product_initialization_requested"})
	assertRuntimeEventPayloadInitializationConfig(t, ctx, db, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, autoEventID, lifecycleNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, autoEventID, lifecycleNodeID)
	assertRuntimeCommittedReplayScope(t, ctx, db, autoEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 0, autoEventID, workflowRuntimeNodeID)
	componentEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = $1
	`, []any{instancePath + "/component_scaffold.spawn_requested"})
	assertRuntimeEventPayloadProductOnly(t, ctx, db, componentEventID)
	if got := activationCalls.Load(); got != 1 {
		t.Fatalf("flow activation calls = %d, want one successful activation", got)
	}
}

func TestTemplateInstanceActivationConfigSubscriberPersistsRenderedRouteAndDeliveryRows(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceActivationConfigSubscriberFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	proofStore := routeMaterializationDBProofStore{pg: pg}
	durable := externalRuntimeTestDurableDependencies(pg)
	durable.FlowRoutes = proofStore
	durable.FlowRouteSets = proofStore
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
		ContractBundle: source, Durable: durable})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc := newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
		FlowRoutes:          bus,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: authorActivityTestSourceArtifactFact,
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerBusRoles(bus),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}, pg))
	bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(manager.FinalizeCommittedAgentReadiness))
	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
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
	instancePath := requireRuntimeInitializedOperatingPath(t, ctx, db, source)
	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = $1
	`, []any{instancePath + "/opco.product_initialization_requested"})

	renderedAgentID := "ceo"
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM routing_rules
		WHERE flow_instance = $2
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $1
		  AND status = 'active'
	`, 1, renderedAgentID, instancePath)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM routing_rules
		WHERE flow_instance = $1
		  AND subscriber_id = 'ceo-{product_id}'
	`, 0, instancePath)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, 1, autoEventID, renderedAgentID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_id = 'ceo-{product_id}'
	`, 0, autoEventID)
}

func TestTemplateInstanceConnectLifecyclePublishRollbackDoesNotLeakInstanceOrRoute(t *testing.T) {
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyTemplateConnectRollback(t))
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	testsql.InstallPostgresEventDeliveryFailureAfterFlowMaterialization(t, ctx, db, testsql.EventCorruptionClaim{
		Invariant: "store.event_record.named_operation_atomicity",
		Reason:    "prove late delivery failure rolls back the event and connect-created lifecycle facts",
	}, "consumer")
	var manager *runtimemanager.AgentManager
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle: source,
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
	pc := newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              newRuntimeTestWorkflowModule(t, source),
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		DeliveryStore:       pg,
		PipelineObligations: pg.PipelineObligations(),
		FlowRoutes:          bus,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: authorActivityTestSourceArtifactFact,
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerBusRoles(bus),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg), ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		"99999999-9999-4999-8999-999999999940",
		events.EventType("producer/deploy.done"),
		"producer",
		"",
		[]byte(`{"vertical_id":"v-1"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EventEnvelope{},
		eventtest.StaticFlowRoutingSource("producer", "producer", eventtest.UUID("template-connect-rollback-producer")),
		time.Now().UTC(),
	)

	err = bus.Publish(ctx, evt)
	if err == nil || !strings.Contains(err.Error(), "injected delivery route persistence failure") {
		t.Fatalf("Publish error = %v, want injected delivery route persistence failure", err)
	}
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM events
		WHERE event_id = $1::uuid
	`, 0, evt.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
	`, 0, evt.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM flow_instances
		WHERE flow_template = 'consumer'
	`, 0)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM entity_state
	`, 0)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM routing_rules
	`, 0)
}

func TestTemplateInstanceAcknowledgedPublishDispatchesRoutedSystemNodeWithoutInternalCarrierAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyTemplateInstanceEmpireOutbox(t))
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
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

		ContractBundle: source,
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
	pc = newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
		FlowRoutes:          bus,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: authorActivityTestSourceArtifactFact,
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerBusRoles(bus),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}))

	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
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
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, mailbox.ID(), approvalNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, mailbox.ID(), approvalNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
		  AND settled_at IS NOT NULL AND created_at < settled_at
	`, 1, mailbox.ID(), approvalNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 0, mailbox.ID(), workflowRuntimeNodeID)

	spinupEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'opco.spinup_requested'
	`, nil)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, spinupEventID, portfolioNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, spinupEventID, portfolioNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
		  AND settled_at IS NOT NULL AND created_at < settled_at
	`, 1, spinupEventID, portfolioNodeID)
	assertRuntimeCommittedReplayScope(t, ctx, db, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 0, spinupEventID, workflowRuntimeNodeID)

	instancePath := requireRuntimeInitializedOperatingPath(t, ctx, db, source)
	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = $1
	`, []any{instancePath + "/opco.product_initialization_requested"})
	assertRuntimeEventPayloadInitializationConfig(t, ctx, db, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, autoEventID, lifecycleNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, autoEventID, lifecycleNodeID)
	assertRuntimeCommittedReplayScope(t, ctx, db, autoEventID)
	componentEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = $1
	`, []any{instancePath + "/component_scaffold.spawn_requested"})
	assertRuntimeEventPayloadProductOnly(t, ctx, db, componentEventID)
}

func TestTemplateInstanceRootOutboxEventDispatchesRoutedSystemNodeAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyTemplateInstanceEmpireOutbox(t))
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
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

		ContractBundle: source,
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
	pc = newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
		FlowRoutes:          bus,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		BaseContext:        ctx,
		SourceArtifactFact: authorActivityTestSourceArtifactFact,
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerBusRoles(bus),
		LifecycleStore:     storetest.AgentLifecycleFixture(t, pg),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)

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
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, mailbox.ID(), approvalNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, mailbox.ID(), approvalNodeID)

	spinupEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'opco.spinup_requested'
	`, nil)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, spinupEventID, portfolioNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, spinupEventID, portfolioNodeID)
	assertRuntimeCommittedReplayScope(t, ctx, db, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 0, spinupEventID, workflowRuntimeNodeID)

	instancePath := requireRuntimeInitializedOperatingPath(t, ctx, db, source)
	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = $1
	`, []any{instancePath + "/opco.product_initialization_requested"})
	assertRuntimeEventPayloadInitializationConfig(t, ctx, db, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, autoEventID, lifecycleNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, 1, autoEventID, lifecycleNodeID)
	assertRuntimeCommittedReplayScope(t, ctx, db, autoEventID)
	componentEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = $1
	`, []any{instancePath + "/component_scaffold.spawn_requested"})
	assertRuntimeEventPayloadProductOnly(t, ctx, db, componentEventID)
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
mode: template
instance: instance_id
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
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

func templateInstanceEmpireStyleFixtureFiles() map[string]string {
	return map[string]string{
		"schema.yaml": `name: test
pins:
  outputs:
    events: [opco.spinup_created]
connect:
  - event: opco.spinup_created
    from: .
    to: operating
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string?
  instance_id: string
  product_id: string
opco.spinup_created:
  instance_id: string
  product_id: string
`,
		"nodes.yaml": `portfolio-node:
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  produces: [opco.spinup_created]
  event_handlers:
    opco.spinup_requested:
      emit:
        event: opco.spinup_created
        fields:
          instance_id: payload.instance_id
          product_id: payload.product_id
`,
		"operating/schema.yaml": `name: operating
mode: template
instance: instance_id
instance_variables:
  variables:
    product_id: string
pins:
  inputs:
    events:
      - event: opco.spinup_created
        resolution: {mode: create}
        initialize:
          product_id: payload.product_id
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"operating/entities.yaml": "operating_state:\n  instance_id: string\n",
		"operating/events.yaml": `opco.product_initialization_requested:
  instance_id: string
  product_id: string
component_scaffold.spawn_requested:
  product_id: string
`,
		"operating/nodes.yaml": `lifecycle-orchestrator:
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      advances_to: ready
      emit:
        event: component_scaffold.spawn_requested
        fields:
          product_id: payload.product_id
`,
	}
}

func templateInstanceActivationConfigSubscriberFixtureFiles() map[string]string {
	return map[string]string{
		"schema.yaml": `name: test
pins:
  outputs:
    events: [opco.spinup_created]
connect:
  - event: opco.spinup_created
    from: .
    to: operating
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string?
  instance_id: string
  product_id: string
opco.spinup_created:
  instance_id: string
  product_id: string
`,
		"nodes.yaml": `portfolio-node:
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  produces: [opco.spinup_created]
  event_handlers:
    opco.spinup_requested:
      emit:
        event: opco.spinup_created
        fields:
          instance_id: payload.instance_id
          product_id: payload.product_id
`,
		"operating/schema.yaml": `name: operating
mode: template
instance: instance_id
instance_variables:
  variables:
    product_id: string
pins:
  inputs:
    events:
      - event: opco.spinup_created
        resolution: {mode: create}
        initialize:
          product_id: payload.product_id
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"operating/entities.yaml": "operating_state:\n  instance_id: string\n",
		"operating/events.yaml": `opco.product_initialization_requested:
  instance_id: string?
  template_id: string?
  flow_path: string?
  parent_entity_id: string?
  product_id: string?
`,
		"operating/agents.yaml": `ceo:
  type: generic
  role: ceo
  intent: {inline: "Initialize the product for this operating instance."}
  model: regular
  subscriptions: [opco.product_initialization_requested]
`,
	}
}

type routeMaterializationDBProofStore struct {
	pg *store.PostgresStore
}

func (s routeMaterializationDBProofStore) PipelineObligations() runtimepipelineobligation.Store {
	return s.pg.PipelineObligations()
}

func (s routeMaterializationDBProofStore) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return s.pg.RegisterAuthorActivityEventCatalog(scope, descriptors)
}

func (s routeMaterializationDBProofStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return s.pg.CommitPublication(ctx, command)
}

func (s routeMaterializationDBProofStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	return s.pg.ListEventDeliveryRecipients(ctx, eventID)
}

func (s routeMaterializationDBProofStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	return s.pg.UpsertFlowInstanceRoute(ctx, route)
}

func (s routeMaterializationDBProofStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.RunScopedFlowInstance,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	return s.pg.ReplaceFlowInstanceRouteRecords(ctx, identity, routes)
}

func (s routeMaterializationDBProofStore) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	return s.pg.DeleteFlowInstanceRoute(ctx, identity)
}

func (s routeMaterializationDBProofStore) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.RunScopedFlowInstance, error) {
	return s.pg.ListFlowInstanceRoutes(ctx)
}

func (s routeMaterializationDBProofStore) ListFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) ([]runtimebus.FlowInstanceRouteRecord, error) {
	return s.pg.ListFlowInstanceRouteRecords(ctx, identity)
}

func (s routeMaterializationDBProofStore) ListActiveFlowInstanceDescriptors(ctx context.Context, runID string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return s.pg.ListActiveFlowInstanceDescriptors(ctx, runID)
}

func seedRuntimeTestRun(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
	storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: templateInstanceDeliveryRunID})
	return ctx
}

func waitRuntimeDBCount(t *testing.T, ctx context.Context, db *sql.DB, query string, want int, args ...any) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got int
		if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
			t.Fatalf("query count: %v", err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("count = %d, want %d for query %s\n%s", got, want, strings.TrimSpace(query), runtimeTestEventDiagnostics(ctx, db))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitRuntimeEventID(t *testing.T, ctx context.Context, db *sql.DB, query string, args []any) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got string
		err := db.QueryRowContext(ctx, query, args...).Scan(&got)
		if err == nil && got != "" {
			return got
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("query event id: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for event id from query %s\n%s", strings.TrimSpace(query), runtimeTestEventDiagnostics(ctx, db))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func runtimeTestEventDiagnostics(ctx context.Context, db *sql.DB) string {
	rows, err := db.QueryContext(ctx, `
		SELECT e.event_name, COALESCE(e.produced_by, ''), COALESCE(r.subscriber_id, ''),
		       COALESCE(r.outcome, ''), COALESCE(r.reason_code, ''), COALESCE(r.side_effects::text, '')
		FROM events e
		LEFT JOIN event_receipts r ON r.event_id = e.event_id
		ORDER BY e.created_at, e.event_id, r.subscriber_id
	`)
	if err != nil {
		return "event diagnostics unavailable: " + err.Error()
	}
	defer rows.Close()
	var out strings.Builder
	for rows.Next() {
		var eventType, producer, subscriber, outcome, reason, sideEffects string
		if err := rows.Scan(&eventType, &producer, &subscriber, &outcome, &reason, &sideEffects); err != nil {
			return "event diagnostics scan: " + err.Error()
		}
		fmt.Fprintf(&out, "event=%s producer=%s subscriber=%s outcome=%s reason=%s side_effects=%s\n", eventType, producer, subscriber, outcome, reason, sideEffects)
	}
	deliveryRows, err := db.QueryContext(ctx, `
		SELECT e.event_name, d.subscriber_type, d.subscriber_id, d.status
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		ORDER BY e.created_at, d.subscriber_type, d.subscriber_id
	`)
	if err == nil {
		defer deliveryRows.Close()
		for deliveryRows.Next() {
			var eventType, subscriberType, subscriberID, status string
			if err := deliveryRows.Scan(&eventType, &subscriberType, &subscriberID, &status); err != nil {
				break
			}
			fmt.Fprintf(&out, "delivery event=%s subscriber=%s[%s] status=%s\n", eventType, subscriberType, subscriberID, status)
		}
	}
	instanceRows, err := db.QueryContext(ctx, `SELECT flow_template, instance_path, status FROM flow_instances ORDER BY created_at, run_id, instance_path`)
	if err == nil {
		defer instanceRows.Close()
		for instanceRows.Next() {
			var template, instance, status string
			if err := instanceRows.Scan(&template, &instance, &status); err != nil {
				break
			}
			fmt.Fprintf(&out, "flow_instance=%s template=%s status=%s\n", instance, template, status)
		}
	}
	deadLetterRows, err := db.QueryContext(ctx, `
		SELECT COALESCE(original_event, ''), COALESCE(handler_node, ''), COALESCE(failure::text, '')
		FROM dead_letters
		ORDER BY created_at, dead_letter_id
	`)
	if err == nil {
		defer deadLetterRows.Close()
		for deadLetterRows.Next() {
			var eventType, handler, failure string
			if err := deadLetterRows.Scan(&eventType, &handler, &failure); err != nil {
				break
			}
			fmt.Fprintf(&out, "dead_letter event=%s handler=%s failure=%s\n", eventType, handler, failure)
		}
	}
	return out.String()
}

func requireRuntimeInitializedOperatingPath(t *testing.T, ctx context.Context, db *sql.DB, source semanticview.Source) string {
	t.Helper()
	waitRuntimeDBCount(t, ctx, db, `SELECT COUNT(*) FROM flow_instances WHERE run_id = $1::uuid AND flow_template = 'operating'`, 1, templateInstanceDeliveryRunID)
	var path, raw string
	if err := db.QueryRowContext(ctx, `SELECT instance_path, (config->'config')::text FROM flow_instances WHERE run_id = $1::uuid AND flow_template = 'operating'`, templateInstanceDeliveryRunID).Scan(&path, &raw); err != nil {
		t.Fatal(err)
	}
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

func assertRuntimeEventPayloadInitializationConfig(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	var raw string
	if err := db.QueryRowContext(ctx, `SELECT payload::text FROM events WHERE event_id = $1::uuid`, eventID).Scan(&raw); err != nil {
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
	// The declared key is business config, not injected route metadata.
	if len(config) != 2 || config["instance_id"] != "11111111-1111-4111-8111-111111111111" || config["product_id"] != "product-1" {
		t.Fatalf("receiver config = %s, want exactly the authored instance_id and product_id", raw)
	}
}

func assertRuntimeEventPayloadProductOnly(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	var raw string
	if err := db.QueryRowContext(ctx, `
		SELECT payload::text FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&raw); err != nil {
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

func assertRuntimeDBCount(t *testing.T, ctx context.Context, db *sql.DB, query string, want int, args ...any) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d for query %s", got, want, strings.TrimSpace(query))
	}
}

func waitRuntimeNodeDeliveryOutcome(t *testing.T, ctx context.Context, db *sql.DB, eventID, nodeID string) {
	t.Helper()
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*)
		FROM event_deliveries d
		JOIN event_delivery_outcomes o
		  ON o.delivery_id = d.delivery_id
		 AND o.claim_version = d.claim_version
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = $2
		  AND d.status = 'delivered'
		  AND d.settled_at IS NOT NULL
		  AND o.outcome = 'delivered'
	`, 1, eventID, nodeID)
}

func assertRuntimeCommittedReplayScope(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*)
		FROM committed_replay_scopes
		WHERE event_id = $1::uuid AND scope = 'subscribed'
	`, 1, eventID)
}
