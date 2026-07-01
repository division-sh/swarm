package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
)

const templateInstanceDeliveryRunID = "99999999-9999-4999-8999-999999999901"

func TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsReceiptAndReplayScopeSeparately(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceDeliveryFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module: module,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
	})
	subscribed := make(chan struct{}, 1)
	pc.SetTestSubscribeHook(func() { subscribed <- struct{}{} })
	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go pc.Run(runCtx)
	select {
	case <-subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow runtime did not subscribe")
	}
	if err := bus.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := "99999999-9999-4999-8999-999999999902"
	evt := eventtest.RootIngress(
		eventID,
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{"entity_id":"11111111-1111-4111-8111-111111111111"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-4111-8111-111111111111"), "operating/inst-1"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator' AND outcome = 'no_op'
	`, 1, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, eventID)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM events
		WHERE event_name = 'operating/opco.ceo_ready'
	`, 1)
}

func TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsAuthorityBeforeHandlerExecution(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceDeliveryFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := bus.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := bus.SubscribeInternal("workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	eventID := "99999999-9999-4999-8999-999999999903"
	evt := eventtest.RootIngress(
		eventID,
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{"entity_id":"11111111-1111-4111-8111-111111111111"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-4111-8111-111111111111"), "operating/inst-1"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-ch:
		if got.FlowInstance() != "operating/inst-1" || got.EntityID() != "11111111-1111-4111-8111-111111111111" {
			t.Fatalf("delivered route identity flow=%q entity=%q, want operating/inst-1 product entity", got.FlowInstance(), got.EntityID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workflow-runtime carrier did not receive concrete template event")
	}
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 0, eventID)
}

func TestTemplateInstanceAutoEmitDispatchesLocalHandlerAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceEmpireStyleFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
	})
	module := newRuntimeTestWorkflowModule(t, source)
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
	})
	bus.SetInterceptors(pc)

	spinup := eventtest.RootIngress(
		"99999999-9999-4999-8999-999999999910",
		events.EventType("opco.spinup_requested"),
		"",
		"",
		[]byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-4222-8222-222222222222"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, spinup); err != nil {
		t.Fatalf("Publish spinup: %v", err)
	}
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node' AND outcome = 'no_op'
	`, 1, spinup.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
	`, 1, spinup.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
		  AND delivered_at IS NOT NULL AND created_at < delivered_at
	`, 1, spinup.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, spinup.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, spinup.ID())
	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, autoEventID)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator' AND outcome = 'no_op'
	`, 1, autoEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, autoEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, autoEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, autoEventID)
	componentEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/component_scaffold.spawn_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, componentEventID)
}

func TestTemplateInstanceActivationConfigSubscriberPersistsRenderedRouteAndDeliveryRows(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceActivationConfigSubscriberFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := &store.PostgresStore{DB: db}
	proofStore := routeMaterializationDBProofStore{pg: pg}
	bus, err := runtimebus.NewEventBusWithOptions(proofStore, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
	})
	module := newRuntimeTestWorkflowModule(t, source)
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
	})
	bus.SetInterceptors(pc)

	spinup := eventtest.RootIngress(
		"99999999-9999-4999-8999-999999999930",
		events.EventType("opco.spinup_requested"),
		"",
		"",
		[]byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-4222-8222-222222222222"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, spinup); err != nil {
		t.Fatalf("Publish spinup: %v", err)
	}
	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested'
	`, nil)

	renderedAgentID := "ceo-product-1"
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM routing_rules
		WHERE flow_instance = 'operating/11111111-1111-4111-8111-111111111111'
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $1
		  AND status = 'active'
	`, 1, renderedAgentID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM routing_rules
		WHERE flow_instance = 'operating/11111111-1111-4111-8111-111111111111'
		  AND subscriber_id = 'ceo-{product_id}'
	`, 0)
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
	bundle := loadRuntimeTempBundle(t, templateInstanceConnectLifecycleRollbackFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := &store.PostgresStore{DB: db}
	proofStore := &failingDeliveryRouteStore{PostgresStore: pg}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	var manager *runtimemanager.AgentManager
	bus, err := runtimebus.NewEventBusWithOptions(proofStore, runtimebus.EventBusOptions{
		ContractBundle: source,
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return errors.New("agent manager is required")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	manager = runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
	})
	proofStore.failDeliveryRoutes = true
	evt := eventtest.RootIngress(
		"99999999-9999-4999-8999-999999999940",
		events.EventType("producer/deploy.done"),
		"",
		"",
		[]byte(`{"vertical_id":"v-1"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)

	err = bus.Publish(ctx, evt)
	if err == nil || !strings.Contains(err.Error(), "injected delivery route persistence failure") {
		t.Fatalf("Publish error = %v, want injected delivery route persistence failure", err)
	}
	if len(proofStore.descriptorsSeenDuringDelivery) != 1 {
		t.Fatalf("descriptors seen during delivery failure = %#v, want one lifecycle-created descriptor", proofStore.descriptorsSeenDuringDelivery)
	}
	descriptor := proofStore.descriptorsSeenDuringDelivery[0]
	if descriptor.FlowTemplate != "consumer" || descriptor.FlowInstance == "" {
		t.Fatalf("descriptor seen during delivery failure = %#v, want consumer flow instance", descriptor)
	}
	if descriptor.AddressFields["entity.vertical_id"] != "v-1" {
		t.Fatalf("descriptor address fields = %#v, want entity.vertical_id v-1", descriptor.AddressFields)
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
		WHERE instance_id = $1
	`, 0, descriptor.FlowInstance)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM entity_state
		WHERE flow_instance = $1
	`, 0, descriptor.FlowInstance)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM routing_rules
		WHERE flow_instance = $1
	`, 0, descriptor.FlowInstance)
	route := runtimeflowidentity.StoredRoute("", "", descriptor.FlowInstance)
	if got := bus.RouteTable().MaterializedRoutes(route); len(got) != 0 {
		t.Fatalf("route table materialized routes after rollback = %#v, want none", got)
	}
}

func TestTemplateInstanceAcknowledgedPublishDispatchesRoutedSystemNodeWithoutInternalCarrierAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceEmpireOutboxFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := &store.PostgresStore{DB: db}
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
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
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
	})
	module := newRuntimeTestWorkflowModule(t, source)
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
	})

	mailbox := eventtest.RootIngress(
		"99999999-9999-4999-8999-999999999913",
		events.EventType("mailbox.item_decided"),
		"",
		"",
		[]byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EventEnvelope{EntityID: "22222222-2222-4222-8222-222222222222"},
		time.Now().UTC())

	if err := bus.PublishAcknowledged(ctx, mailbox); err != nil {
		t.Fatalf("PublishAcknowledged mailbox: %v", err)
	}

	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router' AND outcome = 'no_op'
	`, 1, mailbox.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
	`, 1, mailbox.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
		  AND delivered_at IS NOT NULL AND created_at < delivered_at
	`, 1, mailbox.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, mailbox.ID())

	spinupEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'opco.spinup_requested'
	`, nil)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node' AND outcome = 'no_op'
	`, 1, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
	`, 1, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
		  AND delivered_at IS NOT NULL AND created_at < delivered_at
	`, 1, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, spinupEventID)

	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, autoEventID)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator' AND outcome = 'no_op'
	`, 1, autoEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, autoEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, autoEventID)
	componentEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/component_scaffold.spawn_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, componentEventID)
}

func TestTemplateInstanceRootOutboxEventDispatchesRoutedSystemNodeAndEmpireStyleSideEffect(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceEmpireOutboxFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
	})
	module := newRuntimeTestWorkflowModule(t, source)
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
	})
	subscribed := make(chan struct{}, 1)
	pc.SetTestSubscribeHook(func() { subscribed <- struct{}{} })
	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go pc.Run(runCtx)
	select {
	case <-subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow runtime did not subscribe")
	}

	mailbox := eventtest.RootIngress(
		"99999999-9999-4999-8999-999999999912",
		events.EventType("mailbox.item_decided"),
		"",
		"",
		[]byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-4222-8222-222222222222"),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, mailbox); err != nil {
		t.Fatalf("Publish mailbox: %v", err)
	}

	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router' AND outcome = 'no_op'
	`, 1, mailbox.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
	`, 1, mailbox.ID())

	spinupEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'opco.spinup_requested'
	`, nil)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node' AND outcome = 'no_op'
	`, 1, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
	`, 1, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, spinupEventID)

	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, autoEventID)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator' AND outcome = 'no_op'
	`, 1, autoEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, autoEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, autoEventID)
	componentEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/component_scaffold.spawn_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, componentEventID)
}

type runtimeTestWorkflowModule struct {
	source       semanticview.Source
	workflow     *runtimepipeline.WorkflowDefinition
	workflowNode []runtimepipeline.WorkflowNode
	guards       runtimepipeline.GuardRegistry
	actions      runtimepipeline.ActionRegistry
}

func newRuntimeTestWorkflowModule(t *testing.T, source semanticview.Source) runtimepipeline.WorkflowModule {
	t.Helper()
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return &runtimeTestWorkflowModule{
		source:       source,
		workflow:     workflow,
		workflowNode: nodes,
		guards:       runtimepipeline.NewContractGuardRegistry(source),
		actions:      runtimepipeline.NewContractActionRegistry(source),
	}
}

func (m *runtimeTestWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (m *runtimeTestWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}
func (m *runtimeTestWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.workflowNode...)
}
func (m *runtimeTestWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}
func (m *runtimeTestWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actions
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
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load temp bundle: %v", err)
	}
	return bundle
}

func templateInstanceDeliveryFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: operating
    flow: operating
    mode: template
`,
		"flows/operating/schema.yaml": `name: operating
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"flows/operating/events.yaml": `opco.product_initialization_requested:
  entity_id: string
opco.ceo_ready:
  entity_id: string
`,
		"flows/operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
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
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: operating
    flow: operating
    mode: template
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string
  instance_id: string
  product_id: string
`,
		"nodes.yaml": `portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested:
      action: create_flow_instance
      template: operating
      instance_id_from: payload.instance_id
      config_from:
        product_id: payload.product_id
`,
		"flows/operating/schema.yaml": `name: operating
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"flows/operating/events.yaml": `opco.product_initialization_requested:
  product_id: string
component_scaffold.spawn_requested:
  product_id: string
`,
		"flows/operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      emit:
        event: component_scaffold.spawn_requested
        fields:
          product_id: payload.product_id
`,
	}
}

func templateInstanceActivationConfigSubscriberFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: operating
    flow: operating
    mode: template
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string
  instance_id: string
  product_id: string
`,
		"nodes.yaml": `portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested:
      action: create_flow_instance
      template: operating
      instance_id_from: payload.instance_id
      config_from:
        product_id: payload.product_id
`,
		"flows/operating/schema.yaml": `name: operating
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"flows/operating/events.yaml": `opco.product_initialization_requested:
  instance_id: string
  template_id: string
  flow_path: string
  parent_entity_id: string
  product_id: string
`,
		"flows/operating/agents.yaml": `ceo:
  id: ceo-{product_id}
  type: generic
  role: ceo
  subscriptions: [opco.product_initialization_requested]
`,
	}
}

func templateInstanceConnectLifecycleRollbackFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: template
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
    delivery: one
  - from: producer.deploy_done
    to: consumer.deploy_audited
    delivery: one
`,
		"schema.yaml": "name: test\n",
		"policy.yaml": "{}\n",
		"tools.yaml":  "{}\n",
		"agents.yaml": "{}\n",
		"events.yaml": "{}\n",
		"nodes.yaml":  "{}\n",
		"flows/producer/schema.yaml": `name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: vertical_id
        carries: [vertical_id]
`,
		"flows/producer/policy.yaml":   "{}\n",
		"flows/producer/agents.yaml":   "{}\n",
		"flows/producer/events.yaml":   "deploy.done:\n  vertical_id: string\n",
		"flows/producer/entities.yaml": "{}\n",
		"flows/producer/nodes.yaml":    "{}\n",
		"flows/consumer/schema.yaml": `name: consumer
mode: template
instance:
  by: vertical_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
      - name: deploy_audited
        event: deploy.done
`,
		"flows/consumer/policy.yaml": "{}\n",
		"flows/consumer/agents.yaml": "{}\n",
		"flows/consumer/events.yaml": "deploy.done:\n  vertical_id: string\n",
		"flows/consumer/entities.yaml": `deployment:
  vertical_id:
    type: string
`,
		"flows/consumer/nodes.yaml": `consumer-node:
  id: consumer-node-{instance_id}
  execution_type: system_node
  event_handlers:
    deploy.done: {}
`,
	}
}

type failingDeliveryRouteStore struct {
	*store.PostgresStore
	failDeliveryRoutes            bool
	descriptorsSeenDuringDelivery []runtimebus.ActiveFlowInstanceDescriptor
}

func (s *failingDeliveryRouteStore) RunEventMutation(ctx context.Context, fn func(runtimebus.EventMutation) error) error {
	if s == nil || s.PostgresStore == nil {
		return errors.New("postgres store is required")
	}
	return s.PostgresStore.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
		return fn(&failingDeliveryRouteMutation{
			EventMutation: mutation,
			store:         s,
		})
	})
}

type failingDeliveryRouteMutation struct {
	runtimebus.EventMutation
	store *failingDeliveryRouteStore
}

func (m *failingDeliveryRouteMutation) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	if err := m.captureDescriptorReadback(ctx); err != nil {
		return err
	}
	if m.store.failDeliveryRoutes {
		return errors.New("injected delivery route persistence failure")
	}
	return m.EventMutation.InsertEventDeliveries(ctx, eventID, agentIDs)
}

func (m *failingDeliveryRouteMutation) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	if err := m.captureDescriptorReadback(ctx); err != nil {
		return err
	}
	if m.store.failDeliveryRoutes {
		return errors.New("injected delivery route persistence failure")
	}
	return m.EventMutation.InsertEventDeliveriesWithTargets(ctx, eventID, agentIDs, deliveryTargets)
}

func (m *failingDeliveryRouteMutation) InsertEventDeliveryRoutes(ctx context.Context, eventID string, routes []events.DeliveryRoute) error {
	if err := m.captureDescriptorReadback(ctx); err != nil {
		return err
	}
	if m.store.failDeliveryRoutes {
		return errors.New("injected delivery route persistence failure")
	}
	return m.EventMutation.InsertEventDeliveryRoutes(ctx, eventID, routes)
}

func (m *failingDeliveryRouteMutation) captureDescriptorReadback(ctx context.Context) error {
	if m == nil || m.store == nil {
		return errors.New("delivery route mutation store is required")
	}
	descriptors, err := m.store.ListActiveFlowInstanceDescriptors(ctx)
	if err != nil {
		return err
	}
	m.store.descriptorsSeenDuringDelivery = descriptors
	return nil
}

type routeMaterializationDBProofStore struct {
	pg *store.PostgresStore
}

func (s routeMaterializationDBProofStore) AppendEvent(ctx context.Context, evt events.Event) error {
	return s.pg.AppendEvent(ctx, evt)
}

func (s routeMaterializationDBProofStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return s.pg.InsertEventDeliveries(ctx, eventID, agentIDs)
}

func (s routeMaterializationDBProofStore) InsertEventDeliveryRoutes(ctx context.Context, eventID string, routes []events.DeliveryRoute) error {
	return s.pg.InsertEventDeliveryRoutes(ctx, eventID, routes)
}

func (s routeMaterializationDBProofStore) PersistEventWithDeliveryRouteSetAndScope(ctx context.Context, evt events.Event, routes []events.DeliveryRoute, scope runtimereplayclaim.CommittedReplayScope) error {
	return s.pg.PersistEventWithDeliveryRouteSetAndScope(ctx, evt, routes, scope)
}

func (s routeMaterializationDBProofStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	return s.pg.ListEventDeliveryRecipients(ctx, eventID)
}

func (s routeMaterializationDBProofStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	return s.pg.UpsertFlowInstanceRoute(ctx, route)
}

func (s routeMaterializationDBProofStore) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	return s.pg.DeleteFlowInstanceRoute(ctx, identity)
}

func (s routeMaterializationDBProofStore) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.Route, error) {
	return s.pg.ListFlowInstanceRoutes(ctx)
}

func templateInstanceEmpireOutboxFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: operating
    flow: operating
    mode: template
`,
		"events.yaml": `mailbox.item_decided:
  entity_id: string
  instance_id: string
  product_id: string
opco.spinup_requested:
  instance_id: string
  product_id: string
`,
		"nodes.yaml": `approval-router:
  id: approval-router
  execution_type: system_node
  subscribes_to: [mailbox.item_decided]
  produces: [opco.spinup_requested]
  event_handlers:
    mailbox.item_decided:
      emit:
        event: opco.spinup_requested
        broadcast: true
        fields:
          instance_id: payload.instance_id
          product_id: payload.product_id
portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested:
      action: create_flow_instance
      template: operating
      instance_id_from: payload.instance_id
      config_from:
        product_id: payload.product_id
`,
		"flows/operating/schema.yaml": `name: operating
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"flows/operating/events.yaml": `opco.product_initialization_requested:
  product_id: string
component_scaffold.spawn_requested:
  product_id: string
`,
		"flows/operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      emit:
        event: component_scaffold.spawn_requested
        fields:
          product_id: payload.product_id
`,
	}
}

func seedRuntimeTestRun(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(context.Background(), templateInstanceDeliveryRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, templateInstanceDeliveryRunID); err != nil {
		t.Fatalf("seed runtime test run: %v", err)
	}
	return ctx
}

func waitRuntimeDBCount(t *testing.T, ctx context.Context, db *sql.DB, query string, want int, args ...any) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var got int
		if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
			t.Fatalf("query count: %v", err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("count = %d, want %d for query %s", got, want, strings.TrimSpace(query))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitRuntimeEventID(t *testing.T, ctx context.Context, db *sql.DB, query string, args []any) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
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
			t.Fatalf("timed out waiting for event id from query %s", strings.TrimSpace(query))
		}
		time.Sleep(25 * time.Millisecond)
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
