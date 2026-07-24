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
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storetest "github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testsql"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const templateInstanceDeliveryRunID = "99999999-9999-4999-8999-999999999901"

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
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
		Module:        module,
		DeliveryStore: pg,
	})
	if err := bus.AddFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := "99999999-9999-4999-8999-999999999902"
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"operating",
		"",
		[]byte(`{"entity_id":"11111111-1111-4111-8111-111111111111"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EnvelopeForSourceRoute(
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-4111-8111-111111111111"), "operating/inst-1"),
			events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: "11111111-1111-4111-8111-111111111111"},
		),
		time.Now().UTC(),
	)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitRuntimeNodeDeliveryOutcome(t, ctx, db, eventID, "lifecycle-orchestrator")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, eventID)
	assertRuntimeCommittedReplayScope(t, ctx, db, eventID)
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
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := bus.AddFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := runtimeInternalDeliveriesForTest(t, bus, "workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	eventID := "99999999-9999-4999-8999-999999999903"
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"operating",
		"",
		[]byte(`{"entity_id":"11111111-1111-4111-8111-111111111111"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
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
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	manager := ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances: workflowStore,
		LifecycleStore:    pg,
		DeliveryStore:     pg,
	}))
	activationCalls := 0
	var activationErr error
	module := newRuntimeTestWorkflowModule(t, source)
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
		Module:        module,
		DeliveryStore: pg,
		InstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			activationCalls++
			activationErr = manager.ActivateFlowInstance(ctx, req)
			return activationErr
		},
		WorkflowStore: workflowStore,
	})
	bus.SetInterceptors(pc)

	spinup := eventtest.RunCreatingRootIngress(
		"99999999-9999-4999-8999-999999999910",
		events.EventType("opco.spinup_requested"),
		"test-producer",
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
	if activationCalls != 1 || activationErr != nil {
		t.Fatalf("flow activation calls = %d, error = %v; want one successful activation", activationCalls, activationErr)
	}
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, spinup.ID(), "portfolio-node")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
	`, 1, spinup.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
		  AND settled_at IS NOT NULL AND created_at < settled_at
	`, 1, spinup.ID())
	assertRuntimeCommittedReplayScope(t, ctx, db, spinup.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, spinup.ID())
	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, autoEventID, "lifecycle-orchestrator")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, autoEventID)
	assertRuntimeCommittedReplayScope(t, ctx, db, autoEventID)
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
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	proofStore := routeMaterializationDBProofStore{pg: pg}
	bus, err := newScopedTestEventBus(t, proofStore, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	manager := ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances: workflowStore,
		LifecycleStore:    pg,
		DeliveryStore:     pg,
	}))
	module := newRuntimeTestWorkflowModule(t, source)
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		DeliveryStore:     pg,
	})
	bus.SetInterceptors(pc)

	spinup := eventtest.RunCreatingRootIngress(
		"99999999-9999-4999-8999-999999999930",
		events.EventType("opco.spinup_requested"),
		"test-producer",
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
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	var manager *runtimemanager.AgentManager
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
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
	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances: workflowStore,
		LifecycleStore:    pg,
	}))
	evt := eventtest.RunCreatingRootIngress(
		"99999999-9999-4999-8999-999999999940",
		events.EventType("producer/deploy.done"),
		"producer",
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
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	manager := ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances: workflowStore,
		LifecycleStore:    pg,
		DeliveryStore:     pg,
	}))
	module := newRuntimeTestWorkflowModule(t, source)
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		DeliveryStore:     pg,
	})

	mailbox := eventtest.RunCreatingRootIngress(
		"99999999-9999-4999-8999-999999999913",
		events.EventType("approval.completed"),
		"approval-source",
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

	waitRuntimeNodeDeliveryOutcome(t, ctx, db, mailbox.ID(), "approval-router")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
	`, 1, mailbox.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
		  AND settled_at IS NOT NULL AND created_at < settled_at
	`, 1, mailbox.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, mailbox.ID())

	spinupEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'opco.spinup_requested'
	`, nil)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, spinupEventID, "portfolio-node")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
	`, 1, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
		  AND settled_at IS NOT NULL AND created_at < settled_at
	`, 1, spinupEventID)
	assertRuntimeCommittedReplayScope(t, ctx, db, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, spinupEventID)

	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, autoEventID, "lifecycle-orchestrator")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, autoEventID)
	assertRuntimeCommittedReplayScope(t, ctx, db, autoEventID)
	componentEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/component_scaffold.spawn_requested'
	`, nil)
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
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	manager := ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances: workflowStore,
		LifecycleStore:    pg,
		DeliveryStore:     pg,
	}))
	module := newRuntimeTestWorkflowModule(t, source)
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		DeliveryStore:     pg,
	})

	mailbox := eventtest.RunCreatingRootIngress(
		"99999999-9999-4999-8999-999999999912",
		events.EventType("approval.completed"),
		"approval-source",
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

	waitRuntimeNodeDeliveryOutcome(t, ctx, db, mailbox.ID(), "approval-router")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
	`, 1, mailbox.ID())

	spinupEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'opco.spinup_requested'
	`, nil)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, spinupEventID, "portfolio-node")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
	`, 1, spinupEventID)
	assertRuntimeCommittedReplayScope(t, ctx, db, spinupEventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, spinupEventID)

	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested'
	`, nil)
	assertRuntimeEventPayloadProductOnly(t, ctx, db, autoEventID)
	waitRuntimeNodeDeliveryOutcome(t, ctx, db, autoEventID, "lifecycle-orchestrator")
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, autoEventID)
	assertRuntimeCommittedReplayScope(t, ctx, db, autoEventID)
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

func loadRuntimeBundleRoot(t *testing.T, root string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical routing bundle: %v", err)
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

type providerRollbackBackend interface {
	runtimebus.EventStore
	runtimeinbound.Runner
	runtimepipeline.RuntimeMutationRunner
}

func TestProviderNormalizedLifecycleRollbackMatrix(t *testing.T) {
	checkpoints := []struct {
		name           string
		mutation       providerRollbackMutationCheckpoint
		withoutCarrier bool
		retry          bool
	}{
		{name: "after receiver flow-instance creation", mutation: providerRollbackAfterFlowInstanceCreation},
		{name: "after receiver entity creation", mutation: providerRollbackAfterEntityCreation},
		{name: "after receiver route materialization", mutation: providerRollbackAfterRouteMaterialization},
		{name: "after raw append", mutation: providerRollbackAfterRawAppend},
		{name: "after raw replay before normalized append", mutation: providerRollbackBeforeNormalizedAppend},
		{name: "after normalized append before delivery", mutation: providerRollbackBeforeDelivery},
		{name: "after delivery before normalized replay", mutation: providerRollbackBeforeNormalizedReplay},
		{name: "after normalized replay before receipt", mutation: providerRollbackBeforeReceipt, withoutCarrier: true},
		{name: "after receipt before dead letter", mutation: providerRollbackBeforeDeadLetter, withoutCarrier: true},
		{name: "immediately before commit", mutation: providerRollbackBeforeCommit, retry: true},
	}
	backends := []struct {
		name  string
		setup func(*testing.T, providerRollbackMutationCheckpoint) (context.Context, *sql.DB, providerRollbackBackend)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T, checkpoint providerRollbackMutationCheckpoint) (context.Context, *sql.DB, providerRollbackBackend) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				ctx := seedRuntimeTestRun(t, db)
				ctx = runtimecorrelation.WithBundleSourceFact(ctx, providerRollbackBundleSourceFact())
				return ctx, db, &providerRollbackPostgresStore{
					PostgresStore: storetest.AdmitPostgresRuntimeStore(t, db),
					proof:         &providerRollbackProof{checkpoint: checkpoint},
				}
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T, checkpoint providerRollbackMutationCheckpoint) (context.Context, *sql.DB, providerRollbackBackend) {
				sqliteStore := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				ctx = runtimecorrelation.WithBundleSourceFact(ctx, providerRollbackBundleSourceFact())
				if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, templateInstanceDeliveryRunID); err != nil {
					t.Fatalf("seed SQLite rollback run: %v", err)
				}
				return ctx, sqliteStore.DB, &providerRollbackSQLiteStore{
					SQLiteRuntimeStore: sqliteStore,
					proof:              &providerRollbackProof{checkpoint: checkpoint},
				}
			},
		},
	}

	for _, backend := range backends {
		for _, checkpoint := range checkpoints {
			t.Run(backend.name+"/"+checkpoint.name, func(t *testing.T) {
				ctx, db, eventStore := backend.setup(t, checkpoint.mutation)
				source := providerRollbackSemanticSource(t, !checkpoint.withoutCarrier)
				plans, issues := runtimepinrouting.LowerTargetFreeInputRoutePlans(source, []runtimeprovideroutput.Authorization{providerRollbackAuthorization()})
				if len(issues) != 0 || len(plans) != 1 {
					t.Fatalf("target-free rollback fixture plans=%#v issues=%#v, want one plan", plans, issues)
				}
				workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
				if backend.name == "sqlite" {
					workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, eventStore)
				}
				var manager *runtimemanager.AgentManager
				bus, err := newScopedTestEventBus(t, eventStore, runtimebus.EventBusOptions{
					ContractBundle: source, ProviderOutputVerifier: source.(runtimebus.ProviderOutputAuthorizationVerifier),
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
				manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
					WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
					WorkflowInstances: workflowStore,
				}))

				candidate := providerRollbackStandingCandidate(ctx)
				standing, err := workflowStore.ReconcileStandingService(ctx, candidate)
				if err != nil {
					t.Fatalf("ReconcileStandingService: %v", err)
				}
				sequence, err := workflowStore.PublishStandingService(ctx, candidate.ServiceID, standing.RunID, standing.Generation)
				if err != nil {
					t.Fatalf("PublishStandingService: %v", err)
				}
				ctx = runtimecorrelation.WithRunID(ctx, standing.RunID)
				request := providerRollbackRequest(t, candidate, standing.RunID, sequence)
				batch := providerRollbackInboundBatch(t, request)
				_, err = eventStore.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
					prepareCtx := runtimeauthoractivity.WithInboundProjection(mutation.Context(), runtimeauthoractivity.InboundProjection{})
					prepared, prepareErr := bus.PrepareInboundDeliveryBatchInMutation(prepareCtx, batch)
					if prepareErr != nil {
						return prepareErr
					}
					return finalizeProviderRollbackBatch(mutation, request, batch, prepared)
				})
				if err == nil || !strings.Contains(err.Error(), "injected provider rollback checkpoint") {
					t.Fatalf("PrepareInboundDeliveryBatchInMutation error = %v, want injected checkpoint", err)
				}
				assertProviderRollbackTablesEmpty(t, ctx, db)

				if checkpoint.retry {
					_, err := eventStore.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
						prepareCtx := runtimeauthoractivity.WithInboundProjection(mutation.Context(), runtimeauthoractivity.InboundProjection{})
						prepared, prepareErr := bus.PrepareInboundDeliveryBatchInMutation(prepareCtx, batch)
						if prepareErr != nil {
							return prepareErr
						}
						return finalizeProviderRollbackBatch(mutation, request, batch, prepared)
					})
					if err != nil {
						t.Fatalf("retry PrepareInboundDeliveryBatchInMutation: %v", err)
					}
					assertProviderRollbackRetryCommitted(t, ctx, db)
				}
			})
		}
	}
}

type providerRollbackMutationCheckpoint string

const (
	providerRollbackAfterFlowInstanceCreation providerRollbackMutationCheckpoint = "after_flow_instance_creation"
	providerRollbackAfterEntityCreation       providerRollbackMutationCheckpoint = "after_entity_creation"
	providerRollbackAfterRouteMaterialization providerRollbackMutationCheckpoint = "after_route_materialization"
	providerRollbackAfterRawAppend            providerRollbackMutationCheckpoint = "after_raw_append"
	providerRollbackBeforeNormalizedAppend    providerRollbackMutationCheckpoint = "before_normalized_append"
	providerRollbackBeforeDelivery            providerRollbackMutationCheckpoint = "before_delivery"
	providerRollbackBeforeNormalizedReplay    providerRollbackMutationCheckpoint = "before_normalized_replay"
	providerRollbackBeforeReceipt             providerRollbackMutationCheckpoint = "before_receipt"
	providerRollbackBeforeDeadLetter          providerRollbackMutationCheckpoint = "before_dead_letter"
	providerRollbackBeforeCommit              providerRollbackMutationCheckpoint = "before_commit"
)

type providerRollbackProof struct {
	checkpoint providerRollbackMutationCheckpoint
	failed     bool
	appends    int
	replays    int
}

func (p *providerRollbackProof) fail(checkpoint providerRollbackMutationCheckpoint) error {
	if p == nil || p.failed || p.checkpoint != checkpoint {
		return nil
	}
	p.failed = true
	return fmt.Errorf("injected provider rollback checkpoint %s", checkpoint)
}

type providerRollbackPostgresStore struct {
	*store.PostgresStore
	proof *providerRollbackProof
}

func (s *providerRollbackPostgresStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	return upsertProviderRollbackFlowInstanceRoute(ctx, route, s.proof, s.PostgresStore.UpsertFlowInstanceRoute)
}

func (s *providerRollbackPostgresStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	return s.PostgresStore.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
		if err := fn(newProviderRollbackMutation(mutation, s.proof)); err != nil {
			return err
		}
		return s.proof.fail(providerRollbackBeforeCommit)
	})
}

type providerRollbackSQLiteStore struct {
	*store.SQLiteRuntimeStore
	proof *providerRollbackProof
}

func (s *providerRollbackSQLiteStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	return upsertProviderRollbackFlowInstanceRoute(ctx, route, s.proof, s.SQLiteRuntimeStore.UpsertFlowInstanceRoute)
}

func (s *providerRollbackSQLiteStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	return s.SQLiteRuntimeStore.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
		if err := fn(newProviderRollbackMutation(mutation, s.proof)); err != nil {
			return err
		}
		return s.proof.fail(providerRollbackBeforeCommit)
	})
}

func upsertProviderRollbackFlowInstanceRoute(
	ctx context.Context,
	route runtimebus.FlowInstanceRouteRecord,
	proof *providerRollbackProof,
	upsert func(context.Context, runtimebus.FlowInstanceRouteRecord) error,
) error {
	switch proof.checkpoint {
	case providerRollbackAfterFlowInstanceCreation:
		if err := requireProviderRollbackRowVisible(ctx, "flow_instances"); err != nil {
			return err
		}
		return proof.fail(providerRollbackAfterFlowInstanceCreation)
	case providerRollbackAfterEntityCreation:
		if err := requireProviderRollbackRowVisible(ctx, "entity_state"); err != nil {
			return err
		}
		return proof.fail(providerRollbackAfterEntityCreation)
	}
	if err := upsert(ctx, route); err != nil {
		return err
	}
	if proof.checkpoint == providerRollbackAfterRouteMaterialization {
		if err := requireProviderRollbackRowVisible(ctx, "routing_rules"); err != nil {
			return err
		}
		return proof.fail(providerRollbackAfterRouteMaterialization)
	}
	return nil
}

func requireProviderRollbackRowVisible(ctx context.Context, table string) error {
	if table != "flow_instances" && table != "entity_state" && table != "routing_rules" {
		return fmt.Errorf("unsupported provider rollback proof table %q", table)
	}
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return fmt.Errorf("provider rollback proof requires ambient selected-store transaction")
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		return fmt.Errorf("inspect %s before injected later failure: %w", table, err)
	}
	if count == 0 {
		return fmt.Errorf("%s row was not visible before injected later failure", table)
	}
	return nil
}

type providerRollbackMutation struct {
	runtimeinbound.Mutation
	proof       *providerRollbackProof
	transaction runtimebus.CommitPublishTransaction
}

func newProviderRollbackMutation(mutation runtimeinbound.Mutation, proof *providerRollbackProof) *providerRollbackMutation {
	transaction, _ := runtimebus.CommitPublishTransactionFromContext(mutation.Context())
	return &providerRollbackMutation{Mutation: mutation, proof: proof, transaction: transaction}
}

func (m *providerRollbackMutation) Context() context.Context {
	return runtimebus.WithCommitPublishTransaction(m.Mutation.Context(), m)
}

func (m *providerRollbackMutation) BeginPreparedPublish(ctx context.Context, prepared runtimebus.PreparedPublishEvent) (runtimebus.EventAppendOutcome, error) {
	if m.transaction == nil {
		return runtimebus.EventAppendOutcomeUnknown, errors.New("provider rollback proof requires commit transaction")
	}
	m.proof.appends++
	if m.proof.appends == 2 {
		if err := m.proof.fail(providerRollbackBeforeNormalizedAppend); err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
	}
	outcome, err := m.transaction.BeginPreparedPublish(ctx, prepared)
	if err != nil {
		return outcome, err
	}
	if m.proof.appends == 1 {
		if err := m.proof.fail(providerRollbackAfterRawAppend); err != nil {
			return runtimebus.EventAppendOutcomeUnknown, err
		}
	}
	return outcome, nil
}

func (m *providerRollbackMutation) FinalizePreparedPublish(ctx context.Context, finalization runtimebus.PreparedPublishFinalization) error {
	if m.transaction == nil {
		return errors.New("provider rollback proof requires commit transaction")
	}
	req := finalization.Request()
	if len(req.DeliveryRoutes) > 0 {
		if err := m.proof.fail(providerRollbackBeforeDelivery); err != nil {
			return err
		}
	}
	if req.Disposition != nil {
		if err := m.proof.fail(providerRollbackBeforeReceipt); err != nil {
			return err
		}
	}
	if req.DeadLetter != nil {
		if err := m.proof.fail(providerRollbackBeforeDeadLetter); err != nil {
			return err
		}
	}
	if err := m.transaction.FinalizePreparedPublish(ctx, finalization); err != nil {
		return err
	}
	if m.proof.appends == 2 {
		if err := m.proof.fail(providerRollbackBeforeNormalizedReplay); err != nil {
			return err
		}
	}
	return nil
}

type providerRollbackSource struct {
	semanticview.Source
	authorization runtimeprovideroutput.Authorization
}

func (s providerRollbackSource) ProviderTriggerTargetFreeAuthorizations() []runtimeprovideroutput.Authorization {
	return []runtimeprovideroutput.Authorization{s.authorization}
}

func (s providerRollbackSource) VerifyProviderOutputAuthorization(actual runtimeprovideroutput.Authorization) error {
	if !s.authorization.Matches(actual) {
		return errors.New("authorization does not match rollback catalog owner")
	}
	return nil
}

func (s providerRollbackSource) BaseSemanticSource() semanticview.Source {
	return s.Source
}

func providerRollbackSemanticSource(t *testing.T, withCarrier bool) semanticview.Source {
	t.Helper()
	bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyProviderRollback(t, withCarrier))
	return providerRollbackSource{Source: semanticview.Wrap(bundle), authorization: providerRollbackAuthorization()}
}

func providerRollbackAuthorization() runtimeprovideroutput.Authorization {
	return runtimeprovideroutput.Authorization{
		Provider: "telegram", Event: "inbound.telegram.text_message",
		PackID: "provider.telegram", PackVersion: "1.0.0",
		ManifestHash: "sha256:" + strings.Repeat("a", 64), GenerationID: "rollback-generation",
	}
}

func providerRollbackBundleSourceFact() runtimecorrelation.BundleSourceFact {
	return runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
		BundleSource:      "ephemeral",
		BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
	}
}

func providerRollbackStandingCandidate(ctx context.Context) runtimepipeline.StandingServiceCandidate {
	source, _ := runtimecorrelation.BundleSourceFactFromContext(ctx)
	packageKey := "provider-rollback"
	flowID := "ingress"
	return runtimepipeline.StandingServiceCandidate{
		ServiceID: runtimeflowidentity.StandingServiceID(packageKey, flowID), PackageKey: packageKey,
		FlowID: flowID, InstanceID: uuid.NewString(), EntityID: "22222222-2222-4222-8222-222222222222", Source: source,
	}
}

func providerRollbackRequest(t *testing.T, candidate runtimepipeline.StandingServiceCandidate, runID string, sequence int64) runtimeinbound.Request {
	t.Helper()
	providerEventID := "provider-rollback-event"
	publicationID, markerEventID := runtimeinbound.DeterministicIDs("telegram", candidate.EntityID, providerEventID)
	fingerprint, err := runtimeinbound.SemanticFingerprint(map[string]any{"provider": "telegram", "provider_event_id": providerEventID})
	if err != nil {
		t.Fatal(err)
	}
	return runtimeinbound.Request{
		PublicationID: publicationID, Provider: "telegram", EntityID: candidate.EntityID, ProviderEventID: providerEventID,
		RequestFingerprint: fingerprint, RequestProjectionVersion: runtimeinbound.RequestSemanticProjectionVersion,
		StableServiceID: candidate.ServiceID, PackageKey: candidate.PackageKey, FlowID: candidate.FlowID, InstanceID: candidate.InstanceID,
		TargetAlias: "telegram", TargetFlowInstance: candidate.FlowID + "/" + candidate.InstanceID,
		ExpectedPublicationSequence: sequence, ResolvedRunID: runID, MarkerEventID: markerEventID,
		AcknowledgementMode: runtimeinbound.AcknowledgementDurableBeforeDispatch,
		OriginalReceivedAt:  time.Now().UTC().Truncate(time.Microsecond), OriginalUserAgent: "rollback-proof", OriginalTransportMetadata: []byte(`{"method":"POST"}`),
	}
}

func providerRollbackInboundBatch(t *testing.T, request runtimeinbound.Request) runtimebus.InboundDeliveryBatch {
	t.Helper()
	now := time.Now().UTC()
	rawID, err := runtimeinbound.DeterministicEventID(request.PublicationID, 0)
	if err != nil {
		t.Fatal(err)
	}
	normalizedID, err := runtimeinbound.DeterministicEventID(request.PublicationID, 1)
	if err != nil {
		t.Fatal(err)
	}
	return runtimebus.InboundDeliveryBatch{
		Provider: "telegram",
		Events: []runtimebus.InboundDeliveryEvent{
			{Event: eventtest.ExistingRunRootIngress(
				rawID, "inbound.telegram", "inbound-gateway", "", []byte(`{"raw":true}`), 0,
				request.ResolvedRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), now,
			), Kind: runtimeprovideroutput.KindRaw},
			{Event: eventtest.ExistingRunRootIngress(
				normalizedID, "inbound.telegram.text_message", "inbound-gateway", "", []byte(`{"chat_id":"42"}`), 0,
				request.ResolvedRunID, events.EventEnvelope{}, now,
			), Kind: runtimeprovideroutput.KindNormalized, Authorization: providerRollbackAuthorization()},
		},
	}
}

func finalizeProviderRollbackBatch(mutation runtimeinbound.Mutation, request runtimeinbound.Request, batch runtimebus.InboundDeliveryBatch, prepared []runtimebus.PreparedPublish) error {
	if len(prepared) != len(batch.Events) {
		return fmt.Errorf("prepared provider rollback batch size %d does not match %d events", len(prepared), len(batch.Events))
	}
	eventIDs := make([]string, len(prepared))
	eventNames := make([]string, len(prepared))
	finalization := runtimeinbound.Finalization{Events: make([]runtimeinbound.EventFinalization, len(prepared))}
	for index := range prepared {
		manifest, _, _, err := runtimeinbound.CanonicalRecipientManifest(prepared[index].DeliveryRoutes())
		if err != nil {
			return err
		}
		eventIDs[index] = prepared[index].Event.ID()
		eventNames[index] = string(prepared[index].Event.Type())
		finalization.Events[index] = runtimeinbound.EventFinalization{
			Ordinal: index, Event: prepared[index].Event, Kind: batch.Events[index].Kind,
			Authorization: batch.Events[index].Authorization, RecipientManifest: manifest,
		}
	}
	payload, err := runtimeinbound.BuildEvidencePayload(request, eventIDs, eventNames)
	if err != nil {
		return err
	}
	finalization.EvidenceEvent = eventtest.DiagnosticDirect(
		request.MarkerEventID, events.EventTypePlatformInboundRecord, "runtime", "", payload, 0,
		request.ResolvedRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), request.OriginalReceivedAt,
	)
	return mutation.FinalizeInboundPublication(mutation.Context(), finalization)
}

func assertProviderRollbackTablesEmpty(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for _, table := range []string{"events", "event_deliveries", "event_receipts", "flow_instances", "entity_state", "routing_rules"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("count %s after rollback: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s rows after rollback = %d, want 0", table, count)
		}
	}
	var runs int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runs").Scan(&runs); err != nil {
		t.Fatalf("count runs after rollback: %v", err)
	}
	if runs != 2 {
		t.Fatalf("runs after rollback = %d, want seed plus standing-service run", runs)
	}
}

func assertProviderRollbackRetryCommitted(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for table, minimum := range map[string]int{
		"events": 2, "event_deliveries": 1, "flow_instances": 1, "entity_state": 1, "routing_rules": 1,
	} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("count %s after retry: %v", table, err)
		}
		if count < minimum {
			t.Fatalf("%s rows after retry = %d, want at least %d", table, count, minimum)
		}
	}
}

type routeMaterializationDBProofStore struct {
	pg *store.PostgresStore
}

func (s routeMaterializationDBProofStore) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return s.pg.RegisterAuthorActivityEventCatalog(scope, descriptors)
}

func (s routeMaterializationDBProofStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return s.pg.CommitPublish(ctx, plan)
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

func seedRuntimeTestRun(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
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
	instanceRows, err := db.QueryContext(ctx, `SELECT flow_template, instance_id, status FROM flow_instances ORDER BY created_at, instance_id`)
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
