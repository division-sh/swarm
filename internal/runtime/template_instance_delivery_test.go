package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
	evt := (events.Event{
		ID:        eventID,
		RunID:     templateInstanceDeliveryRunID,
		Type:      events.EventType("operating/inst-1/opco.product_initialization_requested"),
		Payload:   []byte(`{"entity_id":"11111111-1111-4111-8111-111111111111"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("11111111-1111-4111-8111-111111111111").WithFlowInstance("operating/inst-1")
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
	`, 1, eventID)
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

	spinup := (events.Event{
		ID:        "99999999-9999-4999-8999-999999999910",
		RunID:     templateInstanceDeliveryRunID,
		Type:      events.EventType("opco.spinup_requested"),
		Payload:   []byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("22222222-2222-4222-8222-222222222222")
	if err := bus.Publish(ctx, spinup); err != nil {
		t.Fatalf("Publish spinup: %v", err)
	}
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node' AND outcome = 'no_op'
	`, 1, spinup.ID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
	`, 1, spinup.ID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'portfolio-node'
		  AND delivered_at IS NOT NULL AND created_at < delivered_at
	`, 1, spinup.ID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, spinup.ID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, spinup.ID)
	autoEventID := waitRuntimeEventID(t, ctx, db, `
		SELECT event_id::text FROM events
		WHERE event_name = 'operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested'
	`, nil)
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
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM events
		WHERE event_name = 'operating/component_scaffold.spawn_requested'
	`, 1)
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

	spinup := (events.Event{
		ID:        "99999999-9999-4999-8999-999999999930",
		RunID:     templateInstanceDeliveryRunID,
		Type:      events.EventType("opco.spinup_requested"),
		Payload:   []byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("22222222-2222-4222-8222-222222222222")
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

func TestTemplateInstanceRootOutboxEventDispatchesRoutedSystemNodeWithoutInternalCarrierAndEmpireStyleSideEffect(t *testing.T) {
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

	mailbox := (events.Event{
		ID:        "99999999-9999-4999-8999-999999999913",
		RunID:     templateInstanceDeliveryRunID,
		Type:      events.EventType("mailbox.item_decided"),
		Payload:   []byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("22222222-2222-4222-8222-222222222222")
	if err := bus.Publish(ctx, mailbox); err != nil {
		t.Fatalf("Publish mailbox: %v", err)
	}

	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router' AND outcome = 'no_op'
	`, 1, mailbox.ID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
	`, 1, mailbox.ID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
		  AND delivered_at IS NOT NULL AND created_at < delivered_at
	`, 1, mailbox.ID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 0, mailbox.ID)

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
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM events
		WHERE event_name = 'operating/component_scaffold.spawn_requested'
	`, 1)
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

	mailbox := (events.Event{
		ID:        "99999999-9999-4999-8999-999999999912",
		RunID:     templateInstanceDeliveryRunID,
		Type:      events.EventType("mailbox.item_decided"),
		Payload:   []byte(`{"entity_id":"22222222-2222-4222-8222-222222222222","instance_id":"11111111-1111-4111-8111-111111111111","product_id":"product-1"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("22222222-2222-4222-8222-222222222222")
	if err := bus.Publish(ctx, mailbox); err != nil {
		t.Fatalf("Publish mailbox: %v", err)
	}

	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router' AND outcome = 'no_op'
	`, 1, mailbox.ID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'approval-router'
	`, 1, mailbox.ID)

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
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM events
		WHERE event_name = 'operating/component_scaffold.spawn_requested'
	`, 1)
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
  instance_id: string
  template_id: string
  flow_path: string
  parent_entity_id: string
  product_id: string
component_scaffold.spawn_requested:
  instance_id: string
  template_id: string
  flow_path: string
  parent_entity_id: string
  product_id: string
`,
		"flows/operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      emit: component_scaffold.spawn_requested
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

type routeMaterializationDBProofStore struct {
	pg *store.PostgresStore
}

func (s routeMaterializationDBProofStore) AppendEvent(ctx context.Context, evt events.Event) error {
	return s.pg.AppendEvent(ctx, evt)
}

func (s routeMaterializationDBProofStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return s.pg.InsertEventDeliveries(ctx, eventID, agentIDs)
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
  instance_id: string
  template_id: string
  flow_path: string
  parent_entity_id: string
  product_id: string
component_scaffold.spawn_requested:
  instance_id: string
  template_id: string
  flow_path: string
  parent_entity_id: string
  product_id: string
`,
		"flows/operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      emit: component_scaffold.spawn_requested
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
