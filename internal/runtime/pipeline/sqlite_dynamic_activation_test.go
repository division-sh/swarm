package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

func TestSQLiteFanOutCreateFlowInstanceDeliveriesPersistWithoutDeadLetter(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	workflowStore := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ctx := sqliteExactOnceRunContext(t, db)
	pc, bus := newSQLiteDynamicActivationCoordinator(t, db, workflowStore)
	parentEntityID := uuid.NewString()
	parentPath := runtimecorrelation.RunIDFromContext(ctx)

	parent := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("component_scaffold.batch_requested"),
		"",
		"",
		mustJSON(map[string]any{
			"components": []any{
				map[string]any{"component_id": "component-a"},
				map[string]any{"component_id": "component-b"},
			},
		}),
		0,
		runtimecorrelation.RunIDFromContext(ctx),
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, parentEntityID), parentPath),
		time.Now().UTC(),
	)

	if err := workflowStore.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      parentPath,
		StorageRef:      parentPath,
		EntityID:        parentEntityID,
		EntityType:      "parent",
		WorkflowName:    "root",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Fields:          map[string]any{},
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	})); err != nil {
		t.Fatalf("seed parent workflow instance: %v", err)
	}
	parentNode := pipelineSourceNode(t, pc.SemanticSource(), "", "fanout-node")
	parentRoute := seedExactOnceEventDelivery(t, pc, ctx, parent, parentNode)
	state, err := pc.currentWorkflowState(runtimecorrelation.WithInboundEvent(ctx, parent), testWorkflowInstanceRoute(parentPath), identity.NormalizeEntityID(parentEntityID))
	if err != nil {
		t.Fatalf("load parent workflow state: %v", err)
	}
	if got := strings.TrimSpace(state.Control.FlowPath); got != parentPath {
		t.Fatalf("parent workflow state flow_path = %q, want %s", got, parentPath)
	}

	handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, parentRoute), parent)
	if err != nil {
		t.Fatalf("dispatch parent fan-out event: %v", err)
	}
	if !handled {
		t.Fatal("parent fan-out dispatch handled=false, want true")
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("published child events = %d, want 2", got)
	}
	assertDeliveryStatusCount(t, workflowStore, ctx, parent.ID(), parentNode.Key(), "delivered", 1)

	children := []events.Event{bus.publishedEvent(0), bus.publishedEvent(1)}
	childRoutes := make([]events.DeliveryRoute, len(children))
	spawnNode := pipelineSourceNode(t, pc.SemanticSource(), "", "spawn-node")
	for idx, child := range children {
		if got := strings.TrimSpace(child.ParentEventID()); got != parent.ID() {
			t.Fatalf("child %s parent_event_id = %q, want %q", child.ID(), got, parent.ID())
		}
		admitted, err := events.AdmitForPublish(child, events.AdmissionOptions{Now: time.Now().UTC(), RequirePersistentUUIDIdentity: true})
		if err != nil {
			t.Fatalf("admit child %d for selected-store persistence: %v", idx, err)
		}
		child = admitted.Event()
		children[idx] = child
		if strings.TrimSpace(child.ID()) == "" {
			t.Fatalf("child %d has no event identity", idx)
		}
		if got, want := strings.TrimSpace(child.RunID()), runtimecorrelation.RunIDFromContext(ctx); got != want {
			t.Fatalf("child %s run_id = %q, want %q", child.ID(), got, want)
		}
		childRoutes[idx] = seedExactOnceEventDelivery(t, pc, ctx, child, spawnNode)
	}

	for idx, child := range children {
		route := childRoutes[idx]
		handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), child)
		if err != nil {
			t.Fatalf("dispatch child create_flow_instance event: %v", err)
		}
		if !handled {
			t.Fatalf("child event %s type %s was not handled", child.ID(), child.Type())
		}
	}

	assertSQLiteWorkflowInstancePersisted(t, workflowStore, ctx, "review/component-a")
	assertSQLiteWorkflowInstancePersisted(t, workflowStore, ctx, "review/component-b")
	for _, child := range children {
		assertDeliveryStatusCount(t, workflowStore, ctx, child.ID(), spawnNode.Key(), "delivered", 1)
		assertDeliveryStatusCount(t, workflowStore, ctx, child.ID(), spawnNode.Key(), "dead_letter", 0)
	}
	if logs := bus.runtimeLogEntries(); len(logs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", logs)
	}
}

func newSQLiteDynamicActivationCoordinator(t *testing.T, db *sql.DB, workflowStore *workflowInstanceStore) (*PipelineCoordinator, *recordingPipelineBus) {
	t.Helper()
	bus := &recordingPipelineBus{}
	bundle := sqliteDynamicActivationBundle(t)
	deliveryStore := newPipelineTestDeliveryOwnerForDB(t, db)
	bus.configurePipelineTestDeliveryOwner(deliveryStore)
	pc := newDurablePipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
		Persistence:         workflowPersistenceForTest(workflowStore),
		DeliveryStore:       deliveryStore,
		DeliveryRuntime:     bus,
		PipelineObligations: unavailablePipelineTestObligationOwner{},
		InstanceActivator: func(ctx context.Context, req FlowInstanceActivationRequest) error {
			err := workflowStore.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:         strings.TrimSpace(req.Instance.InstanceID),
				StorageRef:         strings.TrimSpace(req.Instance.InstancePath),
				EntityID:           strings.TrimSpace(req.Instance.EntityID),
				InstanceKind:       "dynamic_flow",
				ParentFlowID:       strings.TrimSpace(req.Instance.ParentRoute.FlowID),
				ParentFlowInstance: strings.TrimSpace(req.Instance.ParentRoute.FlowInstance),
				ParentEntityID:     strings.TrimSpace(req.Instance.ParentEntityID),
				WorkflowName:       strings.TrimSpace(req.Instance.TemplateID),
				WorkflowVersion:    "v-test",
				CurrentState:       "pending",
				Config:             cloneStringAnyMap(req.Config),
				Fields:             map[string]any{"component_id": req.Config["component_id"]},
				Bookkeeping:        map[string]any{"last_source_event": strings.TrimSpace(req.TriggerEvent.ID())},
				CreatedAt:          time.Now().UTC(),
				UpdatedAt:          time.Now().UTC(),
				EntityType:         "test_entity",
			}))
			if err != nil {
				return fmt.Errorf("activate %s entity %s: %w", req.Instance.InstancePath, req.Instance.EntityID, err)
			}
			return nil
		},
		Module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("root", []WorkflowStage{
				{Name: "pending"},
			}, nil),
			workflowNodes: []WorkflowNode{
				{
					Node:          pipelineNode(t, "", "fanout-node"),
					Subscriptions: []events.EventType{"component_scaffold.batch_requested"},
					Produces:      []events.EventType{"component_scaffold.spawn_requested"},
				},
				{
					Node:          pipelineNode(t, "", "spawn-node"),
					Subscriptions: []events.EventType{"component_scaffold.spawn_requested"},
				},
			},
		},
	})
	configurePipelineTestDeliveryOwner(t, pc)
	return pc, bus
}

func sqliteDynamicActivationBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review"},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"fanout-node": {ID: "fanout-node", ExecutionType: "system_node"},
			"spawn-node":  {ID: "spawn-node", ExecutionType: "system_node"},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*reviewFlow},
				Events: map[string]runtimecontracts.EventCatalogEntry{
					"component_scaffold.batch_requested": {
						Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
							"components": {Type: "[json]"},
						}},
					},
					"component_scaffold.spawn_requested": {
						Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
							"component_id": {Type: "text"},
						}},
					},
				},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": reviewFlow,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {
				Name:         "review",
				Mode:         "template",
				InitialState: "pending",
				States:       []string{"pending"},
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{Events: []string{"component_scaffold.spawn_requested"}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "root", Version: "v-test",
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"fanout-node": {
					"component_scaffold.batch_requested": {
						FanOut: &runtimecontracts.FanOutSpec{
							ItemsFrom: "payload.components",
							As:        "component",
							Identity:  "component.component_id",
							Emit: runtimecontracts.EmitSpec{
								Event: "component_scaffold.spawn_requested",
								Fields: map[string]runtimecontracts.ExpressionValue{
									"component_id": runtimecontracts.CELExpression("component.component_id"),
								},
							},
						},
					},
				},
				"spawn-node": {
					"component_scaffold.spawn_requested": {
						Action: runtimecontracts.ActionSpec{
							ID:             "create_flow_instance",
							Template:       "review",
							InstanceIDFrom: "payload.component_id",
							ConfigFrom: &runtimecontracts.ConfigFromSpec{
								Bindings: map[string]string{
									"component_id": "payload.component_id",
								},
							},
						},
					},
				},
			},
		},
	}
	for _, nodeID := range []string{"fanout-node", "spawn-node"} {
		node := bundle.Nodes[nodeID]
		node.EventHandlers = bundle.Semantics.NodeHandlers[nodeID]
		bundle.Nodes[nodeID] = node
	}
	bundle.FlowTree.Root.Nodes = bundle.Nodes
	bundle.Events = bundle.FlowTree.Root.Events
	return admitSyntheticEntityContractsForTest(t, bundle, "parent", map[string]string{"review": "test_entity"})
}

func assertSQLiteWorkflowInstancePersisted(t *testing.T, store *workflowInstanceStore, ctx context.Context, storageRef string) {
	t.Helper()
	instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(storageRef))
	if err != nil {
		t.Fatalf("load workflow instance %s: %v", storageRef, err)
	}
	if !ok || strings.TrimSpace(instance.StorageRef) != storageRef {
		t.Fatalf("workflow instance %s loaded=%v value=%+v", storageRef, ok, instance)
	}
}
