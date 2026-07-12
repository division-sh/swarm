package pipeline

import (
	"context"
	"database/sql"
	"github.com/division-sh/swarm/internal/testutil"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

func TestSQLiteFanOutCreateFlowInstanceDeliveriesPersistWithoutDeadLetter(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t, testutil.SQLiteFreshFile())
	workflowStore := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ctx := sqliteExactOnceRunContext(t, db)
	pc, bus := newSQLiteDynamicActivationCoordinator(t, db, workflowStore)

	parent := eventtest.RootIngress(
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
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "parent-ent"), "root/parent"),
		time.Now().UTC(),
	)

	if err := workflowStore.Create(ctx, WorkflowInstance{
		InstanceID:      "parent-ent",
		StorageRef:      "parent-ent",
		WorkflowName:    "root",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Metadata:        map[string]any{"entity_type": "parent"},
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed parent workflow instance: %v", err)
	}
	seedExactOnceEventDelivery(t, workflowStore, ctx, parent, "fanout-node")

	handled, err := pc.dispatchWorkflowNodeEventResult(ctx, parent)
	if err != nil {
		t.Fatalf("dispatch parent fan-out event: %v", err)
	}
	if !handled {
		t.Fatal("parent fan-out dispatch handled=false, want true")
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("published child events = %d, want 2", got)
	}
	assertDeliveryStatusCount(t, workflowStore, ctx, parent.ID(), "fanout-node", "delivered", 1)

	children := []events.Event{bus.publishedEvent(0), bus.publishedEvent(1)}
	for idx, child := range children {
		if got := strings.TrimSpace(child.ParentEventID()); got != parent.ID() {
			t.Fatalf("child %s parent_event_id = %q, want %q", child.ID(), got, parent.ID())
		}
		childID := strings.TrimSpace(child.ID())
		if childID == "" {
			childID = uuid.NewString()
		}
		childRunID := strings.TrimSpace(child.RunID())
		if childRunID == "" {
			childRunID = runtimecorrelation.RunIDFromContext(ctx)
		}
		child = eventtest.RootIngress(
			childID,
			child.Type(),
			child.SourceAgent(),
			child.TaskID(),
			child.Payload(),
			child.ChainDepth(),
			childRunID,
			child.ParentEventID(),
			child.Envelope(),
			child.CreatedAt())

		children[idx] = child
		seedExactOnceEventDelivery(t, workflowStore, ctx, child, "spawn-node")
	}

	for _, child := range children {
		handled, err := pc.dispatchWorkflowNodeEventResult(ctx, child)
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
		assertDeliveryStatusCount(t, workflowStore, ctx, child.ID(), "spawn-node", "delivered", 1)
		assertDeliveryStatusCount(t, workflowStore, ctx, child.ID(), "spawn-node", "dead_letter", 0)
	}
	if logs := bus.runtimeLogEntries(); len(logs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", logs)
	}
}

func newSQLiteDynamicActivationCoordinator(t *testing.T, db *sql.DB, workflowStore *WorkflowInstanceStore) (*PipelineCoordinator, *recordingPipelineBus) {
	t.Helper()
	bus := &recordingPipelineBus{}
	bundle := sqliteDynamicActivationBundle()
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		WorkflowStore: workflowStore,
		InstanceActivator: func(ctx context.Context, req FlowInstanceActivationRequest) error {
			return workflowStore.Create(ctx, WorkflowInstance{
				InstanceID:      strings.TrimSpace(req.Instance.InstanceID),
				StorageRef:      strings.TrimSpace(req.Instance.InstancePath),
				WorkflowName:    strings.TrimSpace(req.Instance.TemplateID),
				WorkflowVersion: "v-test",
				CurrentState:    "pending",
				Config:          cloneStringAnyMap(req.Config),
				Metadata: map[string]any{
					"component_id":         req.Config["component_id"],
					"instance_kind":        "dynamic_flow",
					"last_source_event":    strings.TrimSpace(req.TriggerEvent.ID()),
					"parent_entity_id":     strings.TrimSpace(req.Instance.ParentEntityID),
					"parent_flow_id":       strings.TrimSpace(req.Instance.ParentRoute.FlowID),
					"parent_flow_instance": strings.TrimSpace(req.Instance.ParentRoute.FlowInstance),
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			})
		},
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
		Module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("root", []WorkflowStage{
				{Name: "pending"},
			}, nil),
			workflowNodes: []WorkflowNode{
				{
					ID:            "fanout-node",
					Subscriptions: []events.EventType{"component_scaffold.batch_requested"},
					Produces:      []events.EventType{"component_scaffold.spawn_requested"},
				},
				{
					ID:            "spawn-node",
					Subscriptions: []events.EventType{"component_scaffold.spawn_requested"},
				},
			},
		},
	})
	return pc, bus
}

func sqliteDynamicActivationBundle() *runtimecontracts.WorkflowContractBundle {
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review"},
	}
	return &runtimecontracts.WorkflowContractBundle{
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
			Version: "v-test",
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
}

func assertSQLiteWorkflowInstancePersisted(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, storageRef string) {
	t.Helper()
	instance, ok, err := store.Load(ctx, storageRef)
	if err != nil {
		t.Fatalf("load workflow instance %s: %v", storageRef, err)
	}
	if !ok || strings.TrimSpace(instance.StorageRef) != storageRef {
		t.Fatalf("workflow instance %s loaded=%v value=%+v", storageRef, ok, instance)
	}
}
