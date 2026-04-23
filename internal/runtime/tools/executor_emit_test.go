package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
)

type publishBusCapture struct {
	event events.Event
	count int
}

func (b *publishBusCapture) Publish(_ context.Context, evt events.Event) error {
	b.event = evt
	b.count++
	return nil
}

func (b *publishBusCapture) PublishDirect(_ context.Context, evt events.Event, _ []string) error {
	b.event = evt
	b.count++
	return nil
}

func TestHandleEmitTool_PreservesPayloadForFlowScopedEmit(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category":  {Type: "string"},
						"signal_id": {Type: "string"},
					},
					Required: []string{"category", "signal_id"},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "discovery",
						Flow: "discovery",
					},
					Schema: runtimecontracts.FlowSchemaDocument{
						Pins: runtimecontracts.FlowPins{},
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"category.assessed": {},
					},
					Path: "discovery",
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "market-research-agent",
		Role:       "market_research",
		Mode:       "discovery",
		FlowPath:   "discovery",
		EmitEvents: []string{"category.assessed"},
	}

	_, err := exec.handleEmitTool(context.Background(), actor, "emit_category_assessed", map[string]any{
		"category":  "AP automation",
		"signal_id": "sig-1",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}

	if got, want := string(bus.event.Type), "discovery/category.assessed"; got != want {
		t.Fatalf("published event type = %q, want %q", got, want)
	}

	var payload map[string]any
	if err := json.Unmarshal(bus.event.Payload, &payload); err != nil {
		t.Fatalf("json.Unmarshal payload: %v", err)
	}
	if got, want := payload["category"], "AP automation"; got != want {
		t.Fatalf("payload category = %#v, want %q", got, want)
	}
	if got, want := payload["signal_id"], "sig-1"; got != want {
		t.Fatalf("payload signal_id = %#v, want %q", got, want)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
	}
}

func TestHandleEmitTool_KeepsFlowOutputPinAtParentScope(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"vertical.discovered": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"name": {Type: "string"},
					},
					Required: []string{"name"},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "discovery",
						Flow: "discovery",
					},
					Schema: runtimecontracts.FlowSchemaDocument{
						Pins: runtimecontracts.FlowPins{
							Outputs: runtimecontracts.FlowOutputPins{
								Events: []string{"vertical.discovered"},
							},
						},
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"vertical.discovered": {},
					},
					Path: "discovery",
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "discovery-coordinator",
		Role:       "discovery_coordinator",
		Mode:       "discovery",
		FlowPath:   "discovery",
		EmitEvents: []string{"vertical.discovered"},
	}

	_, err := exec.handleEmitTool(context.Background(), actor, "emit_vertical_discovered", map[string]any{
		"name": "Law firm AP automation",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}

	if got, want := string(bus.event.Type), "discovery/vertical.discovered"; got != want {
		t.Fatalf("published event type = %q, want %q", got, want)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
	}
}

func TestHandleEmitTool_FailsClosedOnUndeclaredPayloadField(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
					},
					Required: []string{"category"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "market-research-agent",
		Role:       "market_research",
		EmitEvents: []string{"category.assessed"},
	}

	_, err := exec.handleEmitTool(context.Background(), actor, "emit_category_assessed", map[string]any{
		"category":   "AP automation",
		"unexpected": true,
	})
	if err == nil {
		t.Fatal("expected undeclared payload field failure")
	}
	if !strings.Contains(err.Error(), "$.unexpected is not allowed") {
		t.Fatalf("error = %v, want undeclared-field validation detail", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_AllowsValidWave1EventPayloadTypes(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Scalars: map[string]runtimecontracts.ScalarTypeDecl{
				"TraceID": {Base: "uuid"},
				"Label":   {Base: "text"},
			},
			Enums: map[string]runtimecontracts.EnumTypeDecl{
				"Mode": {Values: []string{"fast", "deep"}},
			},
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"ScanDetails": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"source": {Type: "text"},
						"count":  {Type: "integer"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"mode":     {Type: "Mode"},
						"details":  {Type: "ScanDetails"},
						"labels":   {Type: "[Label]"},
						"trace_id": {Type: "TraceID"},
					},
					Required: []string{"mode", "details", "labels", "trace_id"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "market-research-agent",
		Role:       "market_research",
		EmitEvents: []string{"scan.completed"},
	}

	_, err := exec.handleEmitTool(context.Background(), actor, "emit_scan_completed", map[string]any{
		"mode": "fast",
		"details": map[string]any{
			"source": "scanner-a",
			"count":  2,
		},
		"labels":   []any{"a", "b"},
		"trace_id": "11111111-1111-1111-1111-111111111111",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
	}
}

func TestHandleEmitTool_ResolvesDuplicateLeafScopedSchemasThroughActor(t *testing.T) {
	reviewFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"details": {Type: "ReviewRequest"},
					},
					Required: []string{"details"},
				},
			},
		},
	}
	validationFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Path:  "validation",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"details": {Type: "ValidationRequest"},
					},
					Required: []string{"details"},
				},
			},
		},
	}
	root := &runtimecontracts.FlowContractView{
		Children: []runtimecontracts.FlowContractView{reviewFlow, validationFlow},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Enums: map[string]runtimecontracts.EnumTypeDecl{
				"ReviewPriority":     {Values: []string{"urgent"}},
				"ValidationPriority": {Values: []string{"low"}},
			},
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"ReviewRequest": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"priority": {Type: "ReviewPriority"},
					},
				},
				"ValidationRequest": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"priority": {Type: "ValidationPriority"},
					},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})

	reviewActor := models.AgentConfig{
		ID:         "review-agent",
		Role:       "reviewer",
		Mode:       "review",
		FlowPath:   "review",
		EmitEvents: []string{"review/task.requested"},
	}
	_, err := exec.handleEmitTool(context.Background(), reviewActor, "emit_task_requested", map[string]any{
		"details": map[string]any{
			"priority": "urgent",
		},
	})
	if err != nil {
		t.Fatalf("review handleEmitTool: %v", err)
	}
	if got, want := string(bus.event.Type), "review/task.requested"; got != want {
		t.Fatalf("review published event type = %q, want %q", got, want)
	}

	validationActor := models.AgentConfig{
		ID:         "validation-agent",
		Role:       "validator",
		Mode:       "validation",
		FlowPath:   "validation",
		EmitEvents: []string{"validation/task.requested"},
	}
	_, err = exec.handleEmitTool(context.Background(), validationActor, "emit_task_requested", map[string]any{
		"details": map[string]any{
			"priority": "low",
		},
	})
	if err != nil {
		t.Fatalf("validation handleEmitTool: %v", err)
	}
	if got, want := string(bus.event.Type), "validation/task.requested"; got != want {
		t.Fatalf("validation published event type = %q, want %q", got, want)
	}
	if bus.count != 2 {
		t.Fatalf("publish count = %d, want 2", bus.count)
	}
}

func TestHandleEmitTool_FailsClosedOnSameActorDuplicateLeafScopedSchemas(t *testing.T) {
	reviewFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"priority": {Type: "string"},
					},
					Required: []string{"priority"},
				},
			},
		},
	}
	validationFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Path:  "validation",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"priority": {Type: "string"},
					},
					Required: []string{"priority"},
				},
			},
		},
	}
	root := &runtimecontracts.FlowContractView{
		Children: []runtimecontracts.FlowContractView{reviewFlow, validationFlow},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "dual-scope-agent",
		Role:       "reviewer",
		Mode:       "review",
		FlowPath:   "review",
		EmitEvents: []string{"review/task.requested", "validation/task.requested"},
	}

	_, err := exec.handleEmitTool(context.Background(), actor, "emit_task_requested", map[string]any{
		"priority": "urgent",
	})
	if err == nil {
		t.Fatal("expected same-actor duplicate local tool name collision to fail closed")
	}
	if !strings.Contains(err.Error(), "invalid emit tool name") {
		t.Fatalf("error = %v, want invalid emit tool name", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_FailsClosedOnNamedTypeViolation(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"ScanDetails": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"source": {Type: "text"},
						"count":  {Type: "integer"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"details": {Type: "ScanDetails"},
					},
					Required: []string{"details"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "market-research-agent",
		Role:       "market_research",
		EmitEvents: []string{"scan.completed"},
	}

	_, err := exec.handleEmitTool(context.Background(), actor, "emit_scan_completed", map[string]any{
		"details": "not-an-object",
	})
	if err == nil {
		t.Fatal("expected named-type payload violation")
	}
	if !strings.Contains(err.Error(), "$.details must be object") {
		t.Fatalf("handleEmitTool error = %v, want named-type detail", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}
