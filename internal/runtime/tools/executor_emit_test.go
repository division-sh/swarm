package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

type emitWorkflowInstanceLoader struct {
	rows map[string]runtimepipeline.WorkflowInstance
	err  error
}

func (l emitWorkflowInstanceLoader) Enabled() bool { return true }

func (l emitWorkflowInstanceLoader) Load(_ context.Context, ref string) (runtimepipeline.WorkflowInstance, bool, error) {
	if l.err != nil {
		return runtimepipeline.WorkflowInstance{}, false, l.err
	}
	instance, ok := l.rows[strings.TrimSpace(ref)]
	return instance, ok, nil
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

func TestHandleEmitTool_PreservesInboundChildFlowOwnerWithinActorScope(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"research.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"summary": {Type: "string"},
					},
					Required: []string{"summary"},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "validation",
						Flow: "validation",
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"research.completed": {},
					},
					Path: "validation",
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "business-research-agent",
		Role:       "business_research",
		Mode:       "validation",
		FlowPath:   "validation",
		EmitEvents: []string{"research.completed"},
	}
	inbound := (events.Event{
		Type: events.EventType("validation/validation.started"),
	}).WithEntityID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").WithFlowInstance("validation/inst-1")
	ctx := runtimebus.WithInboundEvent(context.Background(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_research_completed", map[string]any{
		"summary": "research done",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if got, want := bus.event.EntityID(), "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"; got != want {
		t.Fatalf("published event entity_id = %q, want %q", got, want)
	}
	if got, want := bus.event.FlowInstance(), "validation/inst-1"; got != want {
		t.Fatalf("published event flow_instance = %q, want %q", got, want)
	}
}

func TestHandleEmitTool_DoesNotAdoptForeignInboundFlowOwner(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"research.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"summary": {Type: "string"},
					},
					Required: []string{"summary"},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "validation",
						Flow: "validation",
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"research.completed": {},
					},
					Path: "validation",
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "business-research-agent",
		Role:       "business_research",
		Mode:       "validation",
		FlowPath:   "validation",
		EmitEvents: []string{"research.completed"},
	}
	inbound := (events.Event{
		Type: events.EventType("scoring/vertical.shortlisted"),
	}).WithEntityID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").WithFlowInstance("scoring/inst-1")
	ctx := runtimebus.WithInboundEvent(context.Background(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_research_completed", map[string]any{
		"summary": "research done",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if got, want := bus.event.FlowInstance(), "validation"; got != want {
		t.Fatalf("published event flow_instance = %q, want %q", got, want)
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

func TestHandleEmitTool_TargetsParentRouteForChildPinOutput(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	analyzerFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analyzer-flow",
			Flow: "analyzer-flow",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"analysis.done"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {},
		},
		Path: "analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{
			Children: []runtimecontracts.FlowContractView{analyzerFlow},
		},
		ByID: map[string]*runtimecontracts.FlowContractView{
			"analyzer-flow": &analyzerFlow,
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	parentRoute := events.RouteIdentity{
		FlowID:       "root",
		FlowInstance: "root",
		EntityID:     "11111111-1111-1111-1111-111111111111",
	}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{
		WorkflowSource: source,
		EmitRegistry:   emitRegistry,
		WorkflowInstances: emitWorkflowInstanceLoader{rows: map[string]runtimepipeline.WorkflowInstance{
			"analyzer-flow/inst-1": {
				Metadata: map[string]any{
					"parent_flow_id":       parentRoute.FlowID,
					"parent_flow_instance": parentRoute.FlowInstance,
					"parent_entity_id":     parentRoute.EntityID,
				},
			},
		}},
	})
	actor := models.AgentConfig{
		ID:         "analyzer",
		Role:       "analyzer",
		Mode:       "analyzer-flow",
		FlowPath:   "analyzer-flow/inst-1",
		EntityID:   "22222222-2222-2222-2222-222222222222",
		EmitEvents: []string{"analyzer-flow/analysis.done"},
	}
	childRoute := events.RouteIdentity{
		FlowID:       "analyzer-flow",
		FlowInstance: "analyzer-flow/inst-1",
		EntityID:     "22222222-2222-2222-2222-222222222222",
	}
	wrongInboundParent := events.RouteIdentity{
		FlowID:       "wrong-root",
		FlowInstance: "wrong-root",
		EntityID:     "33333333-3333-3333-3333-333333333333",
	}
	inbound := (events.Event{
		Type: events.EventType("analyzer-flow/analysis.requested"),
	}).WithSourceRoute(wrongInboundParent).WithTargetRoute(childRoute)
	ctx := runtimebus.WithInboundEvent(context.Background(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if got := bus.event.TargetRoute(); got != parentRoute {
		t.Fatalf("target route = %#v, want parent route %#v", got, parentRoute)
	}
	if got := bus.event.SourceRoute(); got.Empty() || got.FlowID != "analyzer-flow" || got.FlowInstance != "analyzer-flow/inst-1" {
		t.Fatalf("source route = %#v, want analyzer-flow/inst-1 source", got)
	}
}

func TestHandleEmitTool_FailsClosedOnIncompleteStoredParentRoute(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	analyzerFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analyzer-flow",
			Flow: "analyzer-flow",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"analysis.done"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {},
		},
		Path: "analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{
			Children: []runtimecontracts.FlowContractView{analyzerFlow},
		},
		ByID: map[string]*runtimecontracts.FlowContractView{
			"analyzer-flow": &analyzerFlow,
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{
		WorkflowSource: source,
		EmitRegistry:   emitRegistry,
		WorkflowInstances: emitWorkflowInstanceLoader{rows: map[string]runtimepipeline.WorkflowInstance{
			"analyzer-flow/inst-1": {
				Metadata: map[string]any{
					"parent_flow_id":   "root",
					"parent_entity_id": "11111111-1111-1111-1111-111111111111",
				},
			},
		}},
	})
	actor := models.AgentConfig{
		ID:         "analyzer",
		Role:       "analyzer",
		Mode:       "analyzer-flow",
		FlowPath:   "analyzer-flow/inst-1",
		EntityID:   "22222222-2222-2222-2222-222222222222",
		EmitEvents: []string{"analyzer-flow/analysis.done"},
	}
	ctx := runtimebus.WithInboundEvent(context.Background(), events.Event{
		Type: events.EventType("analyzer-flow/analysis.requested"),
	})

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err == nil {
		t.Fatal("handleEmitTool error = nil, want parent_route_incomplete")
	}
	if !strings.Contains(err.Error(), "parent_route_incomplete") {
		t.Fatalf("handleEmitTool error = %v, want parent_route_incomplete", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_StaticChildPinOutputTargetsDeliveryEntity(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	analyzerFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analyzer-flow",
			Flow: "analyzer-flow",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"analysis.done"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {},
		},
		Path: "root/analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{
			Children: []runtimecontracts.FlowContractView{analyzerFlow},
		},
		ByID: map[string]*runtimecontracts.FlowContractView{
			"analyzer-flow": &analyzerFlow,
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "analyzer",
		Role:       "analyzer",
		Mode:       "analyzer-flow",
		FlowPath:   "root/analyzer-flow",
		EmitEvents: []string{"analyzer-flow/analysis.done"},
	}
	inbound := (events.Event{
		Type: events.EventType("analyzer-flow/analysis.requested"),
	}).WithEntityID("11111111-1111-1111-1111-111111111111").WithSourceRoute(events.RouteIdentity{
		FlowID:       "wrong-root",
		FlowInstance: "wrong-root",
		EntityID:     "33333333-3333-3333-3333-333333333333",
	})
	ctx := runtimebus.WithInboundEvent(context.Background(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	want := events.RouteIdentity{EntityID: "11111111-1111-1111-1111-111111111111"}
	if got := bus.event.TargetRoute(); got != want {
		t.Fatalf("target route = %#v, want delivery entity route %#v", got, want)
	}
}

func TestHandleEmitTool_RootStaticPinOutputStillRequiresTarget(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	analyzerFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analyzer-flow",
			Flow: "analyzer-flow",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"analysis.done"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {},
		},
		Path: "analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{
			Children: []runtimecontracts.FlowContractView{analyzerFlow},
		},
		ByID: map[string]*runtimecontracts.FlowContractView{
			"analyzer-flow": &analyzerFlow,
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "analyzer",
		Role:       "analyzer",
		Mode:       "analyzer-flow",
		FlowPath:   "analyzer-flow",
		EmitEvents: []string{"analyzer-flow/analysis.done"},
	}
	inbound := (events.Event{
		Type: events.EventType("analyzer-flow/analysis.requested"),
	}).WithEntityID("11111111-1111-1111-1111-111111111111")
	ctx := runtimebus.WithInboundEvent(context.Background(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err == nil {
		t.Fatal("handleEmitTool error = nil, want target_required_missing")
	}
	if !strings.Contains(err.Error(), "target_required_missing") {
		t.Fatalf("handleEmitTool error = %v, want target_required_missing", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_RootSchemaPinOutputStillRequiresTarget(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"root.ready"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"root.ready": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "root-agent",
		Role:       "root-agent",
		EmitEvents: []string{"root.ready"},
	}

	_, err := exec.handleEmitTool(context.Background(), actor, "emit_root_ready", map[string]any{})
	if err == nil {
		t.Fatal("handleEmitTool error = nil, want target_required_missing")
	}
	if !strings.Contains(err.Error(), "target_required_missing") {
		t.Fatalf("handleEmitTool error = %v, want target_required_missing", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
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

func TestHandleEmitTool_AllowsDeclaredTemplateIDBusinessPayload(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"repo.template.selected": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"template_id": {Type: "string"},
					},
					Required: []string{"template_id"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ID:         "repo-agent",
		Role:       "repo_agent",
		EmitEvents: []string{"repo.template.selected"},
	}

	_, err := exec.handleEmitTool(context.Background(), actor, "emit_repo_template_selected", map[string]any{
		"template_id": "application-basic-v1",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.event.Payload, &payload); err != nil {
		t.Fatalf("json.Unmarshal payload: %v", err)
	}
	if got := payload["template_id"]; got != "application-basic-v1" {
		t.Fatalf("payload template_id = %#v, want business value", got)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
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
