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
