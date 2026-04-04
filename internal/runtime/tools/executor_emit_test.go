package tools

import (
	"context"
	"encoding/json"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
)

type publishBusCapture struct {
	event events.Event
}

func (b *publishBusCapture) Publish(_ context.Context, evt events.Event) error {
	b.event = evt
	return nil
}

func (b *publishBusCapture) PublishDirect(_ context.Context, evt events.Event, _ []string) error {
	b.event = evt
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
	InitEventSchemaRegistry(source)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	actorConfig, err := json.Marshal(map[string]any{
		"flow_path":   "discovery",
		"emit_events": []string{"category.assessed"},
	})
	if err != nil {
		t.Fatalf("json.Marshal actor config: %v", err)
	}
	actor := models.AgentConfig{
		ID:     "market-research-agent",
		Role:   "market_research",
		Mode:   "discovery",
		Config: actorConfig,
	}

	_, err = exec.handleEmitTool(context.Background(), actor, "emit_category_assessed", map[string]any{
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
	InitEventSchemaRegistry(source)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})
	actorConfig, err := json.Marshal(map[string]any{
		"flow_path":   "discovery",
		"emit_events": []string{"vertical.discovered"},
	})
	if err != nil {
		t.Fatalf("json.Marshal actor config: %v", err)
	}
	actor := models.AgentConfig{
		ID:     "discovery-coordinator",
		Role:   "discovery_coordinator",
		Mode:   "discovery",
		Config: actorConfig,
	}

	_, err = exec.handleEmitTool(context.Background(), actor, "emit_vertical_discovered", map[string]any{
		"name": "Law firm AP automation",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}

	if got, want := string(bus.event.Type), "discovery/vertical.discovered"; got != want {
		t.Fatalf("published event type = %q, want %q", got, want)
	}
}
