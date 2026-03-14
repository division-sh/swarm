package manager

import (
	"context"
	"testing"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
)

type flowActivationTestBus struct {
	addedPaths   []string
	removedPairs []string
	published    []events.Event
}

func (b *flowActivationTestBus) Publish(_ context.Context, evt events.Event) error {
	b.published = append(b.published, evt)
	return nil
}

func (*flowActivationTestBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*flowActivationTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*flowActivationTestBus) Unsubscribe(string)                                          {}
func (*flowActivationTestBus) Store() runtimebus.EventStore                                { return nil }
func (*flowActivationTestBus) ResetInMemoryState()                                         {}
func (*flowActivationTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) {}

func (b *flowActivationTestBus) AddFlowInstance(_ runtimecontracts.SystemNodeContract, instancePath string) error {
	b.addedPaths = append(b.addedPaths, instancePath)
	return nil
}

func (b *flowActivationTestBus) RemoveFlowInstance(templateID, instanceID string) error {
	b.removedPairs = append(b.removedPairs, templateID+"/"+instanceID)
	return nil
}

func testFlowBundle(autoEmit string) *runtimecontracts.WorkflowContractBundle {
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"reviewer": {
				ID:            "reviewer-{instance_id}",
				Type:          "generic",
				Role:          "reviewer",
				Subscriptions: []string{"task.started"},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*reviewFlow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": reviewFlow,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {
				Mode: "template",
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{Events: []string{"task.started"}},
				},
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{Event: autoEmit},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}
}

func TestActivateFlowInstanceAddsDerivedRouteTableInstance(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := NewAgentManager(bus, nil)
	bundle := testFlowBundle("")

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
		InitialState:   "queued",
	})
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.addedPaths) != 1 || bus.addedPaths[0] != "review/inst-1" {
		t.Fatalf("added paths = %#v, want [review/inst-1]", bus.addedPaths)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
		t.Fatal("expected activated flow agent config")
	}
}

func TestActivateFlowInstancePublishesAutoEmitEvent(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := NewAgentManager(bus, nil)
	bundle := testFlowBundle("task.started")

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
	})
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	var autoEmit *events.Event
	for idx := range bus.published {
		if string(bus.published[idx].Type) == "review/inst-1/task.started" {
			autoEmit = &bus.published[idx]
			break
		}
	}
	if autoEmit == nil {
		t.Fatalf("published events = %#v, want review/inst-1/task.started", bus.published)
	}
	if got := autoEmit.EntityID(); got != "ent-1" {
		t.Fatalf("published entity_id = %q, want ent-1", got)
	}
}

func TestDeactivateFlowInstanceRemovesAgentsAndRoutes(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := NewAgentManager(bus, nil)
	bundle := testFlowBundle("")

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
	})
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := am.DeactivateFlowInstance(context.Background(), "review", "inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("expected flow agent teardown")
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed pairs = %#v, want [review/inst-1]", bus.removedPairs)
	}
}
