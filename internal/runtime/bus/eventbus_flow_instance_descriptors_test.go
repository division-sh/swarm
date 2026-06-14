package bus_test

import (
	"context"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type activeFlowInstanceDescriptorStore struct {
	runtimebus.InMemoryEventStore
	agents        []runtimebus.ActiveAgentDescriptor
	flowInstances []runtimebus.ActiveFlowInstanceDescriptor
}

func (s *activeFlowInstanceDescriptorStore) ListActiveAgentDescriptors(context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.agents...), nil
}

func (s *activeFlowInstanceDescriptorStore) ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return append([]runtimebus.ActiveFlowInstanceDescriptor(nil), s.flowInstances...), nil
}

func TestEventBusPinRoutingDescriptorsIncludeActiveDynamicFlowInstances(t *testing.T) {
	const flowInstance = "component-scaffold/aaaaaaaa-1111-4111-8111-aaaaaaaa1111"
	eb, err := runtimebus.NewEventBus(&activeFlowInstanceDescriptorStore{
		agents: []runtimebus.ActiveAgentDescriptor{{
			AgentID:      "service-owner",
			EntityID:     "service-ent",
			FlowInstance: "service-owner/root",
		}},
		flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{{
			FlowInstance: flowInstance,
			FlowTemplate: "component-scaffold",
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	descriptors, err := eb.PinRoutingDescriptors(context.Background())
	if err != nil {
		t.Fatalf("PinRoutingDescriptors: %v", err)
	}
	var foundFlow, foundAgent bool
	for _, descriptor := range descriptors {
		switch descriptor.FlowInstance {
		case flowInstance:
			foundFlow = descriptor.EntityID == runtimeflowidentity.EntityID(flowInstance)
		case "service-owner/root":
			foundAgent = descriptor.EntityID == "service-ent"
		}
	}
	if !foundFlow {
		t.Fatalf("PinRoutingDescriptors = %#v, want active flow instance descriptor for %s", descriptors, flowInstance)
	}
	if !foundAgent {
		t.Fatalf("PinRoutingDescriptors = %#v, want active agent descriptor preserved", descriptors)
	}
}
