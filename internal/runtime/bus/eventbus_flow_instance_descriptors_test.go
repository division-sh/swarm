package bus_test

import (
	"context"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	const workflowVersion = "v-test"
	eb, err := newScopedTestEventBus(&activeFlowInstanceDescriptorStore{
		agents: []runtimebus.ActiveAgentDescriptor{
			testActiveAgentDescriptor(t, "service-owner", "service-ent", "service-owner/root"),
		},
		flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{{
			EntityID:        "component-owner",
			FlowInstance:    flowInstance,
			FlowTemplate:    "component-scaffold",
			BundleHash:      authorActivityTestBundleHash,
			WorkflowVersion: workflowVersion,
		}},
	}, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Semantics: runtimecontracts.WorkflowSemanticView{Version: workflowVersion},
		}),
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
			foundFlow = descriptor.EntityID == "component-owner"
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
