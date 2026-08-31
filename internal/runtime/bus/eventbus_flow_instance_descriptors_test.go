package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type activeFlowInstanceDescriptorStore struct {
	runtimebus.InMemoryEventStore
	agents        []runtimebus.ActiveAgentDescriptor
	flowInstances []runtimebus.ActiveFlowInstanceDescriptor
}

func (s *activeFlowInstanceDescriptorStore) ListActiveAgentDescriptors(context.Context, string) ([]runtimebus.ActiveAgentDescriptor, error) {
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.agents...), nil
}

func (s *activeFlowInstanceDescriptorStore) ListActiveFlowInstanceDescriptors(context.Context, string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return append([]runtimebus.ActiveFlowInstanceDescriptor(nil), s.flowInstances...), nil
}

func TestEventBusPinRoutingDescriptorsIncludeActiveDynamicFlowInstances(t *testing.T) {
	const runID = "99999999-9999-9999-9999-999999999999"
	const flowInstance = "component-scaffold/aaaaaaaa-1111-4111-8111-aaaaaaaa1111"
	const workflowVersion = "v-test"
	eb, err := newScopedTestEventBus(&activeFlowInstanceDescriptorStore{
		agents: []runtimebus.ActiveAgentDescriptor{
			testActiveAgentDescriptorForRun(t, runID, "service-owner", "service-ent", "service-owner/root"),
		},
		flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{{
			RunID:        runID,
			EntityID:     "component-owner",
			FlowInstance: flowInstance,
			FlowTemplate: "component-scaffold",
			BundleHash:   authorActivityTestBundleHash, BundleSource: authorActivityTestBundleSource,
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

	ctx := runtimecorrelation.WithInboundEvent(context.Background(), eventtest.RunCreatingRootIngress(
		eventtest.UUID("active-flow-descriptor"), events.EventType("descriptor.probe"), "", "", nil, 0,
		runID, "", events.EventEnvelope{}, time.Time{},
	))
	descriptors, err := eb.PinRoutingDescriptors(ctx)
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
