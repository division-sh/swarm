package pipeline

import (
	"context"
	"testing"

	"empireai/internal/events"
)

func TestEmpirePipelineWorkflowNodes_ExposeSubscriptions(t *testing.T) {
	subs := empirePipelineSubscriptions()
	if len(subs) == 0 {
		t.Fatal("expected subscriptions")
	}
	want := map[events.EventType]struct{}{
		events.EventType("scan.requested"):          {},
		events.EventType("vertical.shortlisted"):    {},
		events.EventType("spec.validation_passed"):  {},
		events.EventType("runtime.reset"):           {},
	}
	for evt := range want {
		found := false
		for _, sub := range subs {
			if sub == evt {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing subscription %s", evt)
		}
	}
}

func TestEmpirePipelineWorkflowNodes_ExposePolicies(t *testing.T) {
	policy, ok := empirePipelineEventPolicy("brand.revision_needed")
	if !ok {
		t.Fatal("expected brand.revision_needed policy")
	}
	if policy.Consume {
		t.Fatal("brand.revision_needed should remain visible downstream")
	}
	if !policy.RequireVertical {
		t.Fatal("brand.revision_needed should require vertical_id")
	}

	policy, ok = empirePipelineEventPolicy("category.assessed")
	if !ok || !policy.Consume {
		t.Fatalf("expected category.assessed consume policy, got ok=%v consume=%v", ok, policy.Consume)
	}
}

func TestEmpirePipelineWorkflowNodes_CoverValidationAndScanEdgeEvents(t *testing.T) {
	for _, eventType := range []string{
		"cto.spec_vetoed",
		"opco.ceo_ready",
		"synthesis.resolved",
		"trend_research.scan_complete",
	} {
		policy, ok := empirePipelineEventPolicy(eventType)
		if !ok {
			t.Fatalf("expected policy for %s", eventType)
		}
		switch eventType {
		case "opco.ceo_ready":
			if policy.Consume {
				t.Fatalf("%s should remain visible downstream", eventType)
			}
		default:
			if !policy.Consume {
				t.Fatalf("%s should be consumed by workflow node", eventType)
			}
		}
	}
}

func TestFactoryPipelineCoordinator_DispatchWorkflowNodeEvent(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	if handled := pc.dispatchWorkflowNodeEvent(context.Background(), events.Event{Type: events.EventType("synthesis.resolved")}); !handled {
		t.Fatal("expected synthesis.resolved to be handled by workflow node executor")
	}
	if handled := pc.dispatchWorkflowNodeEvent(context.Background(), events.Event{Type: events.EventType("unknown.event")}); handled {
		t.Fatal("expected unknown.event to remain unhandled")
	}
}
