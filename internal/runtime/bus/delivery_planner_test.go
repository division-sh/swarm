package bus

import (
	"context"
	"errors"
	"testing"

	"swarm/internal/events"
)

func TestDeliveryRouteResolver_SeparatesRouteResolutionAndDiagnostics(t *testing.T) {
	resolver := deliveryRouteResolver{
		resolveRoutedSubscribers: func(string) []Subscriber {
			return []Subscriber{{
				ID:           "scan-orchestrator",
				Type:         "node",
				Path:         "discovery",
				MatchPattern: "producer/scan.requested",
				RouteSource:  "pin_auto_wire",
			}}
		},
		resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
			return []deliveryRecipientCandidate{
				{ID: "direct-agent", PersistAsDelivery: true},
				{ID: "scan-orchestrator", PersistAsDelivery: false},
				{ID: "direct-agent", PersistAsDelivery: true},
			}
		},
		describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
			return []PublishDiagnosticRecipient{{
				ID:             "scan-orchestrator",
				Type:           "node",
				Path:           "discovery",
				MatchedPattern: "producer/scan.requested",
				RouteSource:    "pin_auto_wire",
				LocalizedEvent: "scan.requested",
			}}
		},
	}

	result := resolver.Resolve(events.Event{Type: events.EventType("producer/scan.requested")})
	if got, want := len(result.RoutedRecipients), 1; got != want {
		t.Fatalf("routed recipients = %d, want %d", got, want)
	}
	if got, want := len(result.SubscribedRecipients), 2; got != want {
		t.Fatalf("subscription recipients = %d, want %d", got, want)
	}
	if got, want := len(result.Recipients), 2; got != want {
		t.Fatalf("candidate recipients = %d, want %d", got, want)
	}
	if got := result.Recipients[0].ID; got != "direct-agent" {
		t.Fatalf("first candidate recipient = %q, want direct-agent", got)
	}
	if got := result.Recipients[1].ID; got != "scan-orchestrator" {
		t.Fatalf("second candidate recipient = %q, want scan-orchestrator", got)
	}
	if got := result.ExtraDetail["routed_recipients_count"]; got != 1 {
		t.Fatalf("routed_recipients_count = %#v, want 1", got)
	}
	if got := result.ExtraDetail["subscription_recipients_count"]; got != 3 {
		t.Fatalf("subscription_recipients_count = %#v, want 3", got)
	}
	routed, _ := result.ExtraDetail["routed_recipients"].([]map[string]any)
	if len(routed) != 1 || routed[0]["id"] != "scan-orchestrator" {
		t.Fatalf("routed_recipients detail = %#v", result.ExtraDetail["routed_recipients"])
	}
}

func TestDeliveryRecipientPolicy_FiltersExplicitAgentScopeIntoManifest(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
			return map[string]ActiveAgentDescriptor{
				"entity-agent": {AgentID: "entity-agent", EntityID: "ent-1"},
				"other-agent":  {AgentID: "other-agent", EntityID: "ent-2"},
				"shared-agent": {AgentID: "shared-agent"},
			}, true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), (events.Event{
		Type: "task.completed",
	}).WithEntityID("ent-1"), agentDeliveryRecipientCandidates([]string{"entity-agent", "other-agent", "shared-agent"}))
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if got, want := len(manifest.Recipients), 2; got != want {
		t.Fatalf("recipient count = %d, want %d", got, want)
	}
	if manifest.Recipients[0] != "entity-agent" || manifest.Recipients[1] != "shared-agent" {
		t.Fatalf("recipients = %#v, want [entity-agent shared-agent]", manifest.Recipients)
	}
	if len(manifest.PersistedRecipients) != 2 || manifest.PersistedRecipients[0] != "entity-agent" || manifest.PersistedRecipients[1] != "shared-agent" {
		t.Fatalf("persisted recipients = %#v, want [entity-agent shared-agent]", manifest.PersistedRecipients)
	}
}

func TestDeliveryRecipientPolicy_KeepsInternalSubscribersLiveOnlyUnderDescriptorPlanning(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
			return map[string]ActiveAgentDescriptor{
				"agent-a": {AgentID: "agent-a"},
			}, true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), events.Event{Type: "task.completed"}, []deliveryRecipientCandidate{
		{ID: "workflow-runtime", PersistAsDelivery: false},
		{ID: "node:scan-orchestrator", PersistAsDelivery: false},
		{ID: "agent-a", PersistAsDelivery: true},
		{ID: "missing-agent", PersistAsDelivery: true},
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(manifest.Recipients) != 3 || manifest.Recipients[0] != "workflow-runtime" || manifest.Recipients[1] != "node:scan-orchestrator" || manifest.Recipients[2] != "agent-a" {
		t.Fatalf("recipients = %#v, want [workflow-runtime node:scan-orchestrator agent-a]", manifest.Recipients)
	}
	if len(manifest.PersistedRecipients) != 1 || manifest.PersistedRecipients[0] != "agent-a" {
		t.Fatalf("persisted recipients = %#v, want [agent-a]", manifest.PersistedRecipients)
	}
}

func TestDeliveryPlanner_ComposesRoutingPolicyAndManifest(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(string) []Subscriber {
				return []Subscriber{{ID: "worker", Type: "node"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{
					{ID: "worker", PersistAsDelivery: false},
					{ID: "observer", PersistAsDelivery: true},
				}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "worker", Type: "node"}}
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return nil, false, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), events.Event{Type: "task.completed"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got, want := len(plan.Recipients), 2; got != want {
		t.Fatalf("plan recipients = %d, want %d", got, want)
	}
	if got, want := len(plan.PersistedRecipients), 1; got != want {
		t.Fatalf("persisted recipients = %d, want %d", got, want)
	}
	if got := plan.PersistedRecipients[0]; got != "observer" {
		t.Fatalf("persisted recipient = %q, want observer", got)
	}
	if got := plan.ExtraDetail["routed_recipients_count"]; got != 1 {
		t.Fatalf("routed_recipients_count = %#v, want 1", got)
	}
}

func TestDeliveryPlanner_FailsClosedOnPolicyError(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(string) []Subscriber { return nil },
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "worker", PersistAsDelivery: true}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient { return nil },
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return nil, false, errors.New("descriptor store unavailable")
			},
		},
	)

	_, err := planner.Plan(context.Background(), events.Event{Type: "task.completed"})
	if err == nil || err.Error() != "descriptor store unavailable" {
		t.Fatalf("Plan err = %v, want descriptor store unavailable", err)
	}
}
