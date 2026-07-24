package manager

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type subscriptionAdmissionTestAgent struct{ id string }

func (a subscriptionAdmissionTestAgent) ID() string                      { return a.id }
func (subscriptionAdmissionTestAgent) Type() string                      { return "test" }
func (subscriptionAdmissionTestAgent) Subscriptions() []events.EventType { return nil }
func (subscriptionAdmissionTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func testManagerSubscriptionAdmission(t *testing.T, cfg runtimeactors.AgentConfig) semanticview.FlowOwnedAgentSubscriptionAdmission {
	t.Helper()
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: cfg.ID, FlowID: cfg.FlowID, FlowPath: cfg.CanonicalFlowPath(), Subscriptions: cfg.Subscriptions,
	})
	if err != nil {
		t.Fatalf("admit test manager subscriptions: %v", err)
	}
	return admission
}

func TestSpawnAgentRejectsForeignExactAndPatternBeforeRegistration(t *testing.T) {
	for _, subscription := range []string{"foreign/task.ready", "foreign/**/task.ready"} {
		t.Run(strings.ReplaceAll(subscription, "/", "_"), func(t *testing.T) {
			eb, err := runtimebus.NewEphemeralEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{WorkOwner: newTestManagerWorkOwner(t)})
			if err != nil {
				t.Fatal(err)
			}
			am := newTestAgentManager(t, eb, func(cfg runtimeactors.AgentConfig) (Agent, error) {
				return subscriptionAdmissionTestAgent{id: cfg.ID}, nil
			})
			err = am.SpawnAgent(runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "reviewer",
				FlowPath:      "review/inst-1",
				Subscriptions: []string{subscription},
			})
			if err == nil || !strings.Contains(err.Error(), "cannot cross a flow boundary") {
				t.Fatalf("SpawnAgent error = %v, want admission rejection", err)
			}
			if am.Count() != 0 {
				t.Fatalf("registered agent count = %d, want zero", am.Count())
			}
		})
	}
}

func TestReconfigureAgentRejectsForeignSubscriptionWithoutReplacingCurrentAdmission(t *testing.T) {
	eb, err := runtimebus.NewEphemeralEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{WorkOwner: newTestManagerWorkOwner(t)})
	if err != nil {
		t.Fatal(err)
	}
	am := newTestAgentManager(t, eb, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		return subscriptionAdmissionTestAgent{id: cfg.ID}, nil
	})
	initial := runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "reviewer",
		FlowPath:      "review/inst-1",
		Subscriptions: []string{"task.ready"},
	}
	if err := am.SpawnAgent(initial); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	before, ok := am.lifecycle.executionSnapshot(initial.ID)
	if !ok {
		t.Fatal("initial execution missing")
	}

	err = am.ReconfigureAgent(initial.ID, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		Subscriptions: []string{"foreign/**/task.ready"},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot cross a flow boundary") {
		t.Fatalf("ReconfigureAgent error = %v, want admission rejection", err)
	}
	after, ok := am.lifecycle.executionSnapshot(initial.ID)
	if !ok {
		t.Fatal("execution disappeared after rejected reconfigure")
	}
	if after.Token != before.Token || !reflect.DeepEqual(after.Config, before.Config) || !reflect.DeepEqual(after.Admission.RoutePatterns(), before.Admission.RoutePatterns()) {
		t.Fatalf("rejected reconfigure changed execution: before=%#v after=%#v", before, after)
	}
}
