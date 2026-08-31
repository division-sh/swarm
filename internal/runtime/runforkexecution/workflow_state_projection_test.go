package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedContractWorkflowStateProjectionMaterializesTemplateAgentOwner(t *testing.T) {
	eventID := "worker-ready"
	entityID := "entity-1"
	path := "flow_a/instance-1"
	agent := selectedContractTestAgentIdentity(t, "worker-agent", path)
	source := selectedContractActivitySourceWithMode("http://activity.invalid", runtimecontracts.ActivityEffectClassReadOnly, runtimecontracts.FlowModeTemplate)
	blueprints := []runtimemanager.AgentMaterializationBlueprint{{
		Config: runtimeactors.AgentConfig{ID: "worker-agent"}, Identity: mustTestAgentPlan(agent),
	}}
	states, err := selectedContractWorkflowStateProjectionWithReadiness(
		runfork.RunForkPlan{
			SourceRunID: selectedContractAgentTestRunID,
			PendingWork: []runfork.RunForkPendingWork{{
				EventID: eventID,
				DeliveryRoute: events.DeliveryRoute{
					Recipient:     events.MustAgentDeliveryRecipient(agent.AgentID()),
					AgentIdentity: agent,
					Target: events.MustExistingEntityTarget(events.RouteIdentity{
						FlowID: "flow_a", FlowInstance: path, EntityID: entityID,
					}),
				},
			}},
		},
		source,
		runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: eventID,
			Recipients: []runfork.RunForkContractFrontierRecipient{
				testAgentFrontierRecipient(agent.AgentID(), path, "selected_contracts", mustTestAgentPlan(agent)),
			},
		}}},
		map[string]executionmode.Mode{eventID: executionmode.Mock},
		blueprints,
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	if len(states) != 1 || states[0].EntityID != entityID || states[0].FlowID != "flow_a" ||
		states[0].Mode != runtimecontracts.FlowModeTemplate || states[0].Route.InstancePath != path ||
		states[0].ExecutionMode != executionmode.Mock || len(states[0].Agents) != 1 ||
		states[0].Agents[0].Plan.Normalize() != mustTestAgentPlan(agent).Normalize() || states[0].Agents[0].ConfigRevision == "" {
		t.Fatalf("workflow states = %#v, want exact template-agent owner", states)
	}
}

func TestSelectedContractWorkflowStateProjectionUsesPlatformActivityRoutingSourceWithoutNodeRecipient(t *testing.T) {
	entityID := "entity-1"
	plan := runfork.RunForkPlan{PendingWork: []runfork.RunForkPendingWork{{
		EventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
		RoutingSource: eventtest.StaticFlowRoutingSource("flow_a", "flow_a", entityID),
	}}}
	planning := runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
		SourceEventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
	}}}

	states, err := selectedContractWorkflowStateProjection(
		plan,
		selectedContractActivitySource("http://activity.invalid", runtimecontracts.ActivityEffectClassReadOnly),
		planning,
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	if len(states) != 1 || states[0].EntityID != entityID || states[0].FlowID != "flow_a" ||
		states[0].AddressKind != runfork.RunForkSelectedContractWorkflowStateExact ||
		states[0].Route.InstancePath != "flow_a" {
		t.Fatalf("workflow states = %#v, want exact source-owned activity route", states)
	}
}

func TestSelectedContractWorkflowStateProjectionMapsRootPlatformActivityToRunScope(t *testing.T) {
	rootSource, err := events.NewRootRoutingSource("entity-1")
	if err != nil {
		t.Fatalf("NewRootRoutingSource: %v", err)
	}
	states, err := selectedContractWorkflowStateProjection(
		runfork.RunForkPlan{PendingWork: []runfork.RunForkPendingWork{{
			EventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent, RoutingSource: rootSource,
		}}},
		selectedContractActivitySource("http://activity.invalid", runtimecontracts.ActivityEffectClassReadOnly),
		selectedContractActivityFrontierPlanning("activity-event"),
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	if len(states) != 1 || states[0].FlowID != "activity-fork-proof" ||
		states[0].AddressKind != runfork.RunForkSelectedContractWorkflowStateRunScope {
		t.Fatalf("workflow states = %#v, want root run-scope activity state", states)
	}
}

func TestSelectedContractWorkflowStateProjectionPreservesExactTemplateActivityRoute(t *testing.T) {
	path := "flow_a/instance-1"
	agent := selectedContractTestAgentIdentity(t, "worker-agent", path)
	states, err := selectedContractWorkflowStateProjectionWithReadiness(
		runfork.RunForkPlan{PendingWork: []runfork.RunForkPendingWork{{
			EventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
			RoutingSource: eventtest.ConcreteTemplateRoutingSource("flow_a", path, "entity-1"),
		}}},
		selectedContractActivitySourceWithMode("http://activity.invalid", runtimecontracts.ActivityEffectClassReadOnly, runtimecontracts.FlowModeTemplate),
		runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
		}}},
		map[string]executionmode.Mode{"activity-event": executionmode.Mock},
		[]runtimemanager.AgentMaterializationBlueprint{{
			Config: runtimeactors.AgentConfig{ID: "worker-agent"}, Identity: mustTestAgentPlan(agent),
		}},
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	if len(states) != 1 || states[0].Mode != "template" || states[0].Route.InstancePath != "flow_a/instance-1" {
		t.Fatalf("workflow states = %#v, want exact template activity route", states)
	}
}

func TestSelectedContractWorkflowStateProjectionRejectsInvalidPlatformActivitySource(t *testing.T) {
	source := selectedContractActivitySource("http://activity.invalid", runtimecontracts.ActivityEffectClassReadOnly)
	tests := []struct {
		name   string
		source events.RoutingSource
		want   string
	}{
		{name: "missing", source: events.NoRoutingSource(), want: "exact event and entity identity"},
		{name: "out of scope", source: selectedContractTestFlowOwnedSource(t, "flow_a", "other/instance-1", "entity-1"), want: "outside flow scope"},
		{name: "kind conflicts with schema", source: eventtest.ConcreteTemplateRoutingSource("flow_a", "flow_a", "entity-1"), want: "rejects template routing source"},
		{name: "unknown flow", source: eventtest.StaticFlowRoutingSource("unknown", "unknown", "entity-1"), want: "has no semantic owner"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := selectedContractWorkflowStateProjection(
				runfork.RunForkPlan{PendingWork: []runfork.RunForkPendingWork{{
					EventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent, RoutingSource: tt.source,
				}}},
				source,
				selectedContractActivityFrontierPlanning("activity-event"),
			)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func selectedContractTestFlowOwnedSource(t *testing.T, flowID, flowInstance, entityID string) events.RoutingSource {
	t.Helper()
	source, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
		FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID,
	})
	if err != nil {
		t.Fatalf("NewFlowOwnedControlRoutingSource: %v", err)
	}
	return source
}

func selectedContractActivityFrontierPlanning(eventID string) runfork.RunForkSelectedContractRecipientPlanning {
	return runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
		SourceEventID: eventID,
		EventName:     runfork.RunForkSelectedContractPlatformActivityEvent,
	}}}
}

func TestSelectedContractWorkflowStateProjectionIgnoresNonFrontierPlatformActivityHistory(t *testing.T) {
	states, err := selectedContractWorkflowStateProjection(
		runfork.RunForkPlan{PendingWork: []runfork.RunForkPendingWork{{
			EventID: "completed-activity", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
			RoutingSource:  eventtest.StaticFlowRoutingSource("flow_a", "flow_a", "entity-1"),
			Classification: runfork.RunForkPendingClassificationDeliveredCompleted,
		}}},
		selectedContractActivitySource("http://activity.invalid", runtimecontracts.ActivityEffectClassReadOnly),
		runfork.RunForkSelectedContractRecipientPlanning{},
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("workflow states = %#v, want completed non-frontier activity ignored", states)
	}
}

func TestSelectedContractWorkflowStateProjectionDoesNotProjectUnroutedBusinessEvent(t *testing.T) {
	states, err := selectedContractWorkflowStateProjection(
		runfork.RunForkPlan{PendingWork: []runfork.RunForkPendingWork{{
			EventID: "business-event", EventName: "review.requested",
			RoutingSource: eventtest.ConcreteTemplateRoutingSource("flow_a", "flow_a/instance-1", "entity-1"),
		}}},
		selectedContractActivitySource("http://activity.invalid", runtimecontracts.ActivityEffectClassReadOnly),
		runfork.RunForkSelectedContractRecipientPlanning{},
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("workflow states = %#v, want no recipient-free business-event projection", states)
	}
}
