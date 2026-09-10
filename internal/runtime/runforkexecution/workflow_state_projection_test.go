package runforkexecution

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func selectedContractReadinessFixture(t *testing.T, declarations int) LoadedSelectedContractSource {
	t.Helper()
	repo := runForkExecutionRepoRoot(t)
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "tests/tier12-runtime-fork/test-run-scoped-flow-agent-fork"))); err != nil {
		t.Fatal(err)
	}
	agentsPath := filepath.Join(root, "worker-flow/agents.yaml")
	agents, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatal(err)
	}
	switch declarations {
	case 0:
		if err := os.Remove(agentsPath); err != nil {
			t.Fatal(err)
		}
	case 2:
		second := strings.ReplaceAll(string(agents), "worker-agent", "second-agent")
		prompt, err := os.ReadFile(filepath.Join(root, "worker-flow/prompts/worker-agent.md"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "worker-flow/prompts/second-agent.md"), prompt, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(agentsPath, append(agents, []byte(second)...), 0600); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := (admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: root,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	}).LoadRunForkSelectedContractSource(context.Background(), runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func TestSelectedContractWorkflowStateProjectionDoesNotInventUndeclaredAgentReadiness(t *testing.T) {
	eventID := "worker-ready"
	entityID := "entity-1"
	path := "worker-flow/instance-1"
	agent := selectedContractTestAgentIdentity(t, "worker-agent", path)
	source := selectedContractReadinessFixture(t, 0).Source
	prepared, err := runforkreadiness.Project(
		runfork.RunForkPlan{
			SourceRunID: selectedContractAgentTestRunID,
			Entities:    []runfork.RunForkEntityState{selectedContractReadinessTestEntity(entityID, path, "worker")},
			PendingWork: []runfork.RunForkPendingWork{{
				EventID: eventID,
				DeliveryRoute: events.DeliveryRoute{
					Recipient:     events.MustAgentDeliveryRecipient(agent.AgentID()),
					AgentIdentity: agent,
					Target: events.MustExistingEntityTarget(events.RouteIdentity{
						FlowID: "worker-flow", FlowInstance: path, EntityID: entityID,
					}),
				},
			}},
		},
		source,
		runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: eventID,
			EventName:     "review.requested",
			Recipients: []runfork.RunForkContractFrontierRecipient{
				testAgentFrontierRecipient(mustTestAgentPlan(agent), "review.requested", path, "selected_contracts"),
			},
		}}},
		map[string]executionmode.Mode{eventID: executionmode.Mock},
		runtimemanager.AgentManagerOptions{},
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	states := prepared.States
	if len(states) != 1 || states[0].EntityID != entityID || states[0].FlowID != "worker-flow" ||
		states[0].Mode != runtimecontracts.FlowModeTemplate || states[0].Route.InstancePath != path ||
		states[0].ExecutionMode != executionmode.Mock || len(states[0].Agents) != 0 {
		t.Fatalf("workflow states = %#v, want exact template-agent owner", states)
	}
}

func TestSelectedContractWorkflowReadinessIndependentOfAgentFrontier(t *testing.T) {
	for _, declarations := range []int{0, 1, 2} {
		for selected := 0; selected <= declarations; selected++ {
			for _, frontier := range []string{"node", "activity", "mixed"} {
				t.Run(fmt.Sprintf("declared_%d/selected_%d/%s", declarations, selected, frontier), func(t *testing.T) {
					loaded := selectedContractReadinessFixture(t, declarations)
					source := loaded.Source
					flowID, path, key, nodeID := "worker-flow", "worker-flow/instance-1", "worker_id", "worker-node"
					flow, err := runtimemanager.TemplateFlowMaterialization(source, flowID, path, "entity-1")
					if err != nil {
						t.Fatal(err)
					}
					if len(flow.Agents) != declarations {
						t.Fatalf("declaration count = %d, want %d", len(flow.Agents), declarations)
					}
					plan := runfork.RunForkPlan{SourceRunID: selectedContractAgentTestRunID,
						Entities: []runfork.RunForkEntityState{selectedContractReadinessTestEntity("entity-1", path, "worker")}}
					planning := runfork.RunForkSelectedContractRecipientPlanning{Owner: runfork.RunForkSelectedContractRecipientPlanningOwner}
					modes := map[string]executionmode.Mode{}
					add := func(id, name string, recipients []runfork.RunForkContractFrontierRecipient) {
						plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: id, EventName: name,
							RoutingSource: eventtest.ConcreteTemplateRoutingSource(flowID, path, "entity-1")})
						planning.RecipientPlanEvents = append(planning.RecipientPlanEvents, runfork.RunForkSelectedContractRecipientPlanEvent{SourceEventID: id, EventName: name, Recipients: recipients})
						modes[id] = executionmode.Mock
					}
					if frontier != "activity" {
						add("node", "worker.observed", []runfork.RunForkContractFrontierRecipient{testNodeFrontierRecipient(mustRunForkNode(flowID, nodeID), "worker.observed", path, "selected_contracts")})
					}
					if frontier != "node" {
						add("activity", runfork.RunForkSelectedContractPlatformActivityEvent, nil)
					}
					for i := 0; i < selected; i++ {
						blueprint := flow.Agents[i]
						identity, err := blueprint.Identity.Live(plan.SourceRunID)
						if err != nil {
							t.Fatal(err)
						}
						id := fmt.Sprintf("agent-%d", i)
						add(id, "worker.ready", []runfork.RunForkContractFrontierRecipient{testAgentFrontierRecipient(blueprint.Identity, "worker.ready", path, "selected_contracts")})
						plan.PendingWork[len(plan.PendingWork)-1].DeliveryRoute = events.DeliveryRoute{
							Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity,
							Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: "entity-1"}),
						}
					}
					prepared, err := runforkreadiness.Project(plan, source, planning, modes, runtimemanager.AgentManagerOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if len(prepared.States) != 1 || prepared.States[0].Config[key] != "instance-1" || len(prepared.States[0].Agents) != declarations {
						t.Fatalf("flow-owned readiness = %#v", prepared.States)
					}
					factoryCalls := 0
					runtime, err := prepareSelectedContractAgentRuntimeMaterialization(context.Background(), loaded, planning, prepared.Blueprints, SelectedContractAgentRuntimeOptions{
						AgentFactory: func(actors.AgentConfig) (runtimemanager.Agent, error) {
							factoryCalls++
							return nil, fmt.Errorf("preparation cannot construct an executing agent")
						},
					})
					if err != nil || len(runtime.Blueprints) != declarations || runtime.Proof.MaterializationRequired != (declarations > 0) {
						t.Fatalf("complete admitted preparation census: %#v, %v", runtime, err)
					}
					if factoryCalls != 0 || len(runtime.Proof.AgentRecipientPlans) != selected {
						t.Fatalf("preparation changed initial dispatch or executed a factory: calls=%d recipients=%v", factoryCalls, runtime.Proof.AgentRecipientPlans)
					}
					if runtime.workspaceProjection != nil {
						if err := runtime.workspaceProjection.Release(); err != nil {
							t.Fatal(err)
						}
					}
					if declarations > 0 && selected == 0 {
						_, err := prepareSelectedContractAgentRuntimeMaterialization(context.Background(), loaded, planning, prepared.Blueprints, SelectedContractAgentRuntimeOptions{})
						if err == nil {
							t.Fatal("downstream actor without runtime configuration passed preparation")
						}
					}
				})
			}
		}
	}
}

func TestSelectedContractWorkflowStateProjectionUsesPlatformActivityRoutingSourceWithoutNodeRecipient(t *testing.T) {
	entityID := "entity-1"
	source := selectedContractReceiverReadinessSource(t, canonicalrouting.ForkReceiverOptionalAbsent).Source
	plan := runfork.RunForkPlan{SourceRunID: selectedContractAgentTestRunID,
		Entities: []runfork.RunForkEntityState{selectedContractReadinessTestEntity(entityID, "producer", "work")}, PendingWork: []runfork.RunForkPendingWork{{
			EventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
			RoutingSource: eventtest.StaticFlowRoutingSource("producer", "producer", entityID),
		}}}
	planning := runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
		SourceEventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
	}}}

	states, err := selectedContractWorkflowStateProjection(
		plan,
		source,
		planning,
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	if len(states) != 1 || states[0].EntityID != entityID || states[0].FlowID != "producer" ||
		states[0].AddressKind != runfork.RunForkSelectedContractWorkflowStateExact ||
		states[0].Route.InstancePath != "producer" {
		t.Fatalf("workflow states = %#v, want exact source-owned activity route", states)
	}
}

func TestSelectedContractPreparedActorCensusExactPlans(t *testing.T) {
	loaded := selectedContractReadinessFixture(t, 2)
	flow, err := runtimemanager.TemplateFlowMaterialization(loaded.Source, "worker-flow", "worker-flow/instance-1", "entity-1")
	if err != nil {
		t.Fatal(err)
	}
	actors, err := runforkreadiness.PreparedActorCensus(flow.Agents)
	if err != nil || len(actors) != 2 {
		t.Fatalf("concrete template census: %v, %v", actors, err)
	}
	reversed := []runtimemanager.AgentMaterializationBlueprint{flow.Agents[1], flow.Agents[0], flow.Agents[1]}
	canonical, err := runforkreadiness.PreparedActorCensus(reversed)
	if err != nil || len(canonical) != 2 || canonical[0].Identity != actors[0].Identity || canonical[1].Identity != actors[1].Identity {
		t.Fatalf("order/exact repeated declaration changes census: %v, %v", canonical, err)
	}
	conflicting := append([]runtimemanager.AgentMaterializationBlueprint(nil), reversed...)
	conflicting[2].Config.Role += "-changed"
	if _, err := runforkreadiness.PreparedActorCensus(conflicting); err == nil {
		t.Fatal("same-plan conflicting configuration accepted")
	}
	live := append([]runtimemanager.AgentMaterializationBlueprint(nil), actors...)
	live[0].Config.Identity, err = live[0].Identity.Live(selectedContractAgentTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runforkreadiness.PreparedActorCensus(live); err == nil {
		t.Fatal("live actor configuration adopted into prospective census")
	}
	foreign, err := runtimemanager.TemplateFlowMaterialization(loaded.Source, "worker-flow", "worker-flow/instance-2", "entity-2")
	if err != nil {
		t.Fatal(err)
	}
	separate, err := runforkreadiness.PreparedActorCensus(append(flow.Agents, foreign.Agents...))
	if err != nil || len(separate) != 4 {
		t.Fatalf("same-name concrete template owners merged: %v, %v", separate, err)
	}
}

func TestSelectedContractWorkflowStateProjectionMapsRootPlatformActivityToRunScope(t *testing.T) {
	source := selectedContractReceiverReadinessSource(t, canonicalrouting.ForkReceiverOptionalAbsent).Source
	rootSource, err := events.NewRootRoutingSource(selectedContractAgentTestRunID)
	if err != nil {
		t.Fatalf("NewRootRoutingSource: %v", err)
	}
	states, err := selectedContractWorkflowStateProjection(
		runfork.RunForkPlan{SourceRunID: selectedContractAgentTestRunID,
			Entities: []runfork.RunForkEntityState{selectedContractReadinessTestEntity(selectedContractAgentTestRunID, selectedContractAgentTestRunID, "root")}, PendingWork: []runfork.RunForkPendingWork{{
				EventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent, RoutingSource: rootSource,
			}}},
		source,
		selectedContractActivityFrontierPlanning("activity-event"),
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	if len(states) != 1 || states[0].FlowID != "." ||
		states[0].AddressKind != runfork.RunForkSelectedContractWorkflowStateRunScope {
		t.Fatalf("workflow states = %#v, want root run-scope activity state", states)
	}
}

func TestSelectedContractWorkflowStateProjectionPreservesExactTemplateActivityRoute(t *testing.T) {
	path := "worker-flow/instance-1"
	source := selectedContractReadinessFixture(t, 0).Source
	prepared, err := runforkreadiness.Project(
		runfork.RunForkPlan{SourceRunID: selectedContractAgentTestRunID,
			Entities: []runfork.RunForkEntityState{selectedContractReadinessTestEntity("entity-1", path, "worker")}, PendingWork: []runfork.RunForkPendingWork{{
				EventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
				RoutingSource: eventtest.ConcreteTemplateRoutingSource("worker-flow", path, "entity-1"),
			}}},
		source,
		runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: "activity-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent,
		}}},
		map[string]executionmode.Mode{"activity-event": executionmode.Mock},
		runtimemanager.AgentManagerOptions{},
	)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	states := prepared.States
	if len(states) != 1 || states[0].Mode != "template" || states[0].Route.InstancePath != path {
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

func selectedContractReadinessTestEntity(id, path, entityType string) runfork.RunForkEntityState {
	return runfork.RunForkEntityState{EntityID: id, CurrentState: "idle",
		MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
			Owner:        runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
			Source:       runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState,
			FlowInstance: path, EntityType: entityType,
		}}
}

func selectedContractActivityFrontierPlanning(eventID string) runfork.RunForkSelectedContractRecipientPlanning {
	return runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
		SourceEventID: eventID,
		EventName:     runfork.RunForkSelectedContractPlatformActivityEvent,
	}}}
}

func selectedContractWorkflowStateProjection(
	plan runfork.RunForkPlan,
	source semanticview.Source,
	planning runfork.RunForkSelectedContractRecipientPlanning,
) ([]runfork.RunForkSelectedContractWorkflowState, error) {
	sourceModes := make(map[string]executionmode.Mode, len(planning.RecipientPlanEvents))
	for _, event := range planning.RecipientPlanEvents {
		sourceModes[strings.TrimSpace(event.SourceEventID)] = executionmode.Mock
	}
	prepared, err := runforkreadiness.Project(plan, source, planning, sourceModes, runtimemanager.AgentManagerOptions{})
	if err != nil {
		return nil, err
	}
	return prepared.States, nil
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
