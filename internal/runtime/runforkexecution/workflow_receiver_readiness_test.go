package runforkexecution

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func selectedContractReceiverReadinessSource(t *testing.T, policy canonicalrouting.ForkReceiverPolicy) LoadedSelectedContractSource {
	t.Helper()
	repo := runForkExecutionRepoRoot(t)
	root := canonicalrouting.CopyForkReceiverOwnership(t, []canonicalrouting.ForkReceiver{{Path: "consumer", Policy: policy}}, false)
	loaded, err := (admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: root,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	}).LoadRunForkSelectedContractSource(context.Background(), runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func selectedContractReceiverReadinessPlan(entities ...runfork.RunForkEntityState) (runfork.RunForkPlan, runfork.RunForkSelectedContractRecipientPlanning) {
	return runfork.RunForkPlan{SourceRunID: selectedContractAgentTestRunID, Entities: entities,
			PendingWork: []runfork.RunForkPendingWork{{EventID: "source-event", EventName: "work.ready",
				RoutingSource: eventtest.StaticFlowRoutingSource("producer", "producer", "producer-entity"),
			}}}, runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: "source-event", EventName: "work.ready",
			Recipients: []runfork.RunForkContractFrontierRecipient{testNodeFrontierRecipient(mustRunForkNode("consumer", "collector"), "work.ready", "consumer", "selected")},
		}}}
}

func TestSelectedContractReceiverReadinessDoesNotTransferProducerState(t *testing.T) {
	loaded := selectedContractReceiverReadinessSource(t, canonicalrouting.ForkReceiverOptionalAbsent)
	producer := selectedContractReadinessTestEntity("producer-entity", "producer", "work")
	plan, planning := selectedContractReceiverReadinessPlan(producer)
	before, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := runforkreadiness.Project(plan, loaded.Source, planning,
		map[string]executionmode.Mode{"source-event": executionmode.Mock}, runtimemanager.AgentManagerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.States) != 0 || len(prepared.Flows) != 0 {
		t.Fatalf("optional consumer acquired producer state/companion: %#v", prepared)
	}
	after, err := json.Marshal(plan)
	if err != nil || string(before) != string(after) {
		t.Fatalf("fixed snapshot changed: %v", err)
	}
}

func TestSelectedContractReceiverReadinessPreservesIndependentExistingOwnerWithoutProducer(t *testing.T) {
	for _, policy := range []canonicalrouting.ForkReceiverPolicy{canonicalrouting.ForkReceiverOptionalExisting, canonicalrouting.ForkReceiverRequiredExisting} {
		loaded := selectedContractReceiverReadinessSource(t, policy)
		receiver := selectedContractReadinessTestEntity("receiver-entity", "consumer", "receipt")
		plan, planning := selectedContractReceiverReadinessPlan(receiver)
		plan.PendingWork[0].RoutingSource = events.NoRoutingSource()
		prepared, err := runforkreadiness.Project(plan, loaded.Source, planning,
			map[string]executionmode.Mode{"source-event": executionmode.Mock}, runtimemanager.AgentManagerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(prepared.States) != 1 || prepared.States[0].EntityID != receiver.EntityID || prepared.States[0].FlowID != "consumer" || prepared.States[0].EntityType != "receipt" {
			t.Fatalf("independent receiver lost lawful readiness: %#v", prepared.States)
		}
	}
}

func TestSelectedContractReceiverReadinessRejectsMissingRequiredAndContradictoryMetadata(t *testing.T) {
	loaded := selectedContractReceiverReadinessSource(t, canonicalrouting.ForkReceiverRequiredMissing)
	producer := selectedContractReadinessTestEntity("producer-entity", "producer", "work")
	plan, planning := selectedContractReceiverReadinessPlan(producer)
	if _, err := runforkreadiness.Project(plan, loaded.Source, planning,
		map[string]executionmode.Mode{"source-event": executionmode.Mock}, runtimemanager.AgentManagerOptions{}); err == nil || !strings.Contains(err.Error(), "owner is missing") {
		t.Fatalf("missing required receiver borrowed producer: %v", err)
	}
	for _, name := range []string{"missing metadata", "event metadata", "wrong type", "blank type", "duplicate owner", "duplicate contradictory owner", "ambiguous receiver"} {
		t.Run(name, func(t *testing.T) {
			receiver := selectedContractReadinessTestEntity("receiver-entity", "consumer", "receipt")
			entities := []runfork.RunForkEntityState{producer, receiver}
			switch name {
			case "missing metadata":
				entities[1].MaterializationMetadata = nil
			case "event metadata":
				entities[1].MaterializationMetadata.Source = "source_event"
			case "wrong type":
				entities[1].MaterializationMetadata.EntityType = "work"
			case "blank type":
				entities[1].MaterializationMetadata.EntityType = ""
			case "duplicate owner":
				entities = append(entities, receiver)
			case "duplicate contradictory owner":
				entities = append(entities, selectedContractReadinessTestEntity(receiver.EntityID, "producer", "work"))
			case "ambiguous receiver":
				entities = append(entities, selectedContractReadinessTestEntity("another-receiver", "consumer", "receipt"))
			}
			plan, planning := selectedContractReceiverReadinessPlan(entities...)
			if _, err := runforkreadiness.Project(plan, loaded.Source, planning,
				map[string]executionmode.Mode{"source-event": executionmode.Mock}, runtimemanager.AgentManagerOptions{}); err == nil {
				t.Fatal("contradictory fixed ownership admitted")
			}
		})
	}
}

func TestSelectedContractReceiverReadinessRetainsEveryEventAssociation(t *testing.T) {
	loaded := selectedContractReceiverReadinessSource(t, canonicalrouting.ForkReceiverOptionalExisting)
	plan, planning := selectedContractReceiverReadinessPlan(selectedContractReadinessTestEntity("receiver-entity", "consumer", "receipt"))
	first := planning.RecipientPlanEvents[0]
	first.SourceEventID = "another-event"
	planning.RecipientPlanEvents = append(planning.RecipientPlanEvents, first)
	modes := map[string]executionmode.Mode{"source-event": executionmode.Mock, "another-event": executionmode.Mock}
	left, err := runforkreadiness.Project(plan, loaded.Source, planning, modes, runtimemanager.AgentManagerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(left.States) != 1 || len(left.States[0].SourceEvents) != 2 || left.States[0].SourceEventID != "another-event" {
		t.Fatalf("event association lost: %#v", left.States)
	}
	planning.RecipientPlanEvents[0], planning.RecipientPlanEvents[1] = planning.RecipientPlanEvents[1], planning.RecipientPlanEvents[0]
	right, err := runforkreadiness.Project(plan, loaded.Source, planning, modes, runtimemanager.AgentManagerOptions{})
	if err != nil || !reflect.DeepEqual(left.States, right.States) {
		t.Fatalf("recipient order chose a different state owner: %#v, %v", right, err)
	}
	modes["another-event"] = executionmode.Live
	mixed, err := runforkreadiness.Project(plan, loaded.Source, planning, modes, runtimemanager.AgentManagerOptions{})
	if err != nil || len(mixed.States) != 1 || len(mixed.States[0].SourceEvents) != 2 ||
		mixed.States[0].ExecutionMode.Valid() || mixed.States[0].SourceEvents[0].ExecutionMode != executionmode.Live || mixed.States[0].SourceEvents[1].ExecutionMode != executionmode.Mock {
		t.Fatalf("static state lost exact per-event modes: %#v, %v", mixed, err)
	}
	planning.RecipientPlanEvents[0], planning.RecipientPlanEvents[1] = planning.RecipientPlanEvents[1], planning.RecipientPlanEvents[0]
	planning.RecipientPlanEvents = append(planning.RecipientPlanEvents, planning.RecipientPlanEvents[0])
	repeated, err := runforkreadiness.Project(plan, loaded.Source, planning, modes, runtimemanager.AgentManagerOptions{})
	if err != nil || !reflect.DeepEqual(mixed.States, repeated.States) {
		t.Fatalf("mixed-mode order or repeated recipient changed associations: %#v, %v", repeated, err)
	}
}

func TestSelectedContractReceiverReadinessTemplateAgentDoesNotElectFromHistory(t *testing.T) {
	loaded := selectedContractReadinessFixture(t, 1)
	flow, err := runtimemanager.TemplateFlowMaterialization(loaded.Source, "worker-flow", "worker-flow/one", "selected-entity")
	if err != nil {
		t.Fatal(err)
	}
	agentPlan := flow.Agents[0].Identity
	planning := runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
		SourceEventID: "event", EventName: "worker.ready", Recipients: []runfork.RunForkContractFrontierRecipient{
			testAgentFrontierRecipient(agentPlan, "worker.ready", "worker-flow/one", "selected"),
		},
	}}}
	plan := runfork.RunForkPlan{SourceRunID: selectedContractAgentTestRunID,
		Entities: []runfork.RunForkEntityState{selectedContractReadinessTestEntity("selected-entity", "worker-flow/one", "worker")}}
	for _, historicalRun := range []string{selectedContractAgentTestRunID, "sibling-run"} {
		live, err := agentPlan.Live(historicalRun)
		if err != nil {
			t.Fatal(err)
		}
		plan.PendingWork = []runfork.RunForkPendingWork{{EventID: "event", DeliveryRoute: events.DeliveryRoute{
			Recipient: events.MustAgentDeliveryRecipient(agentPlan.AgentID()), AgentIdentity: live,
			Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker-flow", FlowInstance: "worker-flow/one", EntityID: "historical-only-entity"}),
		}}}
		prepared, err := runforkreadiness.Project(plan, loaded.Source, planning,
			map[string]executionmode.Mode{"event": executionmode.Mock}, runtimemanager.AgentManagerOptions{})
		if err != nil || len(prepared.States) != 1 || prepared.States[0].EntityID != "selected-entity" {
			t.Fatalf("historical target elected selected readiness: %#v, %v", prepared, err)
		}
	}
	plan.Entities = nil
	if _, err := runforkreadiness.Project(plan, loaded.Source, planning,
		map[string]executionmode.Mode{"event": executionmode.Mock}, runtimemanager.AgentManagerOptions{}); err == nil {
		t.Fatal("historical target supplied missing selected receiver state")
	}
}

func TestSelectedContractReceiverReadinessTemplateRequiresOneGenerationMode(t *testing.T) {
	loaded := selectedContractReadinessFixture(t, 1)
	const path = "worker-flow/one"
	flow, err := runtimemanager.TemplateFlowMaterialization(loaded.Source, "worker-flow", path, "receiver-entity")
	if err != nil {
		t.Fatal(err)
	}
	plan := runfork.RunForkPlan{SourceRunID: selectedContractAgentTestRunID,
		Entities: []runfork.RunForkEntityState{selectedContractReadinessTestEntity("receiver-entity", path, "worker")}}
	for _, secondMode := range []executionmode.Mode{executionmode.Mock, executionmode.Live} {
		for _, reversed := range []bool{false, true} {
			planning := runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{
				{SourceEventID: "node-event", EventName: "worker.observed", Recipients: []runfork.RunForkContractFrontierRecipient{
					testNodeFrontierRecipient(mustRunForkNode("worker-flow", "worker-node"), "worker.observed", path, "selected"),
				}},
				{SourceEventID: "agent-event", EventName: "worker.ready", Recipients: []runfork.RunForkContractFrontierRecipient{
					testAgentFrontierRecipient(flow.Agents[0].Identity, "worker.ready", path, "selected"),
				}},
			}}
			if reversed {
				planning.RecipientPlanEvents[0], planning.RecipientPlanEvents[1] = planning.RecipientPlanEvents[1], planning.RecipientPlanEvents[0]
			}
			prepared, err := runforkreadiness.Project(plan, loaded.Source, planning,
				map[string]executionmode.Mode{"node-event": executionmode.Mock, "agent-event": secondMode}, runtimemanager.AgentManagerOptions{})
			if secondMode != executionmode.Mock {
				if err == nil || prepared != nil {
					t.Fatalf("mixed template modes elected a generation (reversed=%t): %#v, %v", reversed, prepared, err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(prepared.States) != 1 || len(prepared.Flows) != 1 || len(prepared.States[0].SourceEvents) != 2 ||
				prepared.States[0].ExecutionMode != executionmode.Mock || prepared.States[0].SourceEventID != "agent-event" {
				t.Fatalf("compatible associations lost single generation: %#v", prepared)
			}
			for _, association := range prepared.States[0].SourceEvents {
				if association.ExecutionMode != prepared.States[0].ExecutionMode {
					t.Fatalf("association disagrees with generation: %#v", prepared.States[0])
				}
			}
		}
	}
}

func TestSelectedContractReceiverReadinessTemplateConsumesExactPlanRoute(t *testing.T) {
	loaded := selectedContractReadinessFixture(t, 1)
	const path = "worker-flow/one"
	flow, err := runtimemanager.TemplateFlowMaterialization(loaded.Source, "worker-flow", path, "receiver-entity")
	if err != nil {
		t.Fatal(err)
	}
	plan := runfork.RunForkPlan{SourceRunID: selectedContractAgentTestRunID,
		Entities: []runfork.RunForkEntityState{selectedContractReadinessTestEntity("receiver-entity", path, "worker")}}
	for _, name := range []string{"exact", "wrong instance", "wrong scope", "outside scope", "sibling path", "wrong metadata type"} {
		t.Run(name, func(t *testing.T) {
			agentPlan := flow.Agents[0].Identity
			currentPlan := plan
			switch name {
			case "wrong instance":
				agentPlan.Route.InstanceID = "other"
			case "wrong scope":
				agentPlan.Route.ScopeKey = "other-flow"
			case "outside scope":
				agentPlan.Route.InstancePath = "other-flow/one"
			case "sibling path":
				agentPlan.Route.InstanceID = "other"
				agentPlan.Route.InstancePath = "worker-flow/other"
			case "wrong metadata type":
				currentPlan.Entities = []runfork.RunForkEntityState{selectedContractReadinessTestEntity("receiver-entity", path, "foreign")}
			}
			recipient := testAgentFrontierRecipient(agentPlan, "worker.ready", agentPlan.FlowInstance(), "selected")
			planning := runfork.RunForkSelectedContractRecipientPlanning{RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
				SourceEventID: "event", EventName: "worker.ready", Recipients: []runfork.RunForkContractFrontierRecipient{recipient},
			}}}
			prepared, err := runforkreadiness.Project(currentPlan, loaded.Source, planning,
				map[string]executionmode.Mode{"event": executionmode.Mock}, runtimemanager.AgentManagerOptions{})
			if name == "exact" {
				if err != nil || len(prepared.States) != 1 || prepared.States[0].EntityID != "receiver-entity" || prepared.States[0].Route.InstancePath != path {
					t.Fatalf("exact plan lost owner: %#v, %v", prepared, err)
				}
				return
			}
			if name == "wrong scope" {
				if err == nil && len(prepared.States) != 0 {
					t.Fatalf("path elected template despite foreign plan scope: %#v", prepared)
				}
				return
			}
			if err == nil {
				t.Fatalf("contradictory exact plan admitted: %#v", prepared)
			}
		})
	}
}

func TestSelectedContractReceiverReadinessRejectsUnsupportedConfigDependencyAuthoring(t *testing.T) {
	loaded := selectedContractReadinessFixture(t, 1)
	flow, err := runtimemanager.TemplateFlowMaterialization(loaded.Source, "worker-flow", "worker-flow/one", "receiver-entity")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(flow.Config, map[string]any{"worker_id": "one"}) {
		t.Fatalf("fresh template config gained unproven inputs: %#v", flow.Config)
	}
	if _, exists := flow.ActivationVariables["foo"]; exists {
		t.Fatalf("fresh activation inferred unavailable foo: %#v", flow.ActivationVariables)
	}
	for _, blueprint := range flow.Agents {
		var config map[string]any
		if err := json.Unmarshal(blueprint.Config.Config, &config); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(config, map[string]any{"worker_id": "one"}) {
			t.Fatalf("agent acquired undeclared activation inputs: %#v", config)
		}
	}
	// These are hostile authority-boundary inputs, not an admitted arbitrary
	// config-dependent template or a historical configuration recovery proof.
	for _, subscription := range []string{"worker.{foo}", "worker.{{foo}}", "worker.{config.foo}", "worker.{{config.foo}}"} {
		t.Run(subscription, func(t *testing.T) {
			admission := semanticview.ClassifyAuthoredSubscription(loaded.Source, semanticview.AuthoredSubscriptionRequest{
				ConsumerKind: semanticview.AuthoredSubscriptionConsumerAgent,
				ConsumerID:   flow.Agents[0].Identity.AgentID(),
				FlowID:       "worker-flow", FlowPath: "worker-flow", Authored: subscription,
			})
			if admission.Admitted() || !strings.Contains(admission.Message(), subscription) {
				t.Fatalf("unproven exact subscription dependency admitted or silently omitted: %#v", admission)
			}
			_, err := semanticview.AdmitFlowOwnedAgentSubscriptions(loaded.Source, semanticview.FlowOwnedAgentSubscriptionRequest{
				AgentID: flow.Agents[0].Identity.AgentID(), FlowID: "worker-flow", FlowPath: "worker-flow",
				Subscriptions: []string{subscription},
			})
			if err == nil || !strings.Contains(err.Error(), subscription) {
				t.Fatalf("runtime subscription owner dropped unproven dependency: %v", err)
			}
		})
	}
	for _, field := range []string{"config: {foo: required}", "prompt_inputs: [config.foo]"} {
		var entry runtimecontracts.AgentRegistryEntry
		if err := yaml.Unmarshal([]byte(field), &entry); err == nil {
			t.Fatalf("unsupported configuration authority accepted: %s", field)
		}
	}
}
