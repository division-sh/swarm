package bus

import (
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestReceiverCompositionLedgerExactPlanAndIndependentOwners(t *testing.T) {
	planA := events.AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-a")))
	planB := events.AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-b")))
	blueprint := events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver"}
	node := testFlowNode(t, "receiver", "collector")
	recipient := events.MustNodeDeliveryRecipient(node)
	evidence, err := events.NewConnectCandidateEvidence(events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte("pin"))), recipient, "receiver", agentidentity.Plan{}, events.ConnectCandidateAccepted)
	if err != nil {
		t.Fatal(err)
	}
	agent := agentidentitytest.Declared(t, "worker", "ledger", "receiver", "receiver", "receiver")
	agentPlan, err := agent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	agentRecipient := events.MustAgentDeliveryRecipient(agent.AgentID())
	agentEvidence, err := events.NewConnectCandidateEvidence(events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte("agent-pin"))), agentRecipient, "receiver", agentPlan, events.ConnectCandidateAccepted)
	if err != nil {
		t.Fatal(err)
	}
	a, err := events.NewConnectPlanEvaluation(planA, events.ConnectPlanResolved, []events.RouteIdentity{blueprint}, []events.ConnectCandidateEvidence{evidence, agentEvidence})
	if err != nil {
		t.Fatal(err)
	}
	b, err := events.NewConnectPlanEvaluation(planB, events.ConnectPlanResolved, []events.RouteIdentity{blueprint}, []events.ConnectCandidateEvidence{evidence})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger([]events.ConnectPlanEvaluation{a, b})
	if err != nil {
		t.Fatal(err)
	}
	ownerA := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: eventtest.UUID("owner-a")})
	ownerB := events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: eventtest.UUID("owner-b")})
	entityless := events.MustEntitylessReceiverTarget(blueprint)
	dependency, err := events.AdmitStoredFlowReceiverInitialization(agent.RunID, "", ownerA, agent.Route)
	if err != nil {
		t.Fatal(err)
	}
	intents := []RoutePlanDeliveryIntent{
		{Recipient: recipient, ConnectPlan: planA, TargetBlueprint: ownerA.Route(), TargetOwnership: ownerA},
		{Recipient: agentRecipient, AgentIdentity: agent, ConnectPlan: planA, TargetBlueprint: entityless.Route(), TargetOwnership: entityless, ReceiverBlueprint: ownerA.Route(), Initialization: dependency},
		{Recipient: recipient, ConnectPlan: planB, TargetBlueprint: ownerB.Route(), TargetOwnership: ownerB},
		// A non-connect route with identical scope cannot become either plan's owner.
		{Recipient: recipient, TargetOwnership: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: eventtest.UUID("foreign-direct")})},
	}
	for _, reverse := range []bool{false, true} {
		if reverse {
			for i, j := 0, len(intents)-1; i < j; i, j = i+1, j-1 {
				intents[i], intents[j] = intents[j], intents[i]
			}
		}
		if err := validateConnectIntentReceivers(RoutePlan{ConnectEvaluation: ledger, DeliveryIntents: intents}, nil); err != nil {
			t.Fatal(err)
		}
		for _, plan := range ledger.Plans() {
			if !reflect.DeepEqual(plan.Targets(), []events.RouteIdentity{blueprint}) {
				t.Fatal("actor state projection changed the compiled selected-flow ledger")
			}
		}
	}
	for _, plan := range ledger.Plans() {
		if !reflect.DeepEqual(plan.Targets(), []events.RouteIdentity{blueprint}) {
			t.Fatal("projection mutated original ledger")
		}
	}
	for _, hostile := range []struct {
		name  string
		plan  events.ConnectPlanIdentity
		owner events.DeliveryTargetOwnership
	}{
		{"missing_owner", planA, events.DeliveryTargetOwnership{}},
		{"foreign_plan", events.AdmitConnectPlanIdentity(sha256.Sum256([]byte("unknown"))), ownerA},
		{"foreign_flow_same_instance", planA, events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "other", FlowInstance: "receiver", EntityID: ownerA.Route().EntityID})},
		{"foreign_instance", planA, events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver/two"})},
	} {
		t.Run(hostile.name, func(t *testing.T) {
			if err := validateConnectIntentReceivers(RoutePlan{ConnectEvaluation: ledger, DeliveryIntents: []RoutePlanDeliveryIntent{{Recipient: recipient, ConnectPlan: hostile.plan, TargetOwnership: hostile.owner}}}, nil); err == nil {
				t.Fatal("contradictory recipient silently disappeared from ledger")
			}
		})
	}
}
