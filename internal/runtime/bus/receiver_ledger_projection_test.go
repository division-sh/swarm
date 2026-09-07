package bus

import (
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestReceiverCompositionLedgerExactPlanAndIndependentOwners(t *testing.T) {
	planA := events.AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-a")))
	planB := events.AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-b")))
	blueprint := events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver"}
	node := testFlowNode(t, "receiver", "collector")
	recipient := events.MustNodeDeliveryRecipient(node)
	evidence, err := events.NewConnectCandidateEvidence(events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte("pin"))), recipient, "receiver", events.DeliveryRoute{}.AgentIdentity, events.ConnectCandidateAccepted)
	if err != nil {
		t.Fatal(err)
	}
	a, err := events.NewConnectPlanEvaluation(planA, events.ConnectPlanResolved, []events.RouteIdentity{blueprint}, []events.ConnectCandidateEvidence{evidence})
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
	intents := []RoutePlanDeliveryIntent{
		{Recipient: recipient, ConnectPlan: planA, TargetBlueprint: ownerA.Route(), TargetOwnership: ownerA},
		{Recipient: recipient, ConnectPlan: planA, TargetBlueprint: entityless.Route(), TargetOwnership: entityless},
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
		got, err := (selectedRunTargetOwnerProjection{}).resolveConnectEvaluation(ledger, intents)
		if err != nil {
			t.Fatal(err)
		}
		for _, plan := range got.Plans() {
			want := map[events.RouteIdentity]bool{ownerB.Route(): true}
			if plan.PlanIdentity() == planA {
				want = map[events.RouteIdentity]bool{ownerA.Route(): true, entityless.Route(): true}
			}
			actual := map[events.RouteIdentity]bool{}
			for _, route := range plan.Targets() {
				actual[route] = true
			}
			if !reflect.DeepEqual(actual, want) {
				t.Fatalf("plan %s borrowed another owner: %#v want %#v", plan.PlanIdentity().String(), actual, want)
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
			if _, err := (selectedRunTargetOwnerProjection{}).resolveConnectEvaluation(ledger, []RoutePlanDeliveryIntent{{Recipient: recipient, ConnectPlan: hostile.plan, TargetOwnership: hostile.owner}}); err == nil {
				t.Fatal("contradictory recipient silently disappeared from ledger")
			}
		})
	}
}
