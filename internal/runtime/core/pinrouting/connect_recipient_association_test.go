package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestConnectRecipientAssociationsPreservePerPlanHandler(t *testing.T) {
	for _, tc := range []struct {
		name      string
		second    string
		flattened int
	}{
		{name: "different_handler_events", second: "work.second", flattened: 2},
		{name: "same_handler_distinct_plans", second: "work.first", flattened: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := associationTestPlan(t, "first", "work.first")
			second := associationTestPlan(t, "second", tc.second)
			graph := CompiledConnectGraph{plans: []ConnectRoutePlan{first, second}}
			node := identitytest.FlowNode(t, "worker", "receive")
			recipient, err := NewConnectNodeRecipient(node, "worker")
			if err != nil {
				t.Fatal(err)
			}
			registrations := graph.AdmitReceiverRecipient("worker", "work.first", recipient)
			if tc.second != "work.first" {
				registrations = append(registrations, graph.AdmitReceiverRecipient("worker", events.EventType(tc.second), recipient)...)
			}
			rejected, err := NewConnectNodeRecipient(node, "worker/unselected")
			if err != nil {
				t.Fatal(err)
			}
			registrations = append(registrations, graph.AdmitReceiverRecipient("worker", "work.first", rejected)...)
			evaluation := graph.EvaluateSourceRecipients(associationTestSourceEvent(t), registrations)
			ledger, err := evaluation.Ledger()
			if err != nil {
				t.Fatal(err)
			}
			if len(ledger.Plans()) != 2 || len(evaluation.Recipients()) != tc.flattened {
				t.Fatalf("plans/flattened recipients = %d/%d, want 2/%d", len(ledger.Plans()), len(evaluation.Recipients()), tc.flattened)
			}
			associations := evaluation.Associations()
			if len(associations) != 2 {
				t.Fatalf("associations = %d, want two accepted per-plan recipients", len(associations))
			}
			for index, plan := range []ConnectRoutePlan{first, second} {
				wantID, err := ConnectPlanIdentity(plan)
				if err != nil {
					t.Fatal(err)
				}
				got := associations[index]
				if got.PlanIdentity() != wantID || got.ReceiverIdentity() != plan.ReceiverPinIdentity().EvidenceIdentity() {
					t.Fatalf("association %d lost its exact plan/receiver pin", index)
				}
				wantEvent := events.EventType("work.first")
				if index == 1 {
					wantEvent = events.EventType(tc.second)
				}
				if got.Recipient().Kind() != ConnectRecipientNode || !got.Recipient().Handler().Node().Equal(node) ||
					got.Recipient().HandlerEvent() != wantEvent || got.Recipient().Path() != "worker" {
					t.Fatalf("association %d recipient = %#v, want exact worker node and %s", index, got.Recipient(), wantEvent)
				}
			}
			firstAssociation := associations[0]
			associations[0] = ConnectRecipientAssociation{}
			graph.plans = nil
			registrations[0] = ConnectRecipientRegistration{}
			if got := evaluation.Associations()[0]; got != firstAssociation {
				t.Fatal("association readback changed stored evaluation or consulted mutated inputs")
			}
		})
	}
}

func TestConnectRecipientAssociationsPreserveFullAgentPlans(t *testing.T) {
	plan := associationTestPlan(t, "ready", "work.ready")
	graph := CompiledConnectGraph{plans: []ConnectRoutePlan{plan}}
	route, err := agentidentity.PresentRoute("worker", "worker-instance", "worker")
	if err != nil {
		t.Fatal(err)
	}
	var registrations []ConnectRecipientRegistration
	want := make(map[agentidentity.Plan]bool)
	for _, name := range []agentidentity.Name{
		{AgentID: "reviewer", Owner: "test://agents/first", Source: agentidentity.NameSourceDeclared},
		{AgentID: "reviewer", Owner: "test://agents/second", Source: agentidentity.NameSourceDeclared},
		{AgentID: "reviewer", Owner: "test://agents/first", Source: agentidentity.NameSourceRuntimeCreated},
	} {
		agent, err := agentidentity.NewPlan(name, route)
		if err != nil {
			t.Fatal(err)
		}
		want[agent] = true
		recipient, err := NewConnectAgentRecipient("reviewer", "worker", agent)
		if err != nil {
			t.Fatal(err)
		}
		registrations = append(registrations, graph.AdmitReceiverRecipient("worker", "work.ready", recipient)...)
	}
	evaluation := graph.EvaluateMaterializedRecipients(plan, []events.RouteIdentity{{FlowID: "worker", FlowInstance: "worker"}}, registrations)
	if _, err := evaluation.Ledger(); err != nil {
		t.Fatal(err)
	}
	wantID, err := ConnectPlanIdentity(plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(evaluation.Associations()) != len(want) || len(evaluation.Recipients()) != len(want) {
		t.Fatalf("association/recipient count = %d/%d, want %d exact plans", len(evaluation.Associations()), len(evaluation.Recipients()), len(want))
	}
	for _, association := range evaluation.Associations() {
		recipient := association.Recipient()
		if association.PlanIdentity() != wantID || association.ReceiverIdentity() != plan.ReceiverPinIdentity().EvidenceIdentity() ||
			recipient.Kind() != ConnectRecipientAgent || recipient.ID() != "reviewer" || recipient.Path() != "worker" ||
			recipient.HandlerEvent() != "work.ready" || !recipient.Handler().Empty() || !want[recipient.AgentPlan()] {
			t.Fatalf("unexpected or duplicate agent association: %#v", association)
		}
		delete(want, recipient.AgentPlan())
	}
	if len(want) != 0 {
		t.Fatalf("lost exact agent plans: %#v", want)
	}
}

func TestConnectRecipientAssociationsKeepLedgerErrorBoundary(t *testing.T) {
	plan := associationTestPlan(t, "ready", "work.ready")
	graph := CompiledConnectGraph{plans: []ConnectRoutePlan{plan}}
	registration := ConnectRecipientRegistration{
		receiverPin: plan.ReceiverPinIdentity(),
		recipient:   ConnectRecipient{kind: ConnectRecipientNode},
	}
	evaluation := graph.EvaluateSourceRecipients(associationTestSourceEvent(t), []ConnectRecipientRegistration{registration})
	if _, err := evaluation.Ledger(); err == nil {
		t.Fatal("Ledger accepted malformed recipient")
	}
	if len(evaluation.Associations()) != 0 {
		t.Fatal("failed per-plan evaluation exposed an accepted association")
	}
}

func associationTestPlan(t *testing.T, pin, handlerEvent string) ConnectRoutePlan {
	t.Helper()
	plan, err := newConnectRoutePlan(connectRoutePlanSpec{
		source: newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, true, "", "", "root", "work.published", "work.published", "work.published").withCompiledPinDigest("source-pin"),
		receiver: newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, "worker", "worker", runtimecontracts.FlowModeStatic,
			pin, handlerEvent, "worker/"+handlerEvent).withCompiledPinDigest("receiver-" + pin),
		producerEvent: &connectProducerEventEvidence{ownerFlowPath: ".", eventName: "work.published", acceptanceSchemaDigest: "source-schema"},
		receiverEvent: &connectProducerEventEvidence{ownerFlowPath: "worker", eventName: handlerEvent, acceptanceSchemaDigest: "receiver-schema"},
		targetKind:    ConnectTargetKindTarget, resolutionKind: ConnectResolutionStatic,
		target: events.RouteIdentity{FlowID: "worker", FlowInstance: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func associationTestSourceEvent(t *testing.T) SourceEvent {
	t.Helper()
	source, err := events.NewRootRoutingSource("source-entity")
	if err != nil {
		t.Fatal(err)
	}
	event, err := AdmitSourceEvent("work.published", source)
	if err != nil {
		t.Fatal(err)
	}
	return event
}
