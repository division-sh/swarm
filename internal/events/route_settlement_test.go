package events

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestConnectRecipientEvaluationPreservesNormalizedPerPlanOutcomes(t *testing.T) {
	empty, err := NewConnectEvaluationLedger(nil)
	if err != nil || !empty.Present() || len(empty.Plans()) != 0 {
		t.Fatalf("zero-plan ledger = %#v err=%v", empty, err)
	}
	planID := AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-a")))
	receiver := AdmitConnectReceiverIdentity(sha256.Sum256([]byte("receiver-a")))
	recipient := MustNodeDeliveryRecipient(identitytest.RootNode(t, "consumer"))
	accepted, err := NewConnectCandidateEvidence(receiver, recipient, "consumer", agentidentity.Identity{}, ConnectCandidateAccepted)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewConnectPlanEvaluation(planID, ConnectPlanResolved, []RouteIdentity{{FlowID: "consumer"}, {FlowID: "consumer"}}, []ConnectCandidateEvidence{accepted, accepted})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewConnectEvaluationLedger([]ConnectPlanEvaluation{plan, plan})
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.Present() || len(ledger.Plans()) != 1 || len(ledger.Plans()[0].Targets()) != 1 || len(ledger.Plans()[0].Candidates()) != 1 {
		t.Fatalf("normalized ledger = %#v", ledger)
	}

	noRegistrationID := AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-no-registration")))
	noRegistration, err := NewConnectPlanEvaluation(noRegistrationID, ConnectPlanNoRegistration, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rejectedReceiver := AdmitConnectReceiverIdentity(sha256.Sum256([]byte("receiver-b")))
	rejected, err := NewConnectCandidateEvidence(rejectedReceiver, MustNodeDeliveryRecipient(identitytest.RootNode(t, "other")), "other", agentidentity.Identity{}, ConnectCandidatePathMismatch)
	if err != nil {
		t.Fatal(err)
	}
	mixedID := AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-mixed")))
	mixed, err := NewConnectPlanEvaluation(mixedID, ConnectPlanResolved, nil, []ConnectCandidateEvidence{rejected, accepted})
	if err != nil {
		t.Fatal(err)
	}
	many, err := NewConnectEvaluationLedger([]ConnectPlanEvaluation{mixed, noRegistration, plan})
	if err != nil || len(many.Plans()) != 3 {
		t.Fatalf("many-plan ledger = %#v err=%v", many, err)
	}
	if many.Plans()[0].PlanIdentity().String() > many.Plans()[1].PlanIdentity().String() || many.Plans()[1].PlanIdentity().String() > many.Plans()[2].PlanIdentity().String() {
		t.Fatalf("many-plan ledger is not sorted: %#v", many.Plans())
	}
	conflicting, err := NewConnectPlanEvaluation(planID, ConnectPlanNoRegistration, []RouteIdentity{{FlowID: "conflict"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewConnectEvaluationLedger([]ConnectPlanEvaluation{plan, conflicting}); err == nil || !strings.Contains(err.Error(), "conflicting plan identity") {
		t.Fatalf("conflicting duplicate plan error = %v", err)
	}
}

func TestConnectRecipientEvaluationReturnsClosedRejectionEvidence(t *testing.T) {
	planID := AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-a")))
	receiver := AdmitConnectReceiverIdentity(sha256.Sum256([]byte("receiver-a")))
	recipient := MustNodeDeliveryRecipient(identitytest.RootNode(t, "consumer"))
	accepted, err := NewConnectCandidateEvidence(receiver, recipient, "consumer", agentidentity.Identity{}, ConnectCandidateAccepted)
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := NewConnectCandidateEvidence(receiver, recipient, "consumer", agentidentity.Identity{}, ConnectCandidatePathMismatch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewConnectPlanEvaluation(planID, ConnectPlanResolved, nil, []ConnectCandidateEvidence{accepted, rejected}); err == nil || !strings.Contains(err.Error(), "conflicting outcomes") {
		t.Fatalf("conflicting candidate outcomes error = %v", err)
	}
	if _, err := NewConnectPlanEvaluation(planID, ConnectPlanNoRegistration, nil, []ConnectCandidateEvidence{rejected}); err == nil || !strings.Contains(err.Error(), "cannot carry candidate") {
		t.Fatalf("plan-level candidate error = %v", err)
	}
	if _, err := NewConnectPlanEvaluation(planID, ConnectPlanResolved, nil, nil); err == nil || !strings.Contains(err.Error(), "requires candidate") {
		t.Fatalf("resolved-without-candidate error = %v", err)
	}
	for _, resolution := range []ConnectPlanResolution{ConnectPlanRuntimeResolutionRequired, ConnectPlanResolutionBlocked, ConnectPlanNoRegistration} {
		plan, err := NewConnectPlanEvaluation(planID, resolution, nil, nil)
		if err != nil || plan.Resolution() != resolution || len(plan.Candidates()) != 0 {
			t.Fatalf("plan-level resolution %q = %#v err=%v", resolution.Code(), plan, err)
		}
	}
	for _, outcome := range []ConnectCandidateOutcome{ConnectCandidatePinMismatch, ConnectCandidatePathMismatch, ConnectCandidateAccepted} {
		candidate, err := NewConnectCandidateEvidence(receiver, recipient, "consumer", agentidentity.Identity{}, outcome)
		if err != nil || candidate.Outcome() != outcome {
			t.Fatalf("candidate outcome %q = %#v err=%v", outcome.Code(), candidate, err)
		}
	}
}

func TestEventWriteClassSettlementPolicyIsExhaustive(t *testing.T) {
	ledger, err := NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	route := DeliveryRoute{Recipient: MustNodeDeliveryRecipient(identitytest.RootNode(t, "consumer"))}
	valid := []struct {
		name   string
		build  func() (RouteSettlement, error)
		routes []DeliveryRoute
	}{
		{"normal delivery", func() (RouteSettlement, error) { return NewDeliverySettlement(EventWriteNormalPublication, ledger) }, []DeliveryRoute{route}},
		{"normal empty", func() (RouteSettlement, error) {
			return NewNoDeliverySettlement(EventWriteNormalPublication, NoDeliveryDeclaredConsumerNoPlan, ledger)
		}, nil},
		{"selected delivery", func() (RouteSettlement, error) {
			return NewDeliverySettlement(EventWriteSelectedForkPublication, ledger)
		}, []DeliveryRoute{route}},
		{"selected empty", func() (RouteSettlement, error) {
			return NewNoDeliverySettlement(EventWriteSelectedForkPublication, NoDeliveryMatchedNoRecipient, ledger)
		}, nil},
		{"directive", func() (RouteSettlement, error) {
			return NewNoDeliverySettlement(EventWriteDirectiveDirect, NoDeliveryNoSubscriberByDesign, ConnectEvaluationLedger{})
		}, nil},
		{"runtime log", func() (RouteSettlement, error) {
			return NewNoDeliverySettlement(EventWriteRuntimeLogDirect, NoDeliveryNoSubscriberByDesign, ConnectEvaluationLedger{})
		}, nil},
		{"inbound evidence", func() (RouteSettlement, error) {
			return NewNoDeliverySettlement(EventWriteInboundEvidenceDirect, NoDeliveryNoSubscriberByDesign, ConnectEvaluationLedger{})
		}, nil},
		{"historical replay", func() (RouteSettlement, error) {
			return NewDeliverySettlement(EventWriteHistoricalRunForkReplay, ConnectEvaluationLedger{})
		}, []DeliveryRoute{route}},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			settlement, err := test.build()
			if err != nil {
				t.Fatal(err)
			}
			if err := settlement.Validate(test.routes); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}

	hostile := []RouteSettlement{
		{},
		{writeClass: EventWriteNormalPublication, arm: routeSettlementDelivery},
		{writeClass: EventWriteDirectiveDirect, arm: routeSettlementDelivery},
		{writeClass: EventWriteDirectiveDirect, arm: routeSettlementNoDelivery, reason: NoDeliveryNoSubscriberByDesign, ledger: ledger},
		{writeClass: EventWriteHistoricalRunForkReplay, arm: routeSettlementNoDelivery, reason: NoDeliveryMatchedNoRecipient},
	}
	for index, settlement := range hostile {
		if err := settlement.Validate(nil); err == nil {
			t.Fatalf("hostile settlement %d was accepted", index)
		}
	}
}

func TestRouteSettlementStrictDecodeRejectsUnknownAndDualShapes(t *testing.T) {
	for _, raw := range []string{
		`{"write_class":"normal_publication","arm":"delivery","unknown":true,"evaluation":{"plans":[]}}`,
		`{"write_class":"normal_publication","arm":"delivery","reason":"matched_no_recipient","evaluation":{"plans":[]}}`,
		`{"write_class":"future","arm":"no_delivery","reason":"matched_no_recipient","evaluation":{"plans":[]}}`,
		`{"write_class":"normal_publication","arm":"no_delivery","reason":"matched_no_recipient","evaluation":{}}`,
		`{"write_class":"normal_publication","arm":"no_delivery","reason":"matched_no_recipient","evaluation":{"plans":null}}`,
		`{"write_class":"normal_publication","arm":"no_delivery","reason":"matched_no_recipient","evaluation":{"plans":[{"plan_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","resolution":"no_registration"}]}}`,
		`{"write_class":"normal_publication","arm":"no_delivery","reason":"matched_no_recipient","evaluation":{"plans":[]}} {}`,
	} {
		var settlement RouteSettlement
		if err := json.Unmarshal([]byte(raw), &settlement); err == nil {
			t.Fatalf("json.Unmarshal accepted %s", raw)
		}
	}
}

func TestRouteSettlementRoundTripsExplicitEmptyPlanTargets(t *testing.T) {
	plan, err := NewConnectPlanEvaluation(
		AdmitConnectPlanIdentity(sha256.Sum256([]byte("no-registration"))),
		ConnectPlanNoRegistration,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewConnectEvaluationLedger([]ConnectPlanEvaluation{plan})
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := NewNoDeliverySettlement(EventWriteNormalPublication, NoDeliveryMatchedNoRecipient, ledger)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := json.Marshal(settlement)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"targets":[]`) {
		t.Fatalf("encoded settlement does not preserve explicit empty targets: %s", raw)
	}
	var restored RouteSettlement
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("round-trip explicit empty targets: %v", err)
	}
	if err := restored.Validate(nil); err != nil {
		t.Fatalf("restored settlement: %v", err)
	}
}
