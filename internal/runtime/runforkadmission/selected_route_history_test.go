package runforkadmission

import (
	"testing"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/store"
)

func TestAdmitSelectedContractRouteHistoryDerivesSelectedRoutesWithoutMutating(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	plan.UnsupportedBlockers = []store.RunForkUnsupportedBlocker{{
		Code: store.RunForkBlockerFlowRouteHistoryUnproven,
	}}
	source := testContractFrontierSource("consumer-node")
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}

	admission, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if admission.Owner != store.RunForkSelectedContractRouteAdmissionOwner {
		t.Fatalf("owner = %q, want %q", admission.Owner, store.RunForkSelectedContractRouteAdmissionOwner)
	}
	if !admission.NonMutating || admission.RouteReconstructionSupported {
		t.Fatalf("mutation flags = non_mutating:%v route_supported:%v", admission.NonMutating, admission.RouteReconstructionSupported)
	}
	if !admission.SourceRouteFactsPresent {
		t.Fatalf("source route facts present = false, want true")
	}
	if len(admission.SelectedRouteEvents) != 1 {
		t.Fatalf("selected route events = %#v, want one historical route event", admission.SelectedRouteEvents)
	}
	event := admission.SelectedRouteEvents[0]
	if event.EventName != "producer/scan.requested" ||
		event.SourceEventID == "" ||
		event.Disposition != store.RunForkSelectedContractDispositionEvidenceOnly ||
		len(event.DerivedRecipients) != 1 ||
		event.DerivedRecipients[0].SubscriberID != "consumer-node" {
		t.Fatalf("selected route event = %#v, want evidence-only selected consumer-node", event)
	}
	if !hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteAdmissionNonMutating) {
		t.Fatalf("blockers = %#v, want non-mutating route admission blocker", admission.UnsupportedBlockers)
	}
	if !hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerFlowRouteHistoryUnproven) {
		t.Fatalf("blockers = %#v, want source route history blocker", admission.UnsupportedBlockers)
	}
	if !routeBoundaryHas(admission.InvalidPaths, "copy_source_routing_rules", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source routing_rules copy invalid", admission.InvalidPaths)
	}
	if !routeBoundaryHas(admission.BlockedSiblings, "mutating_route_reconstruction", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want mutating route reconstruction blocked", admission.BlockedSiblings)
	}
}

func TestAdmitSelectedContractRouteHistoryDoesNotDuplicateFrontierRecipients(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationPending, "node", "source-node")
	historyEventID := uuid.NewString()
	plan.PendingWork = append(plan.PendingWork, store.RunForkPendingWork{
		EventID:        historyEventID,
		EventName:      "producer/scan.requested",
		DeliveryID:     uuid.NewString(),
		SubscriberType: "node",
		SubscriberID:   "completed-node",
		Classification: store.RunForkPendingClassificationDeliveredCompleted,
		Status:         "completed",
		CreatedAt:      plan.ForkPoint.Timestamp,
		DeliveredAt:    &plan.ForkPoint.Timestamp,
		ReceiptAt:      &plan.ForkPoint.Timestamp,
	})
	plan.PendingWorkCount = len(plan.PendingWork)
	source := testContractFrontierSource("consumer-node")
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if frontier.FrontierEventCount != 1 {
		t.Fatalf("frontier events = %#v, want selected frontier work", frontier.FrontierEvents)
	}

	admission, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if len(admission.SelectedRouteEvents) != 1 {
		t.Fatalf("selected route events = %#v, want only same-name historical event", admission.SelectedRouteEvents)
	}
	if admission.SelectedRouteEvents[0].SourceEventID != historyEventID {
		t.Fatalf("route source event = %q, want historical event %q", admission.SelectedRouteEvents[0].SourceEventID, historyEventID)
	}
	if admission.FrontierAdmissionOwner != store.RunForkContractFrontierAdmissionOwner {
		t.Fatalf("frontier owner = %q", admission.FrontierAdmissionOwner)
	}
}

func TestAdmitSelectedContractRouteHistoryClassifiesDynamicFlowInstances(t *testing.T) {
	plan := testRunForkPlan("review/inst-1/task.started", store.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	plan.PendingWork[0].FlowInstance = "review/inst-1"
	source := testContractFrontierTemplateSource()
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}

	admission, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if !hasString(admission.DynamicFlowInstances, "review/inst-1") {
		t.Fatalf("dynamic flow instances = %v, want review/inst-1", admission.DynamicFlowInstances)
	}
	if len(admission.SelectedRouteEvents) != 1 ||
		len(admission.SelectedRouteEvents[0].DerivedRecipients) != 1 ||
		admission.SelectedRouteEvents[0].DerivedRecipients[0].SubscriberID != "reviewer-inst-1" ||
		admission.SelectedRouteEvents[0].DerivedRecipients[0].Path != "review/inst-1" {
		t.Fatalf("selected route events = %#v, want materialized dynamic recipient reviewer-inst-1", admission.SelectedRouteEvents)
	}
	if !routeBoundaryHas(admission.BlockedSiblings, "dynamic_flow_instance_route_reconstruction", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want dynamic route reconstruction blocked", admission.BlockedSiblings)
	}
}

func TestAdmitSelectedContractRouteHistoryRequiresCanonicalFrontier(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationPending, "node", "source-node")
	source := testContractFrontierSource("consumer-node")
	_, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:   plan,
		Source: source,
		FrontierAdmission: store.RunForkContractFrontierAdmission{
			Owner:       "cmd.swarm.local_frontier",
			NonMutating: true,
		},
	})
	if err == nil {
		t.Fatal("AdmitSelectedContractRouteHistory error = nil, want canonical frontier failure")
	}
}

func routeBoundaryHas(items []store.RunForkSelectedContractExecutionBoundary, concept, disposition string) bool {
	for _, item := range items {
		if item.Concept == concept && item.Disposition == disposition {
			return true
		}
	}
	return false
}
