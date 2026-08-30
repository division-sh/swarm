package runforkadmission

import (
	"testing"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestAdmitSelectedContractRouteHistoryDerivesSelectedRoutesWithoutMutating(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	plan.UnsupportedBlockers = []runfork.RunForkUnsupportedBlocker{{
		Code: runfork.RunForkBlockerFlowRouteHistoryUnproven,
	}}
	source := testContractFrontierSource("consumer-node")
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}

	admission, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if admission.Owner != runfork.RunForkSelectedContractRouteAdmissionOwner {
		t.Fatalf("owner = %q, want %q", admission.Owner, runfork.RunForkSelectedContractRouteAdmissionOwner)
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
		event.Disposition != runfork.RunForkSelectedContractDispositionEvidenceOnly ||
		len(event.DerivedRecipients) != 1 ||
		event.DerivedRecipients[0].Recipient.ID() != identitytest.FlowNode(t, "consumer", "consumer-node").Key() {
		t.Fatalf("selected route event = %#v, want evidence-only selected consumer-node", event)
	}
	if !hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerSelectedContractRouteAdmissionNonMutating) {
		t.Fatalf("blockers = %#v, want non-mutating route admission blocker", admission.UnsupportedBlockers)
	}
	if !hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerFlowRouteHistoryUnproven) {
		t.Fatalf("blockers = %#v, want source route history blocker", admission.UnsupportedBlockers)
	}
	if !routeBoundaryHas(admission.InvalidPaths, "copy_source_routing_rules", runfork.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source routing_rules copy invalid", admission.InvalidPaths)
	}
	if !routeBoundaryHas(admission.BlockedSiblings, "mutating_route_reconstruction", runfork.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want mutating route reconstruction blocked", admission.BlockedSiblings)
	}
}

func TestAdmitSelectedContractRouteHistoryConnectMatchesConcreteTemplateSourceEndpoint(t *testing.T) {
	plan := testRunForkPlan("producer/inst-1/scan.requested", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	plan.PendingWork[0].RoutingSource = testConcreteRoutingSource(t, "producer", "producer/inst-1")
	source := testContractFrontierTemplateConnectSource()
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}

	admission, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if len(admission.SelectedRouteEvents) != 1 || len(admission.SelectedRouteEvents[0].DerivedRecipients) != 1 || admission.SelectedRouteEvents[0].DerivedRecipients[0].Recipient.ID() != identitytest.FlowNode(t, "consumer", "consumer-node").Key() {
		t.Fatalf("selected route events = %#v, want consumer-node through producer connect", admission.SelectedRouteEvents)
	}
}

func TestW2SelectedForkUsesPersistedCompiledEdgeAfterCurrentGraphChanges(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "consumer-b")
	plan.PendingWork[0].DeliveryRoute = testW2CompiledConnectRoute(t, "consumer-b")

	// The selected source no longer routes to consumer-b. Historical selection
	// must use the exact stamped compiled-edge claim rather than rematching it.
	source := testContractFrontierSource("consumer-a")
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if len(history.SelectedRouteEvents) != 1 {
		t.Fatalf("selected route events = %#v, want one source event", history.SelectedRouteEvents)
	}
	recipients := history.SelectedRouteEvents[0].DerivedRecipients
	if len(recipients) != 1 || recipients[0].Recipient.ID() != identitytest.RootNode(t, "consumer-b").Key() {
		t.Fatalf("derived recipients = %#v, want persisted consumer-b only", recipients)
	}
}

func testW2CompiledConnectRoute(t testing.TB, subscriberID string) events.DeliveryRoute {
	t.Helper()
	root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{
		Mode:      canonicalrouting.TemplateInstanceRouteSelect,
		SecondPin: canonicalrouting.TemplateInstanceNoSecondPin,
	})
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical connect fixture: %v", err)
	}
	var selected runtimepinrouting.ConnectRoutePlan
	for _, candidate := range runtimepinrouting.CompileConnectGraph(semanticview.Wrap(bundle)).Plans() {
		if candidate.ReceiverEndpoint().Readback().Pin == "deploy.done" {
			selected = candidate
			break
		}
	}
	if selected.ReceiverEndpoint().Readback().Pin == "" {
		t.Fatal("compiled connect graph has no deploy.done receiver pin")
	}
	recipientNode := identitytest.RootNode(t, subscriberID)
	target := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: "consumer-entity"}
	blueprint := runtimepinrouting.ConnectDeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(recipientNode),
		Target:    target,
		Handler:   runtimepinrouting.MustConnectReceiverHandler(recipientNode),
	}
	claim, err := runtimepinrouting.ConnectExecutionClaim(selected, blueprint)
	if err != nil {
		t.Fatalf("mint compiled connect execution claim: %v", err)
	}
	return events.DeliveryRoute{
		Recipient:    blueprint.Recipient,
		Target:       events.MustExistingEntityTarget(target),
		ConnectClaim: claim,
	}
}

func TestAdmitSelectedContractRouteHistoryRejectsConcreteTemplateIdentityWhenSourceRouteIsAbsent(t *testing.T) {
	plan := testRunForkPlan("producer/inst-1/scan.requested", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	plan.PendingWork[0].RoutingSource = events.NoRoutingSource()
	source := testContractFrontierTemplateConnectSource()
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}

	admission, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if len(admission.SelectedRouteEvents) != 1 || len(admission.SelectedRouteEvents[0].DerivedRecipients) != 0 {
		t.Fatalf("selected route events = %#v, want concrete template source without route rejected", admission.SelectedRouteEvents)
	}
}

func TestSelectedContractAdmissionsRejectConflictingExplicitTemplateIdentity(t *testing.T) {
	source := testContractFrontierTemplateConnectSource()
	frontierPlan := testRunForkPlan("producer/inst-1/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node")
	frontierPlan.PendingWork[0].RoutingSource = testConcreteRoutingSource(t, "unrelated", "unrelated/inst-1")
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              frontierPlan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if len(frontier.FrontierEvents) != 1 || len(frontier.FrontierEvents[0].DerivedRecipients) != 0 {
		t.Fatalf("frontier events = %#v, want conflicting explicit identity rejected", frontier.FrontierEvents)
	}
	if !hasBlocker(frontier.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("frontier blockers = %#v, want conflicting explicit identity unresolved", frontier.UnsupportedBlockers)
	}

	historyPlan := testRunForkPlan("producer/inst-1/scan.requested", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	historyPlan.PendingWork[0].RoutingSource = testConcreteRoutingSource(t, "unrelated", "unrelated/inst-1")
	historyFrontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              historyPlan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier history: %v", err)
	}
	history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              historyPlan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
		FrontierAdmission: historyFrontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if len(history.SelectedRouteEvents) != 1 || len(history.SelectedRouteEvents[0].DerivedRecipients) != 0 {
		t.Fatalf("selected route events = %#v, want conflicting explicit identity rejected", history.SelectedRouteEvents)
	}
}

func TestAdmitSelectedContractRouteHistoryConnectRejectsUnrelatedTemplateSameLeaf(t *testing.T) {
	plan := testRunForkPlan("unrelated/inst-1/scan.requested", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	plan.PendingWork[0].RoutingSource = testConcreteRoutingSource(t, "unrelated", "unrelated/inst-1")
	source := testContractFrontierTemplateConnectSource()
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}

	admission, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if len(admission.SelectedRouteEvents) != 1 || len(admission.SelectedRouteEvents[0].DerivedRecipients) != 0 {
		t.Fatalf("selected route events = %#v, want unrelated same-leaf template excluded", admission.SelectedRouteEvents)
	}
}

func TestAdmitSelectedContractRouteHistoryDoesNotDuplicateFrontierRecipients(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node")
	historyEventID := uuid.NewString()
	plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{
		EventID:        historyEventID,
		EventName:      "producer/scan.requested",
		RoutingSource:  plan.PendingWork[0].RoutingSource,
		DeliveryID:     uuid.NewString(),
		SubscriberType: "node",
		SubscriberID:   "completed-node",
		Classification: runfork.RunForkPendingClassificationDeliveredCompleted,
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
		ContractSelection: SelectedContractSelection(source),
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
		ContractSelection: SelectedContractSelection(source),
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
	if admission.FrontierAdmissionOwner != runfork.RunForkContractFrontierAdmissionOwner {
		t.Fatalf("frontier owner = %q", admission.FrontierAdmissionOwner)
	}
}

func TestAdmitSelectedContractRouteHistoryClassifiesDynamicFlowInstances(t *testing.T) {
	plan := testRunForkPlan("review/inst-1/task.started", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	plan.PendingWork[0].RoutingSource = testConcreteRoutingSource(t, "review", "review/inst-1")
	source := testContractFrontierTemplateSource()
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}

	admission, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
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
		admission.SelectedRouteEvents[0].DerivedRecipients[0].Recipient.ID() != identitytest.FlowNode(t, "review", "reviewer").Key() ||
		admission.SelectedRouteEvents[0].DerivedRecipients[0].Path != "review/inst-1" {
		t.Fatalf("selected route events = %#v, want materialized dynamic recipient reviewer-inst-1", admission.SelectedRouteEvents)
	}
	if !routeBoundaryHas(admission.BlockedSiblings, "dynamic_flow_instance_route_reconstruction", runfork.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want dynamic route reconstruction blocked", admission.BlockedSiblings)
	}
}

func TestAdmitSelectedContractRouteHistoryRequiresCanonicalFrontier(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node")
	source := testContractFrontierSource("consumer-node")
	_, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan:   plan,
		Source: source,
		FrontierAdmission: runfork.RunForkContractFrontierAdmission{
			Owner:       "cmd.swarm.local_frontier",
			NonMutating: true,
		},
	})
	if err == nil {
		t.Fatal("AdmitSelectedContractRouteHistory error = nil, want canonical frontier failure")
	}
}

func routeBoundaryHas(items []runfork.RunForkSelectedContractExecutionBoundary, concept, disposition string) bool {
	for _, item := range items {
		if item.Concept == concept && item.Disposition == disposition {
			return true
		}
	}
	return false
}
