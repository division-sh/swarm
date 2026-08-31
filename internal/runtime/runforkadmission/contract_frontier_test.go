package runforkadmission

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestAdmitContractFrontier_DerivesSelectedContractRecipientsWithoutMutating(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if admission.Owner != runfork.RunForkContractFrontierAdmissionOwner {
		t.Fatalf("owner = %q, want %q", admission.Owner, runfork.RunForkContractFrontierAdmissionOwner)
	}
	if !admission.NonMutating || admission.HistoricalExecutionSupported {
		t.Fatalf("admission mutation flags = non_mutating:%v historical_supported:%v", admission.NonMutating, admission.HistoricalExecutionSupported)
	}
	if admission.ContractSelection.Mode != "selected_contracts" {
		t.Fatalf("contract selection = %#v, want selected contracts", admission.ContractSelection)
	}
	if admission.FrontierEventCount != 1 || len(admission.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %d/%d, want 1", admission.FrontierEventCount, len(admission.FrontierEvents))
	}
	event := admission.FrontierEvents[0]
	if event.EventName != "producer/scan.requested" {
		t.Fatalf("event name = %q", event.EventName)
	}
	if !hasString(event.SourceSubscriberTypes, "node") || !hasString(event.SourceSubscriberIDs, "source-node") {
		t.Fatalf("source delivery evidence = types:%v ids:%v", event.SourceSubscriberTypes, event.SourceSubscriberIDs)
	}
	consumerNode := identitytest.FlowNode(t, "consumer", "consumer-node").Key()
	if !hasString(event.WorkflowNodeSubscribers, consumerNode) {
		t.Fatalf("workflow node subscribers = %v, want consumer-node", event.WorkflowNodeSubscribers)
	}
	if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].Recipient.ID() != consumerNode || !event.DerivedRecipients[0].Recipient.IsNode() {
		t.Fatalf("derived recipients = %#v, want selected contract consumer-node", event.DerivedRecipients)
	}
	if !hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierExecutionUnsupported) {
		t.Fatalf("blockers = %#v, want execution unsupported", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_SelectedContractChangesRecipients(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node")
	sourceA := testContractFrontierSource("consumer-a")
	sourceB := testContractFrontierSource("consumer-b")

	admissionA, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            sourceA,
		ContractSelection: SelectedContractSelection(sourceA),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier A: %v", err)
	}
	admissionB, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            sourceB,
		ContractSelection: SelectedContractSelection(sourceB),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier B: %v", err)
	}
	gotA := admissionA.FrontierEvents[0].DerivedRecipients[0].Recipient.ID()
	gotB := admissionB.FrontierEvents[0].DerivedRecipients[0].Recipient.ID()
	if gotA != identitytest.FlowNode(t, "consumer", "consumer-a").Key() || gotB != identitytest.FlowNode(t, "consumer", "consumer-b").Key() {
		t.Fatalf("selected contract recipients = %q/%q, want consumer-a/consumer-b", gotA, gotB)
	}
}

func TestAdmitContractFrontier_ConnectMatchesConcreteTemplateSourceEndpoint(t *testing.T) {
	plan := testRunForkPlan("producer/inst-1/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node")
	plan.PendingWork[0].RoutingSource = testConcreteRoutingSource(t, "producer", "producer/inst-1")
	source := testContractFrontierTemplateConnectSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	event := admission.FrontierEvents[0]
	if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].Recipient.ID() != identitytest.FlowNode(t, "consumer", "consumer-node").Key() {
		t.Fatalf("derived recipients = %#v, want consumer-node through producer connect", event.DerivedRecipients)
	}
	if hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want concrete template source connect to resolve", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_ConnectRejectsConcreteTemplateIdentityWhenSourceRouteIsAbsent(t *testing.T) {
	plan := testRunForkPlan("producer/inst-1/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node")
	plan.PendingWork[0].RoutingSource = events.NoRoutingSource()
	source := testContractFrontierTemplateConnectSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	event := admission.FrontierEvents[0]
	if len(event.DerivedRecipients) != 0 {
		t.Fatalf("derived recipients = %#v, want concrete template source without route rejected", event.DerivedRecipients)
	}
	if !hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want concrete template source without route unresolved", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_ConnectRejectsUnrelatedTemplateSameLeaf(t *testing.T) {
	plan := testRunForkPlan("unrelated/inst-1/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node")
	plan.PendingWork[0].RoutingSource = testConcreteRoutingSource(t, "unrelated", "unrelated/inst-1")
	source := testContractFrontierTemplateConnectSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	event := admission.FrontierEvents[0]
	if len(event.DerivedRecipients) != 0 {
		t.Fatalf("derived recipients = %#v, want unrelated same-leaf template excluded", event.DerivedRecipients)
	}
	if !hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want unrelated same-leaf template to remain unresolved", admission.UnsupportedBlockers)
	}
}

func TestSelectedContractAdmissionsEnforceProducerMode(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		eventName string
		source    events.RouteIdentity
	}{
		{name: "template rejects base identity", mode: "template", eventName: "producer/scan.requested", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer"}},
		{name: "static rejects descendant identity", mode: "static", eventName: "producer/inst-1/scan.requested", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"}},
		{name: "singleton rejects descendant identity", mode: "singleton", eventName: "producer/inst-1/scan.requested", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := testContractFrontierConnectSource(tc.mode)
			frontierPlan := testRunForkPlan(tc.eventName, runfork.RunForkPendingClassificationPending, "node", "source-node")
			frontierPlan.PendingWork[0].RoutingSource = testRoutingSourceForRoute(t, tc.source)
			frontier, err := AdmitContractFrontier(ContractFrontierRequest{
				Plan:              frontierPlan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source),
			})
			if err != nil {
				t.Fatalf("AdmitContractFrontier: %v", err)
			}
			if len(frontier.FrontierEvents) != 1 || len(frontier.FrontierEvents[0].DerivedRecipients) != 0 {
				t.Fatalf("frontier events = %#v, want producer mode mismatch rejected", frontier.FrontierEvents)
			}
			if !hasBlocker(frontier.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
				t.Fatalf("frontier blockers = %#v, want unresolved route", frontier.UnsupportedBlockers)
			}

			historyPlan := testRunForkPlan(tc.eventName, runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
			historyPlan.PendingWork[0].RoutingSource = testRoutingSourceForRoute(t, tc.source)
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
				t.Fatalf("selected route events = %#v, want producer mode mismatch rejected", history.SelectedRouteEvents)
			}
		})
	}
}

func TestSelectedContractAdmissionsPreserveRootAndCarrierPoliciesAndRuntimeIncompleteFanout(t *testing.T) {
	for _, includeRuntimeReceiver := range []bool{false, true} {
		name := "static root and child"
		if includeRuntimeReceiver {
			name = "static root and child plus runtime receiver"
		}
		t.Run(name, func(t *testing.T) {
			source := testContractFrontierMixedReceiverSource(t, includeRuntimeReceiver)
			frontierPlan := testRunForkPlan("producer/work.ready", runfork.RunForkPendingClassificationPending, "node", "source-node")
			frontier, err := AdmitContractFrontier(ContractFrontierRequest{
				Plan:              frontierPlan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source),
			})
			if err != nil {
				t.Fatalf("AdmitContractFrontier: %v", err)
			}
			if len(frontier.FrontierEvents) != 1 {
				t.Fatalf("frontier events = %#v, want one", frontier.FrontierEvents)
			}
			assertContractFrontierMixedRecipients(t, frontier.FrontierEvents[0].DerivedRecipients)
			if got := hasBlocker(frontier.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved); got != includeRuntimeReceiver {
				t.Fatalf("frontier unresolved blocker = %v, want %v: %#v", got, includeRuntimeReceiver, frontier.UnsupportedBlockers)
			}

			historyPlan := testRunForkPlan("producer/work.ready", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
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
			if len(history.SelectedRouteEvents) != 1 {
				t.Fatalf("selected route events = %#v, want one", history.SelectedRouteEvents)
			}
			assertContractFrontierMixedRecipients(t, history.SelectedRouteEvents[0].DerivedRecipients)
			wantDisposition := runfork.RunForkSelectedContractDispositionEvidenceOnly
			if includeRuntimeReceiver {
				wantDisposition = runfork.RunForkSelectedContractDispositionFailClosed
			}
			if history.SelectedRouteEvents[0].Disposition != wantDisposition {
				t.Fatalf("history disposition = %q, want %q", history.SelectedRouteEvents[0].Disposition, wantDisposition)
			}
			if got := hasBlocker(history.UnsupportedBlockers, runfork.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven); got != includeRuntimeReceiver {
				t.Fatalf("history dynamic blocker = %v, want %v: %#v", got, includeRuntimeReceiver, history.UnsupportedBlockers)
			}
		})
	}
}

func assertContractFrontierMixedRecipients(t *testing.T, recipients []runfork.RunForkContractFrontierRecipient) {
	t.Helper()
	want := map[string]bool{
		"node/" + identitytest.RootNode(t, "root-node").Key(): true,
		"agent/root-agent": true,
		"node/" + identitytest.FlowNode(t, "consumer", "consumer-node").Key(): true,
	}
	if len(recipients) != len(want) {
		t.Fatalf("recipients = %#v, want root node, root agent, and child carrier", recipients)
	}
	for _, recipient := range recipients {
		key := recipient.Recipient.Code() + "/" + recipient.Recipient.ID()
		wantConnect, ok := want[key]
		if !ok || (recipient.RouteSourceCode() == "connect_route_plan") != wantConnect {
			t.Fatalf("recipient = %#v, want direct root routes and compiled child recipient", recipient)
		}
	}
}

func TestAdmitContractFrontier_DeliveredCompletedHistoryIsNotFrontierWork(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if admission.FrontierEventCount != 0 || len(admission.FrontierEvents) != 0 {
		t.Fatalf("frontier events = %#v, want none for delivered/completed history", admission.FrontierEvents)
	}
	if len(admission.UnsupportedBlockers) != 0 {
		t.Fatalf("blockers = %#v, want none without unresolved frontier", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_CommittedReplayScopeMarkersAreNotFrontierWork(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationCommittedReplay, "platform", "replay-scope")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if admission.FrontierEventCount != 0 || len(admission.FrontierEvents) != 0 {
		t.Fatalf("frontier events = %#v, want none for replay-scope marker", admission.FrontierEvents)
	}
	if len(admission.UnsupportedBlockers) != 0 {
		t.Fatalf("blockers = %#v, want none without executable frontier work", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_DiagnosticPlatformOutcomesAreLineageOnly(t *testing.T) {
	for _, eventName := range []string{"platform.runtime_log", "platform.inbound_recorded"} {
		t.Run(eventName, func(t *testing.T) {
			plan := testRunForkPlan(eventName, runfork.RunForkPendingClassificationDeadLetter, "platform", "pipeline")
			source := testContractFrontierSource("consumer-node")

			admission, err := AdmitContractFrontier(ContractFrontierRequest{
				Plan:              plan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source),
			})
			if err != nil {
				t.Fatalf("AdmitContractFrontier: %v", err)
			}
			if admission.FrontierEventCount != 0 || len(admission.FrontierEvents) != 0 {
				t.Fatalf("frontier events = %#v, want none for diagnostic platform outcome", admission.FrontierEvents)
			}
			if len(admission.UnsupportedBlockers) != 0 {
				t.Fatalf("blockers = %#v, want none for diagnostic platform outcome", admission.UnsupportedBlockers)
			}
			if len(admission.LineageOnlyEvents) != 1 {
				t.Fatalf("lineage-only events = %#v, want one diagnostic lineage event", admission.LineageOnlyEvents)
			}
			lineage := admission.LineageOnlyEvents[0]
			if lineage.EventName != eventName {
				t.Fatalf("lineage event name = %q, want %q", lineage.EventName, eventName)
			}
			if lineage.Owner != runfork.RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner {
				t.Fatalf("lineage owner = %q, want %q", lineage.Owner, runfork.RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner)
			}
			if lineage.Disposition != runfork.RunForkContractFrontierDispositionLineageNoAction {
				t.Fatalf("lineage disposition = %q, want %q", lineage.Disposition, runfork.RunForkContractFrontierDispositionLineageNoAction)
			}
			if !hasString(lineage.SourceClassifications, runfork.RunForkPendingClassificationDeadLetter) || !hasString(lineage.SourceSubscriberTypes, "platform") {
				t.Fatalf("lineage evidence = classifications:%v subscriber_types:%v", lineage.SourceClassifications, lineage.SourceSubscriberTypes)
			}
		})
	}
}

func TestAdmitContractFrontier_NonDiagnosticPlatformDeadLetterRemainsFailClosed(t *testing.T) {
	plan := testRunForkPlan("platform.dead_letter", runfork.RunForkPendingClassificationDeadLetter, "platform", "pipeline")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if admission.FrontierEventCount != 1 || len(admission.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %#v, want non-diagnostic platform outcome to remain frontier", admission.FrontierEvents)
	}
	if len(admission.LineageOnlyEvents) != 0 {
		t.Fatalf("lineage-only events = %#v, want none for non-diagnostic platform outcome", admission.LineageOnlyEvents)
	}
	if !hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want unresolved-route blocker", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_SelectedDeadLetterRemainsExecutableFrontier(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationDeadLetter, "node", "source-node")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if admission.FrontierEventCount != 1 || len(admission.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %#v, want selected dead-letter source fact to remain frontier", admission.FrontierEvents)
	}
	if len(admission.LineageOnlyEvents) != 0 {
		t.Fatalf("lineage-only events = %#v, want none for selected dead-letter source fact", admission.LineageOnlyEvents)
	}
	if hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want no unresolved-route blocker for selected source fact", admission.UnsupportedBlockers)
	}
	if !hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierExecutionUnsupported) {
		t.Fatalf("blockers = %#v, want execution unsupported", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_MaterializesSourceFlowInstanceRoutes(t *testing.T) {
	plan := testRunForkPlan("review/inst-1/task.started", runfork.RunForkPendingClassificationPending, "node", "source-node")
	plan.PendingWork[0].RoutingSource = testConcreteRoutingSource(t, "review", "review/inst-1")
	source := testContractFrontierTemplateSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if admission.FrontierEventCount != 1 || len(admission.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %#v, want one instantiated frontier event", admission.FrontierEvents)
	}
	event := admission.FrontierEvents[0]
	if !hasString(event.SourceFlowInstances, "review/inst-1") {
		t.Fatalf("source flow instances = %v, want review/inst-1", event.SourceFlowInstances)
	}
	if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].Recipient.ID() != identitytest.FlowNode(t, "review", "reviewer").Key() {
		t.Fatalf("derived recipients = %#v, want materialized reviewer-inst-1", event.DerivedRecipients)
	}
	if event.DerivedRecipients[0].Path != "review/inst-1" {
		t.Fatalf("recipient path = %q, want review/inst-1", event.DerivedRecipients[0].Path)
	}
	if hasBlocker(admission.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want no unresolved-route blocker for materialized instance route", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_FailsClosedWithoutSelectedSource(t *testing.T) {
	_, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan: testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationPending, "node", "source-node"),
	})
	if err == nil {
		t.Fatal("AdmitContractFrontier error = nil, want selected source failure")
	}
}

func TestAdmitContractFrontier_DoesNotInferFlowInstanceRouteFromEventName(t *testing.T) {
	plan := testRunForkPlan("review/inst-1/task.started", runfork.RunForkPendingClassificationPending, "node", "source-node")
	plan.PendingWork[0].RoutingSource = events.NoRoutingSource()
	source := testContractFrontierTemplateSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	event := admission.FrontierEvents[0]
	if len(event.DerivedRecipients) != 0 {
		t.Fatalf("derived recipients = %#v, want no inferred materialized route", event.DerivedRecipients)
	}
}

func testRunForkPlan(eventName, classification, subscriberType, subscriberID string) runfork.RunForkPlan {
	now := time.Unix(1700001000, 0).UTC()
	eventID := uuid.NewString()
	routingSource, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID: "producer", FlowInstance: "producer", EntityID: "producer-entity",
	})
	if err != nil {
		panic(err)
	}
	return runfork.RunForkPlan{
		SourceRunID: uuid.NewString(),
		ForkPoint: runfork.RunForkPoint{
			Input:     eventID,
			EventID:   eventID,
			EventName: eventName,
			Timestamp: now,
		},
		PendingWork: []runfork.RunForkPendingWork{{
			EventID:        eventID,
			EventName:      eventName,
			RoutingSource:  routingSource,
			DeliveryID:     uuid.NewString(),
			SubscriberType: subscriberType,
			SubscriberID:   subscriberID,
			Classification: classification,
			Status:         "pending",
			CreatedAt:      now,
		}},
		PendingWorkCount: 1,
	}
}

func testContractFrontierSource(nodeID string) semanticview.Source {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path:   "producer",
		Events: map[string]runtimecontracts.EventCatalogEntry{"scan.requested": {}},
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			nodeID: {
				ID:           nodeID,
				SubscribesTo: []string{"scan.requested"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"scan.requested": {}},
	}
	root := runtimecontracts.FlowContractView{
		Paths:    runtimecontracts.FlowContractPaths{FlowPath: "."},
		Schema:   runtimecontracts.FlowSchemaDocument{Connect: []runtimecontracts.FlowConnect{{SourceLine: 1, Event: "scan.requested", From: "producer", To: "consumer"}}},
		Children: []runtimecontracts.FlowContractView{producer, consumer},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics:   runtimecontracts.WorkflowSemanticView{Name: "test-workflow", Version: "v-test"},
		Events:      map[string]runtimecontracts.EventCatalogEntry{"scan.requested": {}},
		RootSchema:  &root.Schema,
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"producer": producer.Schema, "consumer": consumer.Schema},
		FlowSources: map[string]runtimecontracts.FlowSource{
			".":        {FlowPath: ".", Schema: "schema.yaml", Children: []string{"producer", "consumer"}},
			"producer": {FlowPath: "producer", Schema: "producer/schema.yaml"},
			"consumer": {FlowPath: "consumer", Schema: "consumer/schema.yaml"},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByPath: map[string]*runtimecontracts.FlowContractView{
				".":        &root,
				"producer": &root.Children[0],
				"consumer": &root.Children[1],
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0],
				"consumer": &root.Children[1],
			},
		},
	}
	return semanticview.Wrap(mustCompileContractFrontierBundle(bundle))
}

func testContractFrontierTemplateSource() semanticview.Source {
	review := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "review"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
		},
		Path: "review",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"reviewer": {
				ID:           "reviewer-{instance_id}",
				SubscribesTo: []string{"task.started"},
				Produces:     []string{"task.started"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.started": {},
		},
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Path: ".", Children: []runtimecontracts.FlowContractView{review}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "test-workflow",
			Version: "v-test",
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByPath: map[string]*runtimecontracts.FlowContractView{
				".":      &root,
				"review": &root.Children[0],
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": &root.Children[0],
			},
		},
	})
}

func testContractFrontierTemplateConnectSource() semanticview.Source {
	return testContractFrontierConnectSource("template")
}

func testContractFrontierRootConnectSource(t testing.TB) semanticview.Source {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundleRoot := canonicalrouting.CopyRootOutputConnect(t, canonicalrouting.RootConnectNoEmitter)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, bundleRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load root connect fixture: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func testContractFrontierConnectSource(producerMode string) semanticview.Source {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: producerMode,
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path: "producer",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {},
		},
	}
	unrelated := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "unrelated"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path: "unrelated",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {},
		},
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-node": {
				ID:           "consumer-node",
				SubscribesTo: []string{"scan.requested"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"scan.requested": {}},
	}
	root := runtimecontracts.FlowContractView{
		Paths:    runtimecontracts.FlowContractPaths{FlowPath: "."},
		Schema:   runtimecontracts.FlowSchemaDocument{Connect: []runtimecontracts.FlowConnect{{SourceLine: 1, Event: "scan.requested", From: "producer", To: "consumer"}}},
		Children: []runtimecontracts.FlowContractView{producer, unrelated, consumer},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics:   runtimecontracts.WorkflowSemanticView{Name: "test-workflow", Version: "v-test"},
		RootSchema:  &root.Schema,
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"producer": producer.Schema, "unrelated": unrelated.Schema, "consumer": consumer.Schema},
		FlowSources: map[string]runtimecontracts.FlowSource{
			".":         {FlowPath: ".", Schema: "schema.yaml", Children: []string{"producer", "unrelated", "consumer"}},
			"producer":  {FlowPath: "producer", Schema: "producer/schema.yaml"},
			"unrelated": {FlowPath: "unrelated", Schema: "unrelated/schema.yaml"},
			"consumer":  {FlowPath: "consumer", Schema: "consumer/schema.yaml"},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByPath: map[string]*runtimecontracts.FlowContractView{
				".":         &root,
				"producer":  &root.Children[0],
				"unrelated": &root.Children[1],
				"consumer":  &root.Children[2],
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"unrelated": &root.Children[1],
				"consumer":  &root.Children[2],
			},
		},
	}
	return semanticview.Wrap(mustCompileContractFrontierBundle(bundle))
}

func mustCompileContractFrontierBundle(bundle *runtimecontracts.WorkflowContractBundle) *runtimecontracts.WorkflowContractBundle {
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
	}
	return bundle
}

func testContractFrontierMixedReceiverSource(t testing.TB, includeRuntimeReceiver bool) semanticview.Source {
	t.Helper()
	variant := canonicalrouting.CompositionConnectReceiverFanoutStatic
	if includeRuntimeReceiver {
		variant = canonicalrouting.CompositionConnectReceiverFanoutRuntimeIncomplete
	}
	repoRoot := canonicalrouting.RepoRoot(t)
	bundleRoot := canonicalrouting.CopyCompositionConnectReceiverFanout(t, variant)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		bundleRoot,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load composition connect receiver fanout: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func testConcreteRoutingSource(t testing.TB, flowID, flowInstance string) events.RoutingSource {
	t.Helper()
	source, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: flowID, FlowInstance: flowInstance, EntityID: flowID + "-entity",
	})
	if err != nil {
		t.Fatalf("construct concrete routing source: %v", err)
	}
	return source
}

func testRoutingSourceForRoute(t testing.TB, route events.RouteIdentity) events.RoutingSource {
	t.Helper()
	route = route.Normalized()
	if route.EntityID == "" {
		route.EntityID = route.FlowID + "-entity"
	}
	var (
		source events.RoutingSource
		err    error
	)
	if route.FlowInstance == route.FlowID {
		source, err = events.NewStaticFlowRoutingSource(route)
	} else {
		source, err = events.NewConcreteTemplateInstanceRoutingSource(route)
	}
	if err != nil {
		t.Fatalf("construct routing source: %v", err)
	}
	return source
}

func hasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func hasBlocker(blockers []runfork.RunForkUnsupportedBlocker, code string) bool {
	for _, blocker := range blockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}
