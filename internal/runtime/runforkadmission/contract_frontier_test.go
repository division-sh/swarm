package runforkadmission

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func TestAdmitContractFrontier_DerivesSelectedContractRecipientsWithoutMutating(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationPending, "node", "source-node")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if admission.Owner != store.RunForkContractFrontierAdmissionOwner {
		t.Fatalf("owner = %q, want %q", admission.Owner, store.RunForkContractFrontierAdmissionOwner)
	}
	if !admission.NonMutating || admission.HistoricalExecutionSupported {
		t.Fatalf("admission mutation flags = non_mutating:%v historical_supported:%v", admission.NonMutating, admission.HistoricalExecutionSupported)
	}
	if admission.ContractSelection.ContractsRoot != "/tmp/contracts-a" {
		t.Fatalf("contracts root = %q", admission.ContractSelection.ContractsRoot)
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
	if !hasString(event.WorkflowNodeSubscribers, "consumer-node") {
		t.Fatalf("workflow node subscribers = %v, want consumer-node", event.WorkflowNodeSubscribers)
	}
	if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].SubscriberID != "consumer-node" || event.DerivedRecipients[0].SubscriberType != "node" {
		t.Fatalf("derived recipients = %#v, want selected contract consumer-node", event.DerivedRecipients)
	}
	if !hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierExecutionUnsupported) {
		t.Fatalf("blockers = %#v, want execution unsupported", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_SelectedContractChangesRecipients(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationPending, "node", "source-node")
	sourceA := testContractFrontierSource("consumer-a")
	sourceB := testContractFrontierSource("consumer-b")

	admissionA, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            sourceA,
		ContractSelection: SelectedContractSelection(sourceA, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier A: %v", err)
	}
	admissionB, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            sourceB,
		ContractSelection: SelectedContractSelection(sourceB, "/tmp/contracts-b"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier B: %v", err)
	}
	gotA := admissionA.FrontierEvents[0].DerivedRecipients[0].SubscriberID
	gotB := admissionB.FrontierEvents[0].DerivedRecipients[0].SubscriberID
	if gotA != "consumer-a" || gotB != "consumer-b" {
		t.Fatalf("selected contract recipients = %q/%q, want consumer-a/consumer-b", gotA, gotB)
	}
}

func TestAdmitContractFrontier_ConnectMatchesConcreteTemplateSourceEndpoint(t *testing.T) {
	plan := testRunForkPlan("producer/inst-1/scan.requested", store.RunForkPendingClassificationPending, "node", "source-node")
	plan.PendingWork[0].SourceRoute = events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"}
	source := testContractFrontierTemplateConnectSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	event := admission.FrontierEvents[0]
	if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].SubscriberID != "consumer-node" {
		t.Fatalf("derived recipients = %#v, want consumer-node through producer connect", event.DerivedRecipients)
	}
	if hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want concrete template source connect to resolve", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_ConnectRejectsConcreteTemplateIdentityWhenSourceRouteIsAbsent(t *testing.T) {
	plan := testRunForkPlan("producer/inst-1/scan.requested", store.RunForkPendingClassificationPending, "node", "source-node")
	source := testContractFrontierTemplateConnectSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	event := admission.FrontierEvents[0]
	if len(event.DerivedRecipients) != 0 {
		t.Fatalf("derived recipients = %#v, want concrete template source without route rejected", event.DerivedRecipients)
	}
	if !hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want concrete template source without route unresolved", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_ConnectRejectsUnrelatedTemplateSameLeaf(t *testing.T) {
	plan := testRunForkPlan("unrelated/inst-1/scan.requested", store.RunForkPendingClassificationPending, "node", "source-node")
	plan.PendingWork[0].SourceRoute = events.RouteIdentity{FlowID: "unrelated", FlowInstance: "unrelated/inst-1"}
	source := testContractFrontierTemplateConnectSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	event := admission.FrontierEvents[0]
	if len(event.DerivedRecipients) != 0 {
		t.Fatalf("derived recipients = %#v, want unrelated same-leaf template excluded", event.DerivedRecipients)
	}
	if !hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierRouteUnresolved) {
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
			frontierPlan := testRunForkPlan(tc.eventName, store.RunForkPendingClassificationPending, "node", "source-node")
			frontierPlan.PendingWork[0].SourceRoute = tc.source
			frontier, err := AdmitContractFrontier(ContractFrontierRequest{
				Plan:              frontierPlan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
			})
			if err != nil {
				t.Fatalf("AdmitContractFrontier: %v", err)
			}
			if len(frontier.FrontierEvents) != 1 || len(frontier.FrontierEvents[0].DerivedRecipients) != 0 {
				t.Fatalf("frontier events = %#v, want producer mode mismatch rejected", frontier.FrontierEvents)
			}
			if !hasBlocker(frontier.UnsupportedBlockers, store.RunForkBlockerContractFrontierRouteUnresolved) {
				t.Fatalf("frontier blockers = %#v, want unresolved route", frontier.UnsupportedBlockers)
			}

			historyPlan := testRunForkPlan(tc.eventName, store.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
			historyPlan.PendingWork[0].SourceRoute = tc.source
			historyFrontier, err := AdmitContractFrontier(ContractFrontierRequest{
				Plan:              historyPlan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
			})
			if err != nil {
				t.Fatalf("AdmitContractFrontier history: %v", err)
			}
			history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
				Plan:              historyPlan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
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
			frontierPlan := testRunForkPlan("producer/work.ready", store.RunForkPendingClassificationPending, "node", "source-node")
			frontier, err := AdmitContractFrontier(ContractFrontierRequest{
				Plan:              frontierPlan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
			})
			if err != nil {
				t.Fatalf("AdmitContractFrontier: %v", err)
			}
			if len(frontier.FrontierEvents) != 1 {
				t.Fatalf("frontier events = %#v, want one", frontier.FrontierEvents)
			}
			assertContractFrontierMixedRecipients(t, frontier.FrontierEvents[0].DerivedRecipients)
			if got := hasBlocker(frontier.UnsupportedBlockers, store.RunForkBlockerContractFrontierRouteUnresolved); got != includeRuntimeReceiver {
				t.Fatalf("frontier unresolved blocker = %v, want %v: %#v", got, includeRuntimeReceiver, frontier.UnsupportedBlockers)
			}

			historyPlan := testRunForkPlan("producer/work.ready", store.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
			historyFrontier, err := AdmitContractFrontier(ContractFrontierRequest{
				Plan:              historyPlan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
			})
			if err != nil {
				t.Fatalf("AdmitContractFrontier history: %v", err)
			}
			history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
				Plan:              historyPlan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
				FrontierAdmission: historyFrontier,
			})
			if err != nil {
				t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
			}
			if len(history.SelectedRouteEvents) != 1 {
				t.Fatalf("selected route events = %#v, want one", history.SelectedRouteEvents)
			}
			assertContractFrontierMixedRecipients(t, history.SelectedRouteEvents[0].DerivedRecipients)
			wantDisposition := store.RunForkSelectedContractDispositionEvidenceOnly
			if includeRuntimeReceiver {
				wantDisposition = store.RunForkSelectedContractDispositionFailClosed
			}
			if history.SelectedRouteEvents[0].Disposition != wantDisposition {
				t.Fatalf("history disposition = %q, want %q", history.SelectedRouteEvents[0].Disposition, wantDisposition)
			}
			if got := hasBlocker(history.UnsupportedBlockers, store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven); got != includeRuntimeReceiver {
				t.Fatalf("history dynamic blocker = %v, want %v: %#v", got, includeRuntimeReceiver, history.UnsupportedBlockers)
			}
		})
	}
}

func assertContractFrontierMixedRecipients(t *testing.T, recipients []store.RunForkContractFrontierRecipient) {
	t.Helper()
	want := map[string]bool{
		"node/root-node":     false,
		"agent/root-agent":   false,
		"node/consumer-node": true,
	}
	if len(recipients) != len(want) {
		t.Fatalf("recipients = %#v, want root node, root agent, and child carrier", recipients)
	}
	for _, recipient := range recipients {
		key := recipient.SubscriberType + "/" + recipient.SubscriberID
		wantCarrier, ok := want[key]
		if !ok || (recipient.RouteSource == "receiver_carrier") != wantCarrier {
			t.Fatalf("recipient = %#v, want root routes non-carrier and child route carrier", recipient)
		}
	}
}

func TestAdmitContractFrontier_DeliveredCompletedHistoryIsNotFrontierWork(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationDeliveredCompleted, "node", "source-node")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
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
	plan := testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationCommittedReplay, "platform", "replay-scope")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
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
			plan := testRunForkPlan(eventName, store.RunForkPendingClassificationDeadLetter, "platform", "pipeline")
			source := testContractFrontierSource("consumer-node")

			admission, err := AdmitContractFrontier(ContractFrontierRequest{
				Plan:              plan,
				Source:            source,
				ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
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
			if lineage.Owner != store.RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner {
				t.Fatalf("lineage owner = %q, want %q", lineage.Owner, store.RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner)
			}
			if lineage.Disposition != store.RunForkContractFrontierDispositionLineageNoAction {
				t.Fatalf("lineage disposition = %q, want %q", lineage.Disposition, store.RunForkContractFrontierDispositionLineageNoAction)
			}
			if !hasString(lineage.SourceClassifications, store.RunForkPendingClassificationDeadLetter) || !hasString(lineage.SourceSubscriberTypes, "platform") {
				t.Fatalf("lineage evidence = classifications:%v subscriber_types:%v", lineage.SourceClassifications, lineage.SourceSubscriberTypes)
			}
		})
	}
}

func TestAdmitContractFrontier_NonDiagnosticPlatformDeadLetterRemainsFailClosed(t *testing.T) {
	plan := testRunForkPlan("platform.dead_letter", store.RunForkPendingClassificationDeadLetter, "platform", "pipeline")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
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
	if !hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want unresolved-route blocker", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_SelectedDeadLetterRemainsExecutableFrontier(t *testing.T) {
	plan := testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationDeadLetter, "node", "source-node")
	source := testContractFrontierSource("consumer-node")

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
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
	if hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want no unresolved-route blocker for selected source fact", admission.UnsupportedBlockers)
	}
	if !hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierExecutionUnsupported) {
		t.Fatalf("blockers = %#v, want execution unsupported", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_MaterializesSourceFlowInstanceRoutes(t *testing.T) {
	plan := testRunForkPlan("review/inst-1/task.started", store.RunForkPendingClassificationPending, "node", "source-node")
	plan.PendingWork[0].SourceRoute = events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1"}
	source := testContractFrontierTemplateSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
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
	if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].SubscriberID != "reviewer-inst-1" {
		t.Fatalf("derived recipients = %#v, want materialized reviewer-inst-1", event.DerivedRecipients)
	}
	if event.DerivedRecipients[0].Path != "review/inst-1" {
		t.Fatalf("recipient path = %q, want review/inst-1", event.DerivedRecipients[0].Path)
	}
	if hasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("blockers = %#v, want no unresolved-route blocker for materialized instance route", admission.UnsupportedBlockers)
	}
}

func TestAdmitContractFrontier_FailsClosedWithoutSelectedSource(t *testing.T) {
	_, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan: testRunForkPlan("producer/scan.requested", store.RunForkPendingClassificationPending, "node", "source-node"),
	})
	if err == nil {
		t.Fatal("AdmitContractFrontier error = nil, want selected source failure")
	}
}

func TestAdmitContractFrontier_DoesNotInferFlowInstanceRouteFromEventName(t *testing.T) {
	plan := testRunForkPlan("review/inst-1/task.started", store.RunForkPendingClassificationPending, "node", "source-node")
	source := testContractFrontierTemplateSource()

	admission, err := AdmitContractFrontier(ContractFrontierRequest{
		Plan:              plan,
		Source:            source,
		ContractSelection: SelectedContractSelection(source, "/tmp/contracts-a"),
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	event := admission.FrontierEvents[0]
	if len(event.DerivedRecipients) != 0 {
		t.Fatalf("derived recipients = %#v, want no inferred materialized route", event.DerivedRecipients)
	}
}

func testRunForkPlan(eventName, classification, subscriberType, subscriberID string) store.RunForkPlan {
	now := time.Unix(1700001000, 0).UTC()
	eventID := uuid.NewString()
	return store.RunForkPlan{
		SourceRunID: uuid.NewString(),
		ForkPoint: store.RunForkPoint{
			Input:     eventID,
			EventID:   eventID,
			EventName: eventName,
			Timestamp: now,
		},
		PendingWork: []store.RunForkPendingWork{{
			EventID:        eventID,
			EventName:      eventName,
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
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			nodeID: {
				ID:           nodeID,
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, consumer}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "test-workflow",
			Version: "v-test",
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{
				"producer": {{Name: "scan_requested", Event: "scan.requested"}},
			},
			FlowInputEventPins: map[string][]runtimecontracts.FlowInputEventPin{
				"consumer": {{Name: "scan_requested", Event: "scan.requested"}},
			},
			CompositionConnects: []runtimecontracts.FlowPackageConnect{{
				SourceFile: "package.yaml",
				SourceLine: 1,
				From:       "producer.scan_requested",
				To:         "consumer.scan_requested",
			}},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0],
				"consumer": &root.Children[1],
			},
		},
	})
}

func testContractFrontierTemplateSource() semanticview.Source {
	review := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
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
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{review}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "test-workflow",
			Version: "v-test",
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": &root.Children[0],
			},
		},
	})
}

func testContractFrontierTemplateConnectSource() semanticview.Source {
	return testContractFrontierConnectSource("template")
}

func testContractFrontierConnectSource(producerMode string) semanticview.Source {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: producerMode,
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {},
		},
	}
	unrelated := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "unrelated", Flow: "unrelated"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "unrelated",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {},
		},
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-node": {
				ID:           "consumer-node",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, unrelated, consumer}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "test-workflow",
			Version: "v-test",
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{
				"producer":  {{Name: "scan_requested", Event: "scan.requested"}},
				"unrelated": {{Name: "scan_requested", Event: "scan.requested"}},
			},
			FlowInputEventPins: map[string][]runtimecontracts.FlowInputEventPin{
				"consumer": {{Name: "scan_requested", Event: "scan.requested"}},
			},
			CompositionConnects: []runtimecontracts.FlowPackageConnect{{
				SourceFile: "package.yaml",
				SourceLine: 1,
				From:       "producer.scan_requested",
				To:         "consumer.scan_requested",
			}},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"unrelated": &root.Children[1],
				"consumer":  &root.Children[2],
			},
		},
	})
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

func hasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func hasBlocker(blockers []store.RunForkUnsupportedBlocker, code string) bool {
	for _, blocker := range blockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}
