package runforkadmission

import (
	"testing"
	"time"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
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
	plan.PendingWork[0].FlowInstance = "review/inst-1"
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

func TestAdmitContractFrontier_InfersFlowInstanceRouteFromEventName(t *testing.T) {
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
	if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].SubscriberID != "reviewer-inst-1" {
		t.Fatalf("derived recipients = %#v, want inferred materialized reviewer-inst-1", event.DerivedRecipients)
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
