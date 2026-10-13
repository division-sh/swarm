package runfork

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func fanOutReplayAdmissionFixture(plan RunForkPlan) RunForkReplayResumeAdmission {
	facts := map[string]string{
		RunForkPendingClassificationPending:            RunForkReplayResumeFactDeliveryPendingHistory,
		RunForkPendingClassificationInProgress:         RunForkReplayResumeFactDeliveryInProgressHistory,
		RunForkPendingClassificationFailedRetryable:    RunForkReplayResumeFactDeliveryFailedHistory,
		RunForkPendingClassificationDeliveredCompleted: RunForkReplayResumeFactDeliveryCompletedHistory,
		RunForkPendingClassificationDeadLetter:         RunForkReplayResumeFactDeliveryDeadLetterHistory,
		RunForkPendingClassificationCanceled:           RunForkReplayResumeFactDeliveryCanceledHistory,
	}
	admission := RunForkReplayResumeAdmission{Owner: RunForkReplayResumeAdmissionOwner}
	blockers := make(map[string]bool)
	for _, item := range plan.PendingWork {
		if item.DeliveryID == "" {
			continue
		}
		disposition := RunForkReplayResumeDisposition{
			Fact: facts[item.Classification], EventID: item.EventID, DeliveryID: item.DeliveryID,
			SubscriberType: item.SubscriberType, SubscriberID: item.SubscriberID, Classification: item.Classification,
		}
		switch item.Classification {
		case RunForkPendingClassificationDeliveredCompleted, RunForkPendingClassificationCanceled:
			disposition.Disposition = RunForkReplayResumeDispositionLineageOnly
		default:
			if RunForkPendingWorkReplayableForHistoricalReplay(item) {
				disposition.Disposition = RunForkReplayResumeDispositionForkReplay
			} else {
				disposition.Disposition = RunForkReplayResumeDispositionFailClosedBlocker
				disposition.BlockerCode = RunForkBlockerDeliveryHistoryUnproven
				if item.SubscriberType != "agent" {
					disposition.BlockerCode = RunForkBlockerNonAgentDeliveryReplayUnsupported
				}
			}
		}
		admission.Dispositions = append(admission.Dispositions, disposition)
		if disposition.BlockerCode != "" && !blockers[disposition.BlockerCode] {
			blockers[disposition.BlockerCode] = true
			admission.UnsupportedBlockers = append(admission.UnsupportedBlockers, RunForkUnsupportedBlocker{
				Code: disposition.BlockerCode, Message: "canonical source delivery blocker",
			})
		}
	}
	return RecalculateReplayResumeAdmission(admission)
}

func fanOutReplayAdmissionSnapshot(t testing.TB, value any) string {
	t.Helper()
	return fmt.Sprintf("%#v", value)
}

func fanOutDeploymentReplayAdmissionFixture(t testing.TB, phase RunForkDeploymentPendingPhase, mixed bool) RunForkPlan {
	t.Helper()
	plan := fanOutPendingPartitionFixture(t)
	original := plan.FanOutObligations[0].PendingReplays[0]
	obligation := &plan.FanOutObligations[0]
	obligation.Intent.Request.Deployment = &fanoutobligation.DeploymentOrigin{}
	obligation.Intent.Request.Key.DeploymentFeedID = uuid.NewString()
	obligation.Intent.Request.Key.TriggeringDeliveryID = ""
	obligation.PendingReplays = nil
	obligation.PendingDeployment = []RunForkDeploymentPendingEvent{{
		Ordinal: original.Ordinal, SourceEventID: original.SourceEventID,
		SourceDeliveryIDs: append([]string(nil), original.SourceDeliveryIDs...), Phase: phase,
	}}
	count := 2
	if mixed {
		count = 5
		obligation.PendingDeployment[0].InheritedDisposition = original.InheritedDisposition
	}
	plan.PendingWork = append([]RunForkPendingWork(nil), plan.PendingWork[:count]...)
	source, err := events.NewDeploymentFeedRoutingSource(".")
	if err != nil {
		t.Fatal(err)
	}
	now := plan.PendingWork[0].CreatedAt
	for index := range plan.PendingWork {
		item := &plan.PendingWork[index]
		item.RoutingSource = source
		if index < 2 {
			item.ClaimVersion, item.RetryCount, item.ActiveSessionID, item.StartedAt = 0, 0, "", nil
			if phase == RunForkDeploymentPendingReceiver {
				item.ContinuationHandoffAt = &now
			}
		}
	}
	if phase == RunForkDeploymentPendingReceiver {
		plan.PendingWork = append(plan.PendingWork, RunForkPendingWork{
			EventID: original.SourceEventID, SubscriberType: "platform", SubscriberID: "pipeline",
			Classification: RunForkPendingClassificationDeliveredCompleted, ReceiptOutcome: "success", ReceiptAt: &now,
		})
	}
	return plan
}

func TestRunForkSelectedFanOutReplayAdmissionMixedPartition(t *testing.T) {
	states := []struct{ classification, status string }{
		{RunForkPendingClassificationPending, "pending"},
		{RunForkPendingClassificationInProgress, "in_progress"},
		{RunForkPendingClassificationFailedRetryable, "failed"},
	}
	for _, node := range states {
		for _, agent := range states {
			for _, fresh := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/fresh=%t", node.classification, agent.classification, fresh), func(t *testing.T) {
					plan := fanOutPendingPartitionFixture(t)
					plan.PendingWork[0].Classification, plan.PendingWork[0].Status = node.classification, node.status
					plan.PendingWork[1].Classification, plan.PendingWork[1].Status = agent.classification, agent.status
					checkFanOutReplayMixedPartition(t, plan, fresh)
				})
			}
		}
	}
}

func TestSelectedFanOutReplayDeliveryAdmissionRequiresExactDisposition(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*RunForkReplayResumeAdmission, *RunForkPendingWork)
	}{
		{"exact", func(*RunForkReplayResumeAdmission, *RunForkPendingWork) {}},
		{"foreign_admission", func(a *RunForkReplayResumeAdmission, _ *RunForkPendingWork) { a.Owner = "foreign" }},
		{"missing", func(a *RunForkReplayResumeAdmission, _ *RunForkPendingWork) { a.Dispositions = a.Dispositions[1:] }},
		{"duplicate", func(a *RunForkReplayResumeAdmission, _ *RunForkPendingWork) {
			a.Dispositions = append(a.Dispositions, a.Dispositions[0])
		}},
		{"unrelated_owner", func(a *RunForkReplayResumeAdmission, _ *RunForkPendingWork) { a.Dispositions[0].Owner = "foreign" }},
		{"terminal", func(a *RunForkReplayResumeAdmission, _ *RunForkPendingWork) {
			a.Dispositions[0].Disposition = RunForkReplayResumeDispositionLineageOnly
		}},
		{"blocked", func(a *RunForkReplayResumeAdmission, _ *RunForkPendingWork) {
			a.Dispositions[0].BlockerCode = RunForkBlockerDeliveryHistoryUnproven
		}},
		{"wrong_fact", func(a *RunForkReplayResumeAdmission, _ *RunForkPendingWork) {
			a.Dispositions[0].Fact = RunForkReplayResumeFactDeliveryCompletedHistory
		}},
		{"wrong_classification", func(a *RunForkReplayResumeAdmission, _ *RunForkPendingWork) {
			a.Dispositions[0].Classification = RunForkPendingClassificationDeliveredCompleted
		}},
		{"foreign_delivery", func(_ *RunForkReplayResumeAdmission, i *RunForkPendingWork) { i.DeliveryID = uuid.NewString() }},
		{"foreign_event", func(_ *RunForkReplayResumeAdmission, i *RunForkPendingWork) { i.EventID = uuid.NewString() }},
		{"receipt_only", func(_ *RunForkReplayResumeAdmission, i *RunForkPendingWork) {
			i.DeliveryID, i.SubscriberType, i.SubscriberID = "", "platform", "pipeline"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := fanOutPendingPartitionFixture(t)
			admission, err := RunForkSelectedFanOutReplayAdmission(plan, fanOutReplayAdmissionFixture(plan))
			if err != nil {
				t.Fatal(err)
			}
			item := plan.PendingWork[0]
			tc.edit(&admission, &item)
			if got := SelectedFanOutReplayDeliveryAdmitted(admission, item); got != (tc.name == "exact") {
				t.Fatalf("exact admission = %t: item=%+v admission=%+v", got, item, admission)
			}
		})
	}
}

func checkFanOutReplayMixedPartition(t *testing.T, plan RunForkPlan, fresh bool) {
	t.Helper()
	if fresh {
		for index := 0; index < 2; index++ {
			item := &plan.PendingWork[index]
			item.ClaimVersion, item.RetryCount, item.ActiveSessionID, item.StartedAt = 0, 0, "", nil
		}
	}
	admission := fanOutReplayAdmissionFixture(plan)
	got, err := RunForkSelectedFanOutReplayAdmission(plan, admission)
	if err != nil {
		t.Fatal(err)
	}
	if !got.DeliveryEventReplayReady || !got.BoundedReplaySupported || !got.ReplayResumeFactsPresent || got.StateOnlyExecutionReady || len(got.UnsupportedBlockers) != 0 {
		t.Fatalf("bounded replay readiness = %+v", got)
	}
	for index, disposition := range got.Dispositions {
		want := RunForkReplayResumeDispositionLineageOnly
		if index < 2 {
			want = RunForkReplayResumeDispositionForkReplay
			if RunForkPendingWorkReplayableForHistoricalReplay(plan.PendingWork[index]) != (fresh && index == 1 && plan.PendingWork[1].Classification == RunForkPendingClassificationPending) {
				t.Fatal("selected admission widened the generic replay predicate")
			}
		}
		if disposition.Disposition != want || disposition.BlockerCode != "" || disposition.Owner != RunForkSelectedFanOutReplayAdmissionOwner || disposition.Classification != admission.Dispositions[index].Classification || disposition.Fact != admission.Dispositions[index].Fact {
			t.Fatalf("recipient %d = %+v; want %s without classification conversion", index, disposition, want)
		}
	}
	repeated, err := RunForkSelectedFanOutReplayAdmission(plan, got)
	if err != nil || fanOutReplayAdmissionSnapshot(t, repeated) != fanOutReplayAdmissionSnapshot(t, got) {
		t.Fatalf("repeat admission changed the partition: %+v, %v", repeated, err)
	}
}

func TestRunForkSelectedFanOutReplayAdmissionRejectsInexactDispositions(t *testing.T) {
	for _, scope := range []string{"handler", "deployment"} {
		for _, tc := range []struct {
			name   string
			mutate func(*RunForkReplayResumeAdmission)
		}{
			{"missing_owed", func(a *RunForkReplayResumeAdmission) { a.Dispositions = a.Dispositions[1:] }},
			{"missing_terminal", func(a *RunForkReplayResumeAdmission) { a.Dispositions = a.Dispositions[:4] }},
			{"missing_all", func(a *RunForkReplayResumeAdmission) { a.Dispositions = nil }},
			{"duplicate_owed", func(a *RunForkReplayResumeAdmission) { a.Dispositions = append(a.Dispositions, a.Dispositions[0]) }},
			{"duplicate_terminal", func(a *RunForkReplayResumeAdmission) { a.Dispositions = append(a.Dispositions, a.Dispositions[3]) }},
			{"extra_delivery", func(a *RunForkReplayResumeAdmission) {
				extra := a.Dispositions[0]
				extra.DeliveryID = uuid.NewString()
				a.Dispositions = append(a.Dispositions, extra)
			}},
			{"substituted_event", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].EventID = uuid.NewString() }},
			{"substituted_delivery", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].DeliveryID = uuid.NewString() }},
			{"substituted_type", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].SubscriberType = "agent" }},
			{"substituted_subscriber", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].SubscriberID = "other" }},
			{"substituted_classification", func(a *RunForkReplayResumeAdmission) {
				a.Dispositions[0].Classification = RunForkPendingClassificationInProgress
			}},
			{"substituted_fact", func(a *RunForkReplayResumeAdmission) {
				a.Dispositions[0].Fact = RunForkReplayResumeFactDeliveryFailedHistory
			}},
			{"other_family_substitution", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].Fact = RunForkReplayResumeFactTimerHistory }},
			{"padded_event", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].EventID += " " }},
			{"padded_delivery", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].DeliveryID += " " }},
			{"padded_subscriber", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].SubscriberID += " " }},
			{"receipt_substitution", func(a *RunForkReplayResumeAdmission) {
				a.Dispositions[0].DeliveryID, a.Dispositions[0].SubscriberType = "", "platform"
			}},
			{"unsupported_disposition", func(a *RunForkReplayResumeAdmission) {
				a.Dispositions[0].Disposition = RunForkReplayResumeDispositionReconstruct
			}},
			{"unrelated_blocker", func(a *RunForkReplayResumeAdmission) {
				a.Dispositions[0].BlockerCode = RunForkBlockerTimerHistoryUnproven
			}},
			{"missing_blocker", func(a *RunForkReplayResumeAdmission) { a.Dispositions[0].BlockerCode = "" }},
			{"terminal_replay", func(a *RunForkReplayResumeAdmission) {
				a.Dispositions[2].Disposition = RunForkReplayResumeDispositionForkReplay
			}},
			{"unfinished_lineage", func(a *RunForkReplayResumeAdmission) {
				a.Dispositions[0].Disposition, a.Dispositions[0].BlockerCode = RunForkReplayResumeDispositionLineageOnly, ""
			}},
			{"noncanonical_owner", func(a *RunForkReplayResumeAdmission) { a.Owner = "other" }},
		} {
			t.Run(scope+"/"+tc.name, func(t *testing.T) {
				plan := fanOutPendingPartitionFixture(t)
				if scope == "deployment" {
					plan = fanOutDeploymentReplayAdmissionFixture(t, RunForkDeploymentPendingReceiver, true)
				}
				admission := fanOutReplayAdmissionFixture(plan)
				tc.mutate(&admission)
				beforePlan, beforeAdmission := fanOutReplayAdmissionSnapshot(t, plan), fanOutReplayAdmissionSnapshot(t, admission)
				got, err := RunForkSelectedFanOutReplayAdmission(plan, admission)
				if err == nil || !reflect.DeepEqual(got, RunForkReplayResumeAdmission{}) {
					t.Fatalf("inexact dispositions admitted: %+v, %v", got, err)
				}
				if beforePlan != fanOutReplayAdmissionSnapshot(t, plan) || beforeAdmission != fanOutReplayAdmissionSnapshot(t, admission) {
					t.Fatal("refusal mutated source evidence")
				}
			})
		}
	}
}

func TestRunForkSelectedFanOutReplayAdmissionPreservesUnrelatedBlockers(t *testing.T) {
	for _, index := range []int{0, 1} {
		t.Run(map[int]string{0: "non_agent", 1: "delivery_history"}[index], func(t *testing.T) {
			plan := fanOutPendingPartitionFixture(t)
			outside := plan.PendingWork[index]
			outside.EventID = uuid.NewString()
			var err error
			outside.DeliveryID, err = deliverylifecycle.DeliveryID(outside.EventID, outside.DeliveryRoute)
			if err != nil {
				t.Fatal(err)
			}
			plan.PendingWork = append(plan.PendingWork, outside)
			admission := fanOutReplayAdmissionFixture(plan)
			unrelated := admission.Dispositions[len(admission.Dispositions)-1]
			got, err := RunForkSelectedFanOutReplayAdmission(plan, admission)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Dispositions[len(got.Dispositions)-1], unrelated) || len(got.UnsupportedBlockers) != 1 || got.UnsupportedBlockers[0].Code != unrelated.BlockerCode || got.DeliveryEventReplayReady || got.BoundedReplaySupported {
				t.Fatalf("unrelated same-code blocker discharged: %+v", got)
			}
		})
	}
	plan := fanOutPendingPartitionFixture(t)
	admission := fanOutReplayAdmissionFixture(plan)
	otherFamily := RunForkReplayResumeDisposition{
		Fact: RunForkReplayResumeFactTimerHistory, Disposition: RunForkReplayResumeDispositionFailClosedBlocker,
		EventID: plan.PendingWork[0].EventID, BlockerCode: RunForkBlockerNonAgentDeliveryReplayUnsupported,
		Message: "another family still references this aggregate blocker",
	}
	admission.Dispositions = append(admission.Dispositions, otherFamily)
	admission.UnsupportedBlockers = append(admission.UnsupportedBlockers,
		RunForkUnsupportedBlocker{Code: RunForkBlockerSessionHistoryUnproven, Message: "unmatched other-family blocker"},
		RunForkUnsupportedBlocker{Code: RunForkBlockerCommittedReplayScopeReplayUnsupported, Message: "committed scope remains blocked"})
	got, err := RunForkSelectedFanOutReplayAdmission(plan, admission)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Dispositions[len(got.Dispositions)-1], otherFamily) || len(got.UnsupportedBlockers) != 3 || got.UnsupportedBlockers[0].Code != RunForkBlockerNonAgentDeliveryReplayUnsupported || got.UnsupportedBlockers[1] != admission.UnsupportedBlockers[2] || got.UnsupportedBlockers[2] != admission.UnsupportedBlockers[3] {
		t.Fatalf("other-family blockers changed: %+v", got)
	}
}

func TestRunForkSelectedFanOutReplayAdmissionPreservesSources(t *testing.T) {
	plan := fanOutPendingPartitionFixture(t)
	admission := fanOutReplayAdmissionFixture(plan)
	admission.UnsupportedBlockers = append(admission.UnsupportedBlockers, RunForkUnsupportedBlocker{Code: RunForkBlockerTimerHistoryUnproven, Message: "keep"})
	plan.ReplayResumeAdmission = admission
	plan.Entities = []RunForkEntityState{{Fields: map[string]any{"kept": "source"}}}
	plan.historicalInputs = map[string]InputPublication{"kept": {}}
	plan.historicalJoins = map[string]PublishedJoinOccurrence{"kept": {}}
	plan.historicalEventIDs = []string{"kept"}
	beforePlan, beforeAdmission := fanOutReplayAdmissionSnapshot(t, plan), fanOutReplayAdmissionSnapshot(t, admission)
	got, err := RunForkSelectedFanOutReplayAdmission(plan, admission)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.UnsupportedBlockers) != 1 || got.UnsupportedBlockers[0].Code != RunForkBlockerTimerHistoryUnproven {
		t.Fatalf("unrelated aggregate changed: %+v", got.UnsupportedBlockers)
	}
	got.Dispositions[0].SubscriberID = "output-only"
	got.UnsupportedBlockers[0].Message = "output-only"
	if beforePlan != fanOutReplayAdmissionSnapshot(t, plan) || beforeAdmission != fanOutReplayAdmissionSnapshot(t, admission) {
		t.Fatal("selected admission shares mutable slices with the source")
	}
	if len(plan.historicalInputs) != 1 || len(plan.historicalJoins) != 1 || !reflect.DeepEqual(plan.historicalEventIDs, []string{"kept"}) {
		t.Fatal("selected admission changed private source evidence")
	}
}

func TestRunForkSelectedFanOutReplayAdmissionNonFanOutRemainsGeneric(t *testing.T) {
	for _, scope := range []string{"none", "settled_outcome", "deployment"} {
		t.Run(scope, func(t *testing.T) {
			plan := fanOutPendingPartitionFixture(t)
			admission := fanOutReplayAdmissionFixture(plan)
			switch scope {
			case "none":
				plan.FanOutObligations = nil
			case "settled_outcome":
				plan.FanOutObligations[0].PendingReplays = nil
				plan.FanOutObligations[0].Outcomes = []fanoutobligation.Outcome{{
					Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted, SourceEventID: plan.PendingWork[0].EventID,
					InheritedDisposition: fanoutobligation.InheritedSucceeded, CreatedAt: plan.PendingWork[0].CreatedAt,
				}}
			case "deployment":
				plan.FanOutObligations[0].PendingReplays = nil
				plan.FanOutObligations[0].Intent.Cursor = 0
				plan.FanOutObligations[0].Intent.Request.Deployment = &fanoutobligation.DeploymentOrigin{}
			}
			got, err := RunForkSelectedFanOutReplayAdmission(plan, admission)
			if err != nil || !reflect.DeepEqual(got, admission) || RunForkPendingWorkReplayableForHistoricalReplay(plan.PendingWork[0]) {
				t.Fatalf("non-handler-pending scope acquired permission: %+v, %v", got, err)
			}
			if got.Dispositions[0].Disposition != RunForkReplayResumeDispositionFailClosedBlocker || got.Dispositions[0].BlockerCode != RunForkBlockerNonAgentDeliveryReplayUnsupported {
				t.Fatal("generic node refusal changed")
			}
		})
	}
}

func TestRunForkSelectedFanOutReplayAdmissionRejectsInvalidPartition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*RunForkPlan)
	}{
		{"missing_source_delivery", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs = nil }},
		{"omitted_owed_delivery", func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs = p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[:1]
		}},
		{"foreign_run", func(p *RunForkPlan) { p.SourceRunID = uuid.NewString() }},
		{"contradictory_status", func(p *RunForkPlan) { p.PendingWork[0].Status = "delivered" }},
		{"terminal_owed", func(p *RunForkPlan) {
			p.PendingWork[0].Classification, p.PendingWork[0].Status = RunForkPendingClassificationDeliveredCompleted, "delivered"
		}},
		{"duplicate_fixed_delivery", func(p *RunForkPlan) { p.PendingWork = append(p.PendingWork, p.PendingWork[0]) }},
		{"deployment_replay", func(p *RunForkPlan) {
			p.FanOutObligations[0].Intent.Request.Deployment = &fanoutobligation.DeploymentOrigin{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := fanOutPendingPartitionFixture(t)
			admission := fanOutReplayAdmissionFixture(plan)
			tc.mutate(&plan)
			before := fanOutReplayAdmissionSnapshot(t, plan)
			if got, err := RunForkSelectedFanOutReplayAdmission(plan, admission); err == nil || !reflect.DeepEqual(got, RunForkReplayResumeAdmission{}) {
				t.Fatalf("invalid fixed partition admitted: %+v, %v", got, err)
			}
			if before != fanOutReplayAdmissionSnapshot(t, plan) {
				t.Fatal("invalid partition refusal mutated the source")
			}
		})
	}
}

func TestRunForkSelectedFanOutReplayAdmissionDoesNotOwePlatformReceipts(t *testing.T) {
	plan := fanOutPendingPartitionFixture(t)
	admission := fanOutReplayAdmissionFixture(plan)
	receipt := plan.PendingWork[len(plan.PendingWork)-1]
	receiptDisposition := RunForkReplayResumeDisposition{
		Fact: RunForkReplayResumeFactDeliveryPendingHistory, Disposition: RunForkReplayResumeDispositionFailClosedBlocker,
		EventID: receipt.EventID, SubscriberType: receipt.SubscriberType, SubscriberID: receipt.SubscriberID,
		Classification: receipt.Classification, BlockerCode: RunForkBlockerNonAgentDeliveryReplayUnsupported,
		Message: "receipt-only fact cannot acquire delivery replay",
	}
	admission.Dispositions = append(admission.Dispositions, receiptDisposition)
	got, err := RunForkSelectedFanOutReplayAdmission(plan, admission)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Dispositions[len(got.Dispositions)-1], receiptDisposition) || len(got.UnsupportedBlockers) != 1 || got.UnsupportedBlockers[0].Code != RunForkBlockerNonAgentDeliveryReplayUnsupported || got.DeliveryEventReplayReady {
		t.Fatalf("receipt-only fact acquired replay or lost its blocker: %+v", got)
	}
	for index := 0; index < 2; index++ {
		if got.Dispositions[index].Disposition != RunForkReplayResumeDispositionForkReplay {
			t.Fatal("receipt presence erased an owed delivery")
		}
	}
}

func TestRunForkSelectedFanOutReplayAdmissionDeploymentPartition(t *testing.T) {
	for _, phase := range []RunForkDeploymentPendingPhase{RunForkDeploymentPendingPublication, RunForkDeploymentPendingReceiver} {
		for _, mixed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/mixed=%t", phase, mixed), func(t *testing.T) {
				checkFanOutDeploymentReplayAdmission(t, phase, mixed)
			})
		}
	}
}

func checkFanOutDeploymentReplayAdmission(t *testing.T, phase RunForkDeploymentPendingPhase, mixed bool) {
	t.Helper()
	plan := fanOutDeploymentReplayAdmissionFixture(t, phase, mixed)
	admission := fanOutReplayAdmissionFixture(plan)
	if phase == RunForkDeploymentPendingReceiver {
		receipt := plan.PendingWork[len(plan.PendingWork)-1]
		admission.Dispositions = append(admission.Dispositions, RunForkReplayResumeDisposition{
			Fact: RunForkReplayResumeFactDeliveryCompletedHistory, Disposition: RunForkReplayResumeDispositionLineageOnly,
			EventID: receipt.EventID, SubscriberType: receipt.SubscriberType, SubscriberID: receipt.SubscriberID,
			Classification: receipt.Classification,
		})
	}
	beforePlan, beforeAdmission := fanOutReplayAdmissionSnapshot(t, plan), fanOutReplayAdmissionSnapshot(t, admission)
	got, err := RunForkSelectedFanOutReplayAdmission(plan, admission)
	if err != nil {
		t.Fatal(err)
	}
	if !got.DeliveryEventReplayReady || !got.BoundedReplaySupported || got.StateOnlyExecutionReady || len(got.UnsupportedBlockers) != 0 {
		t.Fatalf("selected deployment readiness = %+v", got)
	}
	for index, disposition := range got.Dispositions {
		if disposition.DeliveryID == "" {
			if !reflect.DeepEqual(disposition, admission.Dispositions[index]) {
				t.Fatal("deployment pipeline receipt changed")
			}
			continue
		}
		want := RunForkReplayResumeDispositionLineageOnly
		if index < 2 {
			want = RunForkReplayResumeDispositionForkReplay
		}
		if disposition.Disposition != want || disposition.Classification != admission.Dispositions[index].Classification || disposition.BlockerCode != "" || disposition.Owner != RunForkSelectedFanOutReplayAdmissionOwner {
			t.Fatalf("deployment recipient %d = %+v; want %s", index, disposition, want)
		}
	}
	if beforePlan != fanOutReplayAdmissionSnapshot(t, plan) || beforeAdmission != fanOutReplayAdmissionSnapshot(t, admission) {
		t.Fatal("selected deployment admission mutated its source")
	}
	eventID := plan.FanOutObligations[0].PendingDeployment[0].SourceEventID
	if _, present, err := plan.FanOutPendingReplay(eventID); err != nil || present {
		t.Fatalf("deployment acquired the handler-only replay API: %t, %v", present, err)
	}
	if RunForkPendingWorkReplayableForHistoricalReplay(plan.PendingWork[0]) {
		t.Fatal("selected deployment widened the generic node predicate")
	}
}

func TestRunForkSelectedFanOutReplayAdmissionDeploymentPhaseProof(t *testing.T) {
	for _, tc := range []struct {
		name   string
		phase  RunForkDeploymentPendingPhase
		mutate func(*RunForkPlan)
	}{
		{"in_progress", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) {
			p.PendingWork[0].Classification, p.PendingWork[0].Status = RunForkPendingClassificationInProgress, "in_progress"
		}},
		{"retryable", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) {
			p.PendingWork[0].Classification, p.PendingWork[0].Status = RunForkPendingClassificationFailedRetryable, "failed"
		}},
		{"source_retry", RunForkDeploymentPendingPublication, func(p *RunForkPlan) { p.PendingWork[0].RetryCount = 1 }},
		{"source_session", RunForkDeploymentPendingPublication, func(p *RunForkPlan) { p.PendingWork[0].ActiveSessionID = uuid.NewString() }},
		{"source_started", RunForkDeploymentPendingPublication, func(p *RunForkPlan) {
			started := p.PendingWork[0].CreatedAt
			p.PendingWork[0].StartedAt = &started
		}},
		{"missing_handoff", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.PendingWork[0].ContinuationHandoffAt = nil }},
		{"unexpected_handoff", RunForkDeploymentPendingPublication, func(p *RunForkPlan) {
			handoff := p.PendingWork[0].CreatedAt
			p.PendingWork[0].ContinuationHandoffAt = &handoff
		}},
		{"missing_pipeline_receipt", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.PendingWork = p.PendingWork[:len(p.PendingWork)-1] }},
		{"duplicate_pipeline_receipt", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.PendingWork = append(p.PendingWork, p.PendingWork[len(p.PendingWork)-1]) }},
		{"failed_pipeline_receipt", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.PendingWork[len(p.PendingWork)-1].ReceiptOutcome = "dead_letter" }},
		{"wrong_source", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.PendingWork[0].RoutingSource = events.NoRoutingSource() }},
		{"wrong_phase", RunForkDeploymentPendingPublication, func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingDeployment[0].Phase = RunForkDeploymentPendingReceiver
		}},
		{"invalid_phase", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.FanOutObligations[0].PendingDeployment[0].Phase = "other" }},
		{"missing_delivery", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.FanOutObligations[0].PendingDeployment[0].SourceDeliveryIDs = nil }},
		{"omitted_delivery", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingDeployment[0].SourceDeliveryIDs = p.FanOutObligations[0].PendingDeployment[0].SourceDeliveryIDs[:1]
		}},
		{"terminal_owed", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingDeployment[0].SourceDeliveryIDs[0] = p.PendingWork[2].DeliveryID
		}},
		{"invalid_retained", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingDeployment[0].InheritedDisposition = fanoutobligation.InheritedNoRoute
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := fanOutDeploymentReplayAdmissionFixture(t, tc.phase, true)
			admission := fanOutReplayAdmissionFixture(plan)
			tc.mutate(&plan)
			before := fanOutReplayAdmissionSnapshot(t, plan)
			if got, err := RunForkSelectedFanOutReplayAdmission(plan, admission); err == nil || !reflect.DeepEqual(got, RunForkReplayResumeAdmission{}) {
				t.Fatalf("deployment phase proof weakened: %+v, %v", got, err)
			}
			if before != fanOutReplayAdmissionSnapshot(t, plan) {
				t.Fatal("deployment refusal changed source evidence")
			}
		})
	}
}

func TestRunForkSelectedFanOutReplayAdmissionRejectsAmbiguousOrigins(t *testing.T) {
	plan := fanOutDeploymentReplayAdmissionFixture(t, RunForkDeploymentPendingReceiver, true)
	admission := fanOutReplayAdmissionFixture(plan)
	pending := plan.FanOutObligations[0].PendingDeployment[0]
	handler := plan.FanOutObligations[0]
	handler.Intent.Request.Deployment = nil
	handler.Intent.Request.Key.DeploymentFeedID = ""
	handler.Intent.Request.Key.TriggeringDeliveryID = uuid.NewString()
	handler.PendingDeployment = nil
	handler.PendingReplays = []RunForkFanOutPendingReplay{{
		Ordinal: pending.Ordinal, SourceEventID: pending.SourceEventID,
		SourceDeliveryIDs: append([]string(nil), pending.SourceDeliveryIDs...), InheritedDisposition: pending.InheritedDisposition,
	}}
	plan.FanOutObligations = append(plan.FanOutObligations, handler)
	before := fanOutReplayAdmissionSnapshot(t, plan)
	if got, err := RunForkSelectedFanOutReplayAdmission(plan, admission); err == nil || !reflect.DeepEqual(got, RunForkReplayResumeAdmission{}) {
		t.Fatalf("one event borrowed two continuation owners: %+v, %v", got, err)
	}
	if before != fanOutReplayAdmissionSnapshot(t, plan) {
		t.Fatal("ambiguous-origin refusal mutated the source")
	}
}
