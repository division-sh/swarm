package runforkpersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedFanOutInFlightAdmissionSurvivesMutationGate(t *testing.T) {
	for _, classification := range []string{runfork.RunForkPendingClassificationPending, runfork.RunForkPendingClassificationInProgress, runfork.RunForkPendingClassificationFailedRetryable} {
		t.Run(classification, func(t *testing.T) {
			runID, eventID := uuid.NewString(), uuid.NewString()
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, ".", "receiver")),
				Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID})}
			deliveryID, err := deliverylifecycle.DeliveryID(eventID, route)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Unix(100, 0).UTC()
			status := "pending"
			if classification == runfork.RunForkPendingClassificationInProgress {
				status = "in_progress"
			} else if classification == runfork.RunForkPendingClassificationFailedRetryable {
				status = "failed"
			}
			item := runfork.RunForkPendingWork{EventID: eventID, DeliveryID: deliveryID, DeliveryRoute: route,
				SubscriberType: "node", SubscriberID: route.Recipient.ID(), Classification: classification, Status: status,
				CreatedAt: now, StartedAt: &now, ClaimVersion: 3, RetryCount: 1}
			plan := runfork.RunForkPlan{SourceRunID: runID, PendingWork: []runfork.RunForkPendingWork{item},
				FanOutObligations: []runfork.RunForkFanOutObligation{{
					Intent: fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{Key: fanoutobligation.IntentKey{
						RunID: runID, TriggeringDeliveryID: uuid.NewString()}}, Cursor: 1},
					PendingReplays: []runfork.RunForkFanOutPendingReplay{{Ordinal: 0, SourceEventID: eventID, SourceDeliveryIDs: []string{deliveryID}}},
				}}}
			admission, err := runfork.RunForkSelectedContractReplayResumeAdmission(planWithReplayAdmission(plan))
			if err != nil {
				t.Fatal(err)
			}
			for _, allowed := range [][]string{nil, {eventID}} {
				if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, admission, allowed); len(blockers) != 0 {
					t.Fatalf("mutation gate discarded exact fan-out admission: %+v", blockers)
				}
			}
			if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, admission, []string{uuid.NewString()}); len(blockers) == 0 {
				t.Fatal("exact fan-out admission bypassed selected-frontier membership")
			}
			if runfork.RunForkPendingWorkReplayableForHistoricalReplay(item) {
				t.Fatal("selected fan-out widened generic node historical replay")
			}
			foreign := item
			foreign.EventID, foreign.DeliveryID = uuid.NewString(), uuid.NewString()
			foreignPlan := plan
			foreignPlan.PendingWork = append([]runfork.RunForkPendingWork{foreign}, plan.PendingWork...)
			foreignAdmission, err := runfork.RunForkSelectedContractReplayResumeAdmission(planWithReplayAdmission(foreignPlan))
			if err != nil {
				t.Fatal(err)
			}
			if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(foreignPlan, foreignAdmission, []string{eventID}); len(blockers) == 0 {
				t.Fatal("unrelated source delivery borrowed fan-out admission")
			}
		})
	}
}

func planWithReplayAdmission(plan runfork.RunForkPlan) runfork.RunForkPlan {
	plan.ReplayResumeAdmission = runForkReplayResumeAdmission(runForkAdmissionEvidence{Pending: plan.PendingWork})
	return plan
}
