package runforkexecution

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func selectedFanOutHistoricalWorkFixture(t *testing.T, deployment bool) HistoricalReplayExecutionRequest {
	t.Helper()
	run, event := uuid.NewString(), uuid.NewString()
	plan := runfork.RunForkPlan{SourceRunID: run}
	var owed []string
	for index, name := range []string{"pending", "settled"} {
		route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, ".", name)),
			Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: run})}
		id, err := deliverylifecycle.DeliveryID(event, route)
		if err != nil {
			t.Fatal(err)
		}
		item := runfork.RunForkPendingWork{EventID: event, DeliveryID: id, DeliveryRoute: route,
			SubscriberType: "node", SubscriberID: route.Recipient.ID(), Classification: runfork.RunForkPendingClassificationPending, Status: "pending"}
		if index == 0 {
			owed = append(owed, id)
		} else {
			item.Classification, item.Status = runfork.RunForkPendingClassificationDeadLetter, "dead_letter"
		}
		if deployment {
			item.RoutingSource, err = events.NewDeploymentFeedRoutingSource(".")
			if err != nil {
				t.Fatal(err)
			}
		}
		plan.PendingWork = append(plan.PendingWork, item)
	}
	obligation := runfork.RunForkFanOutObligation{Intent: fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{Key: fanoutobligation.IntentKey{RunID: run, TriggeringDeliveryID: uuid.NewString()}}, Cursor: 1,
	}}
	if deployment {
		obligation.Intent.Request.Key.TriggeringDeliveryID = ""
		obligation.Intent.Request.Key.DeploymentFeedID = uuid.NewString()
		obligation.Intent.Request.Deployment = &fanoutobligation.DeploymentOrigin{}
		obligation.PendingDeployment = []runfork.RunForkDeploymentPendingEvent{{Ordinal: 0, SourceEventID: event,
			SourceDeliveryIDs: owed, Phase: runfork.RunForkDeploymentPendingReceiver, InheritedDisposition: fanoutobligation.InheritedDeadLettered}}
		at := time.Unix(100, 0).UTC()
		plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: event, SubscriberType: "platform",
			SubscriberID: "pipeline", ReceiptOutcome: "success", ReceiptAt: &at})
	} else {
		obligation.PendingReplays = []runfork.RunForkFanOutPendingReplay{{Ordinal: 0, SourceEventID: event,
			SourceDeliveryIDs: owed, InheritedDisposition: fanoutobligation.InheritedDeadLettered}}
	}
	plan.FanOutObligations = []runfork.RunForkFanOutObligation{obligation}
	return HistoricalReplayExecutionRequest{Admission: runfork.RunForkHistoricalReplayExecutionAdmission{SourceRunID: run},
		PendingWork: append([]runfork.RunForkPendingWork(nil), plan.PendingWork...), SourcePlan: &plan}
}

func TestSelectedFanOutHistoricalWorkUsesOnlyExactUnfinishedPartition(t *testing.T) {
	for _, deployment := range []bool{false, true} {
		t.Run(map[bool]string{false: "handler", true: "deployment"}[deployment], func(t *testing.T) {
			req := selectedFanOutHistoricalWorkFixture(t, deployment)
			before := append([]runfork.RunForkPendingWork(nil), req.SourcePlan.PendingWork...)
			work, err := historicalReplayDeliveryEventReplayWork(req)
			if err != nil || len(work) != 1 || work[0].SourceDeliveryID != req.SourcePlan.PendingWork[0].DeliveryID ||
				work[0].SubscriberID != req.SourcePlan.PendingWork[0].SubscriberID || work[0].Classification != runfork.RunForkPendingClassificationPending {
				t.Fatalf("selected unfinished work=%+v err=%v", work, err)
			}
			if !reflect.DeepEqual(before, req.SourcePlan.PendingWork) {
				t.Fatal("collection changed the fixed cut")
			}
			req.SourcePlan = nil
			ordinary, err := historicalReplayDeliveryEventReplayWork(req)
			if err != nil || len(ordinary) != 0 || runfork.RunForkPendingWorkReplayableForHistoricalReplay(req.PendingWork[0]) {
				t.Fatalf("selected work broadened generic node replay: %+v err=%v", ordinary, err)
			}
		})
	}
}

func TestSelectedFanOutHistoricalWorkRejectsMissingOrSubstitutedInput(t *testing.T) {
	for _, deployment := range []bool{false, true} {
		for _, fault := range []string{"source", "missing", "duplicate", "event", "delivery", "subscriber", "classification", "fact_partition"} {
			t.Run(map[bool]string{false: "handler", true: "deployment"}[deployment]+"/"+fault, func(t *testing.T) {
				req := selectedFanOutHistoricalWorkFixture(t, deployment)
				switch fault {
				case "source":
					req.Admission.SourceRunID = uuid.NewString()
				case "missing":
					req.PendingWork = req.PendingWork[1:]
				case "duplicate":
					req.PendingWork = append(req.PendingWork, req.PendingWork[0])
				case "event":
					req.PendingWork[0].EventID = uuid.NewString()
				case "delivery":
					req.PendingWork[0].DeliveryID = uuid.NewString()
				case "subscriber":
					req.PendingWork[0].SubscriberID = "another-node"
				case "classification":
					req.PendingWork[0].Classification = runfork.RunForkPendingClassificationInProgress
				case "fact_partition":
					if deployment {
						req.SourcePlan.FanOutObligations[0].PendingDeployment[0].SourceDeliveryIDs = nil
					} else {
						req.SourcePlan.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs = nil
					}
				}
				if _, err := historicalReplayDeliveryEventReplayWork(req); err == nil {
					t.Fatal("selected collector admitted incomplete or substituted work")
				}
			})
		}
	}
}
