package runfork

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestHistoricalRefusalPredicatesPreserveSourceCutDecisions(t *testing.T) {
	classes := map[string]string{
		"pending": RunForkPendingClassificationPending, "in_progress": RunForkPendingClassificationInProgress,
		"failed": RunForkPendingClassificationFailedRetryable, "delivered": RunForkPendingClassificationDeliveredCompleted,
		"dead_letter": RunForkPendingClassificationDeadLetter, "canceled": RunForkPendingClassificationCanceled,
	}
	checked := 0
	for status, classification := range classes {
		for mask := 0; mask < 32; mask++ {
			started, bound := mask&1 != 0, mask&2 != 0
			delivered, receipted, outcome := mask&4 != 0, mask&8 != 0, mask&16 != 0
			if status == "in_progress" && !started || bound && status != "in_progress" {
				continue
			}
			for _, identity := range []string{"exact", "different_source", "different_agent", "non_agent", "missing_delivery"} {
				plan := RunForkPlan{ForkPoint: RunForkPoint{SourceEventID: "source", ProducedByType: "agent", ProducedBy: "worker"}}
				item := RunForkPendingWork{
					EventID: "source", DeliveryID: "delivery", SubscriberType: "agent", SubscriberID: "worker",
					Status: status, Classification: classification,
					SourceExecution: deliverylifecycle.SourceExecutionAtCut{InProgress: status == "in_progress", Started: started, SessionBound: bound},
				}
				at := time.Unix(1, 0).UTC()
				if delivered {
					item.DeliveredAt = &at
				}
				if receipted {
					item.ReceiptAt = &at
				}
				if outcome {
					item.ReceiptOutcome = "processed"
				}
				switch identity {
				case "different_source":
					plan.ForkPoint.SourceEventID = "other"
				case "different_agent":
					plan.ForkPoint.ProducedBy = "other"
				case "non_agent":
					item.SubscriberType = "node"
				case "missing_delivery":
					item.DeliveryID = ""
				}
				// These are the pre-cutover predicates, with physical presence
				// represented directly rather than invented session IDs/start times.
				wantCoupling := status == "in_progress" || status == "pending" && (bound || started || delivered || receipted || outcome)
				wantEmitter := status == "in_progress" && identity == "exact" && started && !bound && !delivered && !receipted && !outcome
				wantReplay := status == "pending" && item.DeliveryID != "" && item.SubscriberType == "agent" && !bound && !started && !delivered && !receipted
				if got := PendingWorkHasActiveDeliverySessionCoupling(item); got != wantCoupling {
					t.Fatalf("coupling changed: status=%s mask=%d identity=%s got=%t want=%t", status, mask, identity, got, wantCoupling)
				}
				if got := runForkSelectedContractSameSourceDeliveryForkPointEmission(plan, item); got != wantEmitter {
					t.Fatalf("emitter exception changed: status=%s mask=%d identity=%s got=%t want=%t", status, mask, identity, got, wantEmitter)
				}
				if got := RunForkPendingWorkReplayableForHistoricalReplay(item); got != wantReplay {
					t.Fatalf("replay changed: status=%s mask=%d identity=%s got=%t want=%t", status, mask, identity, got, wantReplay)
				}
				checked++
			}
		}
	}
	t.Logf("compared old/new decisions for %d cut/identity combinations", checked)
}
