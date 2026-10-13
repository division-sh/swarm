package runforkpersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestPublishedActivityCutRetainsOnlyItsUnfinishedPipelineWork(t *testing.T) {
	for _, outcome := range []string{"", "success", "dead_letter"} {
		t.Run("receipt_"+outcome, func(t *testing.T) {
			at := time.Unix(1700000000, 0).UTC()
			snapshot := &runForkRevisionSnapshot{Events: []runForkRevisionEvent{
				{EventID: "activity", EventName: runfork.RunForkSelectedContractPlatformActivityEvent, CreatedAt: at},
				{EventID: "business", EventName: "ordinary.business.event", CreatedAt: at},
			}}
			if outcome != "" {
				snapshot.Receipts = []runForkRevisionReceipt{{EventID: "activity", SubscriberType: "platform", SubscriberID: "pipeline", Outcome: outcome, ProcessedAt: at}}
			}
			pending, err := loadRunForkPendingWorkFromRevision(snapshot)
			if err != nil || len(pending) != 1 || pending[0].EventID != "activity" || pending[0].DeliveryID != "" || pending[0].SubscriberType != "platform" || pending[0].SubscriberID != "pipeline" {
				t.Fatalf("exact platform occurrence was lost, duplicated or invented for a business event: %+v error=%v", pending, err)
			}
			want := runfork.RunForkPendingClassificationPending
			if outcome == "success" {
				want = runfork.RunForkPendingClassificationDeliveredCompleted
			} else if outcome == "dead_letter" {
				want = runfork.RunForkPendingClassificationDeadLetter
			}
			if pending[0].Classification != want || outcome == "" && (!pending[0].CreatedAt.Equal(at) || pending[0].ReceiptAt != nil || pending[0].DeliveredAt != nil) {
				t.Fatalf("platform outcome changed at its publication cut: %+v want=%s", pending[0], want)
			}
		})
	}
}
