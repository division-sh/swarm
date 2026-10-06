package runforkpersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestCanceledDeliveryHistoryRetainsAuthoredDispositionWithoutReplay(t *testing.T) {
	classification := classifyRunForkDeliverySnapshot(deliverylifecycle.Snapshot{Status: deliverylifecycle.StatusCanceled}, false)
	if classification != runfork.RunForkPendingClassificationCanceled {
		t.Fatalf("canceled source history was reinterpreted: %s", classification)
	}
	item := runfork.RunForkPendingWork{Classification: classification, Status: "canceled", SubscriberType: "agent", ReasonCode: "turn_timeout"}
	disposition := runForkReplayResumeDispositionForPendingWork(item)
	if disposition.Fact != runfork.RunForkReplayResumeFactDeliveryCanceledHistory ||
		disposition.Disposition != runfork.RunForkReplayResumeDispositionLineageOnly || runfork.RunForkPendingWorkReplayableForHistoricalReplay(item) {
		t.Fatalf("canceled work became executable: %+v", disposition)
	}
}
