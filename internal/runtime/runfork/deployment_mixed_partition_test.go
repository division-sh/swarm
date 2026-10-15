package runfork

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

// REVIEWER-A's 6e053cb71 counterexample, retained without reducing its partition.
func TestReviewerDeploymentMixedPartitionRetainsTerminalRows(t *testing.T) {
	plan := fanOutPendingPartitionFixture(t)
	original := plan.FanOutObligations[0].PendingReplays[0]
	obligation := &plan.FanOutObligations[0]
	obligation.Intent.Request.Deployment = &fanoutobligation.DeploymentOrigin{}
	obligation.Intent.Request.Key.DeploymentFeedID = uuid.NewString()
	obligation.Intent.Request.Key.TriggeringDeliveryID = ""
	obligation.PendingReplays = nil
	obligation.PendingDeployment = []RunForkDeploymentPendingEvent{{
		Ordinal: original.Ordinal, SourceEventID: original.SourceEventID,
		SourceDeliveryIDs: []string{plan.PendingWork[0].DeliveryID},
		Phase:             RunForkDeploymentPendingReceiver,
	}}
	source, err := events.NewDeploymentFeedRoutingSource(".")
	if err != nil {
		t.Fatal(err)
	}
	pending := plan.PendingWork[0]
	pending.RoutingSource = source
	pending.ClaimVersion, pending.RetryCount = 0, 0
	pending.ActiveSessionID, pending.StartedAt = "", nil
	now := time.Unix(100, 0).UTC()
	terminal := append([]RunForkPendingWork(nil), plan.PendingWork[2:5]...)
	plan.PendingWork = []RunForkPendingWork{pending, {EventID: original.SourceEventID,
		SubscriberType: "platform", SubscriberID: "pipeline", ReceiptOutcome: "success", ReceiptAt: &now}}
	if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
		t.Fatalf("positive pending-only control: %v", err)
	}
	plan.PendingWork = append(plan.PendingWork, terminal...)
	obligation.PendingDeployment[0].InheritedDisposition = fanoutobligation.InheritedDeadLettered
	if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
		t.Fatalf("settled source rows must remain evidence, not owed recipients: %v", err)
	}
}
