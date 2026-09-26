package runfork

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestDeploymentPendingCandidateRequiresExactFixedDeliveryAndHandoff(t *testing.T) {
	declaration := durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"}
	version := durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("a", 64))
	request := fanoutobligation.IntentRequest{
		Key: fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()},
		Deployment: &fanoutobligation.DeploymentOrigin{
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("b", 64), Declaration: declaration,
			VersionID: version, SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("c", 64)),
		},
		Source:      fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: declaration, VersionID: version},
		Cardinality: 1,
	}
	now := time.Now().UTC()
	intent := fanoutobligation.Intent{Request: request, Source: request.Source, Cursor: 1, Status: fanoutobligation.StatusClosed,
		NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	if err := intent.Validate(); err != nil {
		t.Fatal(err)
	}
	source, err := events.NewDeploymentFeedRoutingSource(".")
	if err != nil {
		t.Fatal(err)
	}
	eventID, deliveryID := uuid.NewString(), uuid.NewString()
	base := func(phase RunForkDeploymentPendingPhase) RunForkPlan {
		pending := RunForkPendingWork{EventID: eventID, DeliveryID: deliveryID, SubscriberType: "node", SubscriberID: "receiver",
			RoutingSource: source, Classification: RunForkPendingClassificationPending, Status: "pending"}
		if phase == RunForkDeploymentPendingReceiver {
			pending.ContinuationHandoffAt = &now
		}
		plan := RunForkPlan{PendingWork: []RunForkPendingWork{pending}, FanOutObligations: []RunForkFanOutObligation{{
			Intent: intent, PendingDeployment: []RunForkDeploymentPendingEvent{{Ordinal: 0, SourceEventID: eventID,
				SourceDeliveryIDs: []string{deliveryID}, Phase: phase}},
		}}}
		if phase == RunForkDeploymentPendingReceiver {
			plan.PendingWork = append(plan.PendingWork, RunForkPendingWork{EventID: eventID, SubscriberType: "platform",
				SubscriberID: "pipeline", ReceiptOutcome: "success", ReceiptAt: &now})
		}
		return plan
	}
	for _, phase := range []RunForkDeploymentPendingPhase{RunForkDeploymentPendingPublication, RunForkDeploymentPendingReceiver} {
		if err := ValidateFanOutPendingReplayAdmission(base(phase)); err != nil {
			t.Fatalf("exact %s candidate rejected: %v", phase, err)
		}
	}
	for _, tc := range []struct {
		name   string
		phase  RunForkDeploymentPendingPhase
		mutate func(*RunForkPlan)
	}{
		{"wrong_phase", RunForkDeploymentPendingPublication, func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingDeployment[0].Phase = RunForkDeploymentPendingReceiver
		}},
		{"missing_receipt", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.PendingWork = p.PendingWork[:1] }},
		{"missing_handoff", RunForkDeploymentPendingReceiver, func(p *RunForkPlan) { p.PendingWork[0].ContinuationHandoffAt = nil }},
		{"foreign_delivery", RunForkDeploymentPendingPublication, func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingDeployment[0].SourceDeliveryIDs[0] = uuid.NewString()
		}},
		{"omitted_sibling", RunForkDeploymentPendingPublication, func(p *RunForkPlan) {
			sibling := p.PendingWork[0]
			sibling.DeliveryID = uuid.NewString()
			p.PendingWork = append(p.PendingWork, sibling)
		}},
		{"generic_replay", RunForkDeploymentPendingPublication, func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays = []RunForkFanOutPendingReplay{{Ordinal: 0, SourceEventID: eventID}}
		}},
		{"two_feeds_one_event", RunForkDeploymentPendingPublication, func(p *RunForkPlan) {
			other := p.FanOutObligations[0]
			other.Intent.Request.Key.DeploymentFeedID = uuid.NewString()
			p.FanOutObligations = append(p.FanOutObligations, other)
		}},
		{"wrong_source", RunForkDeploymentPendingPublication, func(p *RunForkPlan) { p.PendingWork[0].RoutingSource = events.NoRoutingSource() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := base(tc.phase)
			tc.mutate(&plan)
			if err := ValidateFanOutPendingReplayAdmission(plan); err == nil {
				t.Fatal("contradictory deployment pending candidate was admitted")
			}
		})
	}
}
