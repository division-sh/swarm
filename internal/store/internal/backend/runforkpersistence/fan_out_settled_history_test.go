package runforkpersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestIssue642SettledFanOutHistoryDoesNotRequireSelectedPlan(t *testing.T) {
	plan, selected, _, carriage := a2ForkMapSelectedFixture(t, true)
	obligation := &plan.FanOutObligations[0]
	obligation.Intent.Cursor = obligation.Intent.Request.Cardinality
	obligation.Intent.Status = fanoutobligation.StatusClosed
	for ordinal := 1; ordinal < obligation.Intent.Cursor; ordinal++ {
		obligation.Outcomes = append(obligation.Outcomes, fanoutobligation.Outcome{
			Ordinal: ordinal, Kind: fanoutobligation.OutcomeCommitted, SourceEventID: uuid.NewString(),
			InheritedDisposition: fanoutobligation.InheritedNoRoute, CreatedAt: obligation.Intent.CreatedAt,
		})
	}
	for _, tc := range []struct {
		name     string
		carriage semanticview.FanOutPlanCarriage
	}{
		{"changed", carriage},
		{"removed", semanticview.FanOutPlanCarriage{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refs, err := resolveRunForkFanOutPlanRefs(plan, selected.Ref.BundleHash, tc.carriage)
			if err != nil {
				t.Fatalf("settled history demanded a selected declaration: %v", err)
			}
			for _, ref := range refs {
				if ref != obligation.Intent.Request.PlanRef {
					t.Fatal("settled history acquired a new executable interpretation")
				}
			}
		})
	}
	active := *obligation
	active.Intent.Request.Key.TriggeringDeliveryID = uuid.NewString()
	active.Intent.Status, active.Intent.Cursor = fanoutobligation.StatusOpen, 0
	active.Outcomes = nil
	plan.FanOutObligations = append(plan.FanOutObligations, active)
	refs, err := resolveRunForkFanOutPlanRefs(plan, selected.Ref.BundleHash, carriage)
	if err != nil || refs[plan.FanOutObligations[0].Intent.Request.Key] != plan.FanOutObligations[0].Intent.Request.PlanRef || refs[active.Intent.Request.Key] != selected.Ref || len(refs) != 2 {
		t.Fatalf("settled and active instances of one declaration were collapsed: %v %v", refs, err)
	}
	if _, err := resolveRunForkFanOutPlanRefs(plan, selected.Ref.BundleHash, semanticview.FanOutPlanCarriage{}); err == nil {
		t.Fatal("settled sibling excused the active instance's missing selected declaration")
	}
	plan.FanOutObligations = append(plan.FanOutObligations, active)
	if _, err := runfork.FanOutContinuationRequirements(plan); err == nil {
		t.Fatal("continuation projection collapsed duplicate exact intent facts")
	}
}

func TestIssue642ClosedFanOutRetainsPendingRecipientObligation(t *testing.T) {
	plan, selected, _, _ := a2ForkMapSelectedFixture(t, true)
	obligation := &plan.FanOutObligations[0]
	obligation.Intent.Status, obligation.Intent.Cursor = fanoutobligation.StatusClosed, obligation.Intent.Request.Cardinality
	for ordinal := 1; ordinal < obligation.Intent.Cursor-1; ordinal++ {
		obligation.Outcomes = append(obligation.Outcomes, fanoutobligation.Outcome{Ordinal: ordinal, Kind: fanoutobligation.OutcomeCommitted,
			SourceEventID: uuid.NewString(), InheritedDisposition: fanoutobligation.InheritedNoRoute, CreatedAt: obligation.Intent.CreatedAt})
	}
	event := uuid.NewString()
	obligation.PendingReplays = []runfork.RunForkFanOutPendingReplay{{Ordinal: obligation.Intent.Cursor - 1, SourceEventID: event}}
	plan.PendingWork = []runfork.RunForkPendingWork{{DeliveryID: uuid.NewString(), EventID: event, SubscriberType: "agent", SubscriberID: "worker",
		Status: "pending", Classification: runfork.RunForkPendingClassificationPending}}
	if _, err := resolveRunForkFanOutPlanRefs(plan, selected.Ref.BundleHash, semanticview.FanOutPlanCarriage{}); err == nil {
		t.Fatal("closed issuance erased its pending recipient obligation")
	}
	obligation.PendingReplays = nil
	if _, err := runfork.FanOutContinuationRequirements(plan); err == nil {
		t.Fatal("missing ordinal evidence was mistaken for settled history")
	}
}
