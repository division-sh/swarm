package conformance

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func assertDeploymentForkPendingCandidate(t *testing.T, f *deploymentResourceFixture, request runfork.RunForkPlanRequest, eventID, deliveryID string, phase runfork.RunForkDeploymentPendingPhase) runfork.RunForkPoint {
	t.Helper()
	planner, ok := f.selected.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	})
	if !ok {
		t.Fatalf("selected store %T has no canonical fork planner", f.selected)
	}
	plan, err := planner.PlanRunFork(f.ctx, request)
	if err != nil {
		t.Fatalf("plan exact deployment pending cut: %v", err)
	}
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		t.Fatalf("planned deployment pending evidence is invalid: %v", err)
	}
	matches := 0
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment == nil {
			continue
		}
		if len(obligation.PendingReplays) != 0 {
			t.Fatalf("deployment work borrowed generic replay: %+v", obligation.PendingReplays)
		}
		for _, candidate := range obligation.PendingDeployment {
			if candidate.SourceEventID != eventID {
				continue
			}
			matches++
			if candidate.Phase != phase || len(candidate.SourceDeliveryIDs) != 1 || candidate.SourceDeliveryIDs[0] != deliveryID {
				t.Fatalf("deployment candidate does not match fixed cut: %+v", candidate)
			}
		}
	}
	if matches != 1 {
		t.Fatalf("fixed cut produced %d deployment candidates for event %s", matches, eventID)
	}
	return plan.ForkPoint
}
