package runtimepersistence_test

import (
	"errors"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// This exercises the existing fence/cleanup protocol, not the complete idle
// work census. No physical resources are installed by this store fixture.
func TestIssue2269FencedAttemptCannotAdmitSuccessorBeforeCleanupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, ordering := range []string{"admission_wins", "fence_wins"} {
			t.Run(backend+"/"+ordering, func(t *testing.T) {
				f := newDynamicFlowCreationAtomicityFixture(t, backend)
				before, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
				if err != nil || !found || before.AttemptState != "accepted" {
					t.Fatalf("initial attempt: found=%v row=%+v err=%v", found, before, err)
				}
				if ordering == "admission_wins" {
					if err := f.selected.VerifyDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err != nil {
						t.Fatalf("admission before fencing: %v", err)
					}
				}
				// Plan replacement supplies the existing durable fence. The new idle
				// operation must use this disposition without acknowledging cleanup.
				desired := before.Plan
				desired.WorkflowVersion += "-resource-fence"
				if _, err := f.selected.ReconcileDynamicFlowRuntimeReadinessPlans(f.ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: before, Expected: desired}}, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				fenced, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
				if err != nil || !found || fenced.AttemptState != "superseded" || fenced.AttemptOrdinal != before.AttemptOrdinal {
					t.Fatalf("fence falsely acknowledged cleanup: found=%v row=%+v err=%v", found, fenced, err)
				}
				if err := f.selected.VerifyDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err == nil {
					t.Fatal("fenced predecessor retained forward authority")
				}
				request := runtimepipeline.NewDynamicFlowRuntimeActivationRequest(fenced.Plan, fenced.AttemptOrdinal, fenced.AttemptState, f.attempt.ProcessBinding())
				if admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, request); err == nil || admitted.Acknowledged {
					t.Fatalf("successor bypassed exact predecessor cleanup: %+v %v", admitted, err)
				}
				if err := f.selected.RetireDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err != nil {
					t.Fatal(err)
				}
				if admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, request); !errors.Is(err, runtimepipeline.ErrDynamicFlowRuntimeReadinessObservationStale) || admitted.Acknowledged {
					t.Fatalf("pre-cleanup observation admitted successor: %+v %v", admitted, err)
				}
				settled, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
				if err != nil || !found || settled.AttemptState != "retired" {
					t.Fatalf("cleanup acknowledgement: found=%v row=%+v err=%v", found, settled, err)
				}
				admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(settled.Plan, settled.AttemptOrdinal, settled.AttemptState, f.attempt.ProcessBinding()))
				if err != nil || !admitted.Acknowledged || admitted.Attempt.Ordinal() != f.attempt.Ordinal()+1 {
					t.Fatalf("successor after exact cleanup: %+v %v", admitted, err)
				}
			})
		}
	}
}
