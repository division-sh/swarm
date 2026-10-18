package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimestanding "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestStandingOwnedPauseRejectsGenericContinueBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, posture := range []string{"suspended", "reset_preserved", "orphaned", "session_required"} {
				t.Run(posture, func(t *testing.T) {
					fixture := openStandingDispositionParityFixture(t, backend)
					ctx := testAuthorActivityRuntimeContext()
					current := fixture.create(t, ctx, "audit-"+posture)
					operation := runtimepipeline.StandingServiceOperation{ServiceID: current.ServiceID, Actor: "audit"}
					if _, err := fixture.workflow.SuspendStandingService(ctx, operation); err != nil {
						t.Fatalf("suspend: %v", err)
					}
					wantBefore := runtimestanding.StandingRestartSuspended
					switch posture {
					case "reset_preserved":
						reset, err := fixture.workflow.ResetStandingService(ctx, operation)
						if err != nil {
							t.Fatalf("reset preserving suspension: %v", err)
						}
						if reset.Generation != current.Generation+1 || reset.RunID == current.RunID {
							t.Fatalf("reset did not create exact successor: %+v", reset)
						}
						current = reset
					case "orphaned":
						if _, err := fixture.workflow.ReconcileStandingServiceSet(ctx, nil); err != nil {
							t.Fatalf("remove complete declaration set: %v", err)
						}
						wantBefore = runtimestanding.StandingRestartOrphaned
					case "session_required":
						candidate := fixture.candidate("audit-" + posture)
						candidate.BindingEnabled = false
						candidate.BindingBlockReason = runtimestanding.StandingBindingSessionRequired
						if _, err := fixture.workflow.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate}); err != nil {
							t.Fatal(err)
						}
						wantBefore = runtimestanding.StandingRestartSessionDormant
					}
					before := assertStandingDisposition(t, ctx, fixture, current.RunID, wantBefore)
					if before.OperatorOverride != "suspended" || before.RunState != "paused" {
						t.Fatalf("not a retained standing-owned pause: %+v", before)
					}
					controller, ok := fixture.selected.(interface {
						ContinueRunControlOutcome(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error)
					})
					if !ok {
						t.Fatalf("selected store %T lacks run control owner", fixture.selected)
					}
					outcome, err := controller.ContinueRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{
						RunID: current.RunID, ControlledBy: "audit", Reason: "ordinary_continue", Now: time.Now().UTC(),
					})
					if !errors.Is(err, runtimeruncontrol.ErrNotPaused) || outcome.Acknowledged {
						t.Fatalf("continue did not refuse standing authority: outcome=%+v err=%v", outcome, err)
					}
					after := assertStandingDisposition(t, ctx, fixture, current.RunID, wantBefore)
					if !reflect.DeepEqual(before, after) {
						t.Fatalf("standing changed on refused continue: before=%+v after=%+v", before, after)
					}
					if posture == "session_required" {
						for _, mutate := range []func(context.Context, runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error){
							fixture.workflow.ResumeStandingService, fixture.workflow.ResetStandingService,
						} {
							if _, err := mutate(ctx, operation); err == nil {
								t.Fatal("standing controls bypassed native admission")
							}
							if after := assertStandingDisposition(t, ctx, fixture, current.RunID, wantBefore); !reflect.DeepEqual(before, after) {
								t.Fatal("refused standing control changed native dormancy", before, after)
							}
						}
					}
				})
			}
		})
	}
}
