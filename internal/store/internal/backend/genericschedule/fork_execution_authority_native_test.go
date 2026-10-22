package genericschedule_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type forkScheduleExecutionOperations interface {
	PrepareGenericScheduleOccurrence(context.Context, runtimegenericschedule.Wakeup) (runtimegenericschedule.PreparationCommit, error)
	ClaimGenericScheduleWakeup(context.Context, runtimegenericschedule.Wakeup) (bool, error)
	ReleaseGenericScheduleWakeup(context.Context, runtimegenericschedule.Wakeup) error
}

// A hostile selected-mode carrier is not a grant. These are genuine native
// ordinary runs; inherited row provenance cannot lend them selected authority.
func TestNativeScheduleRejectsSelectedAuthorityForOrdinaryRunBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			operations, ok := f.generic.(forkScheduleExecutionOperations)
			if !ok {
				t.Fatal("native schedule execution operations are required")
			}
			for _, action := range []string{"claim", "prepare", "cancel", "admit"} {
				t.Run(action, func(t *testing.T) {
					ctx, request := forkJoinNativeRequest(t, f, ".", false, false)
					hostile := forkScheduleHostileSelectedContext(t, ctx, request.Child.RunID)
					if action == "admit" {
						result, err := f.generic.AdmitGenericScheduleOutcome(hostile, request.Child)
						if err == nil || result.Acknowledged {
							t.Fatalf("selected carrier admitted ordinary work: acknowledged=%t outcome=%s err=%v", result.Acknowledged, result.Result.Outcome, err)
						}
						rows, err := f.generic.ListActiveGenericScheduleActivations(ctx)
						if err != nil {
							t.Fatal(err)
						}
						for _, row := range rows {
							if row.Command.RunID == request.Child.RunID {
								t.Fatal("refused admission leaked a child schedule")
							}
						}
						return
					}
					child := forkJoinNativeRestore(t, f, ctx, request)
					wakeup, err := runtimegenericschedule.NewWakeup(child.ID, child.CurrentDueAt)
					if err != nil {
						t.Fatal(err)
					}
					switch action {
					case "claim":
						claimed, err := operations.ClaimGenericScheduleWakeup(hostile, wakeup)
						if claimed {
							_ = operations.ReleaseGenericScheduleWakeup(context.Background(), wakeup)
							t.Fatal("selected carrier acquired ordinary schedule execution")
						}
						if err != nil {
							t.Fatalf("legitimate scope observation failed instead of declining work: %v", err)
						}
					case "prepare":
						result, err := operations.PrepareGenericScheduleOccurrence(hostile, wakeup)
						if err == nil || result.Acknowledged {
							t.Fatalf("selected carrier prepared ordinary occurrence: acknowledged=%t outcome=%s err=%v", result.Acknowledged, result.Result.Outcome, err)
						}
					case "cancel":
						result, err := f.generic.CancelGenericScheduleOutcome(hostile, runtimegenericschedule.CancelCommand{
							ActivationID: child.ID, Cause: "operator_cancelled",
							CancelledAt: runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC()),
						})
						if err == nil || result.Acknowledged {
							t.Fatalf("selected carrier canceled ordinary work: acknowledged=%t outcome=%s err=%v", result.Acknowledged, result.Result.Outcome, err)
						}
					}
					actual, found, err := f.generic.LoadGenericScheduleActivation(ctx, child.ID)
					if err != nil || !found || !reflect.DeepEqual(child.Canonical(), actual.Canonical()) {
						t.Fatalf("refused execution changed inherited facts: found=%t err=%v", found, err)
					}
					forkJoinOccurrenceNativeUnchanged(t, f, ctx, child)
				})
			}
		})
	}
}

func forkScheduleHostileSelectedContext(t *testing.T, ctx context.Context, runID string) context.Context {
	t.Helper()
	executionID := uuid.NewString()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthoritySelectedContractFork, ID: executionID,
		ExecutionOwner: "hostile-mode-carrier", LeaseExpiresAt: time.Now().UTC().Add(time.Hour),
		FenceGeneration: 1, ExecutionMode: runtimeeffects.ExecutionModeLive,
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{
			ExecutionID: executionID, ForkRunID: runID, Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container",
			ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
		},
	}
	if !authority.Valid() {
		t.Fatal("invalid hostile selected carrier")
	}
	return runtimeeffects.WithAuthority(ctx, authority)
}
