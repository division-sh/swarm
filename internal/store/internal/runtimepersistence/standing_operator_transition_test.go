package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestStandingFreshNoopRejectsCorruptRelationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, command := range []string{"suspend", "resume"} {
				t.Run(command, func(t *testing.T) {
					f := openStandingDispositionParityFixture(t, backend)
					ctx := testAuthorActivityRuntimeContext()
					current := f.create(t, ctx, "corrupt-noop-"+command)
					operation := runtimepipeline.StandingServiceOperation{ServiceID: current.ServiceID}
					call := f.workflow.ResumeStandingService
					if command == "suspend" {
						call = f.workflow.SuspendStandingService
						if _, err := call(ctx, operation); err != nil {
							t.Fatal(err)
						}
					}
					_, before := f.standingRevisionAndJournalCount(t, ctx, current.ServiceID)
					query := `DELETE FROM standing_service_generations WHERE service_id=?`
					if backend == "postgres" {
						query = `DELETE FROM standing_service_generations WHERE service_id=$1::uuid`
					}
					if _, err := f.db.ExecContext(ctx, query, current.ServiceID); err != nil {
						t.Fatal(err)
					}
					result, err := call(ctx, operation)
					if err == nil || !strings.Contains(err.Error(), "exact active generation relations") || result.CommittedMutation != "" {
						t.Fatalf("corrupt fresh %s acknowledged: result=%+v err=%v", command, result, err)
					}
					_, after := f.standingRevisionAndJournalCount(t, ctx, current.ServiceID)
					if after != before {
						t.Fatalf("corrupt no-op wrote journal: %d -> %d", before, after)
					}
				})
			}
		})
	}
}

func TestStandingOperatorRefusesEveryChangedAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, change := range []string{"generation", "source", "suspension", "pause", "terminal"} {
				t.Run(change, func(t *testing.T) {
					for _, command := range []string{"suspend", "resume", "reset"} {
						t.Run(command, func(t *testing.T) {
							f := openStandingDispositionParityFixture(t, backend)
							ctx := testAuthorActivityRuntimeContext()
							candidate := f.candidate("authority-race-" + change + "-" + command)
							observed, err := f.workflow.ReconcileStandingService(ctx, candidate)
							if err != nil {
								t.Fatal(err)
							}
							candidate = changeStandingAuthority(t, ctx, f, candidate, observed, change)
							before, found, err := f.workflow.LoadReconciledStandingService(ctx, candidate)
							if err != nil || !found || observed.SameAuthority(before) {
								t.Fatalf("race did not change exact authority: found=%t current=%+v err=%v", found, before, err)
							}
							revision, journal := f.standingRevisionAndJournalCount(t, ctx, observed.ServiceID)
							call := f.workflow.SuspendStandingService
							if command == "resume" {
								call = f.workflow.ResumeStandingService
							} else if command == "reset" {
								call = f.workflow.ResetStandingService
							}
							result, err := call(ctx, runtimepipeline.StandingServiceOperation{ServiceID: observed.ServiceID, Expected: &observed})
							if err == nil || result.CommittedMutation != "" {
								t.Fatalf("stale %s acknowledged after %s: result=%+v err=%v", command, change, result, err)
							}
							after, found, err := f.workflow.LoadReconciledStandingService(ctx, candidate)
							if err != nil || !found || !reflect.DeepEqual(before, after) {
								t.Fatalf("refusal changed current authority: before=%+v after=%+v found=%t err=%v", before, after, found, err)
							}
							newRevision, newJournal := f.standingRevisionAndJournalCount(t, ctx, observed.ServiceID)
							if revision != newRevision || journal != newJournal {
								t.Fatal("refusal changed the revision or journal")
							}
						})
					}
				})
			}
		})
	}
}

func changeStandingAuthority(t *testing.T, ctx context.Context, f standingDispositionParityFixture, candidate runtimepipeline.StandingServiceCandidate, observed runtimepipeline.StandingServiceReconciliation, change string) runtimepipeline.StandingServiceCandidate {
	t.Helper()
	operation := runtimepipeline.StandingServiceOperation{ServiceID: observed.ServiceID}
	var err error
	switch change {
	case "generation":
		_, err = f.workflow.ResetStandingService(ctx, operation)
	case "source":
		candidate = f.reviseCandidateSource(t, candidate, "changed-authority")
		_, err = f.workflow.ReconcileStandingService(ctx, candidate)
	case "suspension":
		_, err = f.workflow.SuspendStandingService(ctx, operation)
	case "pause":
		controller, ok := f.selected.(interface {
			PauseRunControlOutcome(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error)
		})
		if !ok {
			t.Fatal("selected store has no canonical pause owner")
		}
		outcome, pauseErr := controller.PauseRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{
			RunID: observed.RunID, Reason: "standing-authority-race", ControlledBy: "operator", Now: time.Now().UTC(),
		})
		if pauseErr != nil || !outcome.Acknowledged {
			t.Fatalf("pause race: outcome=%+v err=%v", outcome, pauseErr)
		}
	case "terminal":
		f.terminalize(t, ctx, observed.RunID, runtimerunlifecycle.StateCancelled)
	default:
		t.Fatalf("unclassified authority race %q", change)
	}
	if err != nil {
		t.Fatalf("change %s through canonical writer: %v", change, err)
	}
	return candidate
}

func TestStandingOperatorAcknowledgesFreshNoopBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			candidate := f.candidate("fresh-noop")
			current, err := f.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			for _, command := range []string{"resume", "suspend", "suspend", "resume", "resume"} {
				observed, found, err := f.workflow.LoadReconciledStandingService(ctx, candidate)
				if err != nil || !found {
					t.Fatalf("observe command precondition: found=%t err=%v", found, err)
				}
				_, before := f.standingRevisionAndJournalCount(t, ctx, current.ServiceID)
				operation := runtimepipeline.StandingServiceOperation{ServiceID: current.ServiceID, Expected: &observed}
				call := f.workflow.ResumeStandingService
				noop := observed.RestartDisposition.Executable()
				if command == "suspend" {
					call = f.workflow.SuspendStandingService
					noop = observed.RestartDisposition.Kind == runtimerunlifecycle.StandingRestartSuspended
				}
				result, err := call(ctx, operation)
				if err != nil {
					t.Fatal(err)
				}
				want := runtimerunlifecycle.MutationApplied
				journal := before + 1
				if noop {
					want, journal = runtimerunlifecycle.MutationExactNoop, before
				}
				_, after := f.standingRevisionAndJournalCount(t, ctx, current.ServiceID)
				if result.CommittedMutation != want || result.RunID != current.RunID || result.Generation != current.Generation || after != journal {
					t.Fatalf("%s outcome=%+v journal=%d want=%s journal=%d", command, result, after, want, journal)
				}
			}
		})
	}
}

func TestStandingOperatorRefusesStaleAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, command := range []string{"suspend", "resume", "reset"} {
				t.Run(command, func(t *testing.T) {
					f := openStandingDispositionParityFixture(t, backend)
					ctx := testAuthorActivityRuntimeContext()
					candidate := f.candidate("stale-" + command)
					observed, err := f.workflow.ReconcileStandingService(ctx, candidate)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: observed.ServiceID}); err != nil {
						t.Fatal(err)
					}
					_, before := f.standingRevisionAndJournalCount(t, ctx, observed.ServiceID)
					call := f.workflow.SuspendStandingService
					if command == "resume" {
						call = f.workflow.ResumeStandingService
					} else if command == "reset" {
						call = f.workflow.ResetStandingService
					}
					result, err := call(ctx, runtimepipeline.StandingServiceOperation{ServiceID: observed.ServiceID, Expected: &observed})
					if err == nil || !strings.Contains(err.Error(), "authority changed") || result.CommittedMutation != "" {
						t.Fatalf("stale %s result=%+v err=%v", command, result, err)
					}
					_, after := f.standingRevisionAndJournalCount(t, ctx, observed.ServiceID)
					if after != before {
						t.Fatal("stale command changed the journal")
					}
				})
			}
		})
	}
}
