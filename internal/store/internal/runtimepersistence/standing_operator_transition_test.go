package runtimepersistence

import (
	"strings"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
