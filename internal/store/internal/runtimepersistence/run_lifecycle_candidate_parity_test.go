package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const runLifecycleCandidateParityBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
const runLifecycleCandidateParityReplacementHash = "bundle-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

type runLifecycleCandidateParityStore interface {
	runtimerunlifecycle.CandidateStore
	runtimerunlifecycle.OperationOwner
	LoadRunLifecycleSnapshot(context.Context, string) (runtimebus.RunLifecycleSnapshot, error)
}

type runLifecycleCandidateParityFixture struct {
	store    runLifecycleCandidateParityStore
	db       *sql.DB
	postgres bool
}

func TestRunLifecycleTerminalAbsorbsCandidateParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			for _, state := range []runtimerunlifecycle.State{
				runtimerunlifecycle.StateCompleted,
				runtimerunlifecycle.StateFailed,
				runtimerunlifecycle.StateCancelled,
			} {
				state := state
				t.Run(string(state), func(t *testing.T) {
					runID := uuid.NewString()
					startedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
					ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)

					first := requestRunLifecycleCandidateParity(t, fixture, ctx, runID)
					if first.Disposition != runtimerunlifecycle.CandidateRequested {
						t.Fatalf("post-create candidate disposition = %s", first.Disposition)
					}
					second := requestRunLifecycleCandidateParity(t, fixture, ctx, runID)
					if second.Disposition != runtimerunlifecycle.CandidateAlreadyCurrent ||
						second.Candidate.Revision != first.Candidate.Revision ||
						!second.Candidate.DueAt.Equal(first.Candidate.DueAt) {
						t.Fatalf("duplicate candidate = %#v, want exact current %#v", second, first)
					}

					snapshot, disposition, err := terminalizeRunLifecycleCandidateParity(
						fixture, ctx, runID, state, startedAt.Add(time.Minute),
					)
					if err != nil {
						t.Fatalf("terminalize %s: %v", state, err)
					}
					if disposition != runtimerunlifecycle.MutationApplied || snapshot.State != state {
						t.Fatalf("terminal mutation = %s/%s", disposition, snapshot.State)
					}
					gotState, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID)
					if gotState != string(state) || duePresent || revision != first.Candidate.Revision {
						t.Fatalf("terminal candidate facts = state:%s due:%v revision:%d", gotState, duePresent, revision)
					}

					absorbed := requestRunLifecycleCandidateParity(t, fixture, ctx, runID)
					if absorbed.Disposition != runtimerunlifecycle.CandidateAbsorbedTerminal {
						t.Fatalf("terminal request disposition = %s", absorbed.Disposition)
					}
				})
			}
		})
	}
}

func TestRunLifecycleTimestampAdmissionParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			startedAt := time.Date(2026, 7, 29, 12, 0, 0, 987654321, time.FixedZone("offset", -4*60*60))
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)

			created, err := fixture.store.LoadRunLifecycleSnapshot(ctx, runID)
			if err != nil {
				t.Fatalf("load created lifecycle: %v", err)
			}
			wantStartedAt := runtimerunlifecycle.CanonicalTimestamp(startedAt)
			if !created.StartedAt.Equal(wantStartedAt) {
				t.Fatalf("started_at = %s, want %s", created.StartedAt, wantStartedAt)
			}

			endedAt := startedAt.Add(time.Minute + 789*time.Nanosecond)
			terminal, _, err := terminalizeRunLifecycleCandidateParity(
				fixture, ctx, runID, runtimerunlifecycle.StateCancelled, endedAt,
			)
			if err != nil {
				t.Fatalf("terminalize lifecycle: %v", err)
			}
			wantEndedAt := runtimerunlifecycle.CanonicalTimestamp(endedAt)
			if terminal.EndedAt == nil || !terminal.EndedAt.Equal(wantEndedAt) {
				t.Fatalf("ended_at = %v, want %s", terminal.EndedAt, wantEndedAt)
			}
		})
	}
}

func TestRunLifecycleCandidateTimestampPrecisionParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			if store, ok := fixture.store.(*SQLiteRuntimeStore); ok {
				store.nowFn = func() time.Time {
					return time.Date(2026, 7, 29, 12, 0, 0, 987654321, time.UTC)
				}
			}

			immediateRunID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(
				t, fixture, ctx, immediateRunID,
				time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC),
			)
			immediate := requestRunLifecycleCandidateParity(t, fixture, ctx, immediateRunID)
			if immediate.Candidate.DueAt.Nanosecond()%1000 != 0 {
				t.Fatalf("immediate due_at is noncanonical: %s", immediate.Candidate.DueAt)
			}
			listedImmediate := loadRunLifecycleCandidate(t, fixture, ctx, immediateRunID)
			if listedImmediate.Revision != immediate.Candidate.Revision ||
				!listedImmediate.DueAt.Equal(immediate.Candidate.DueAt) {
				t.Fatalf("listed immediate candidate = %#v, want %#v", listedImmediate, immediate.Candidate)
			}
			duplicateImmediate := requestRunLifecycleCandidateParity(t, fixture, ctx, immediateRunID)
			if duplicateImmediate.Disposition != runtimerunlifecycle.CandidateAlreadyCurrent ||
				duplicateImmediate.Candidate.Revision != immediate.Candidate.Revision ||
				!duplicateImmediate.Candidate.DueAt.Equal(immediate.Candidate.DueAt) {
				t.Fatalf("duplicate immediate candidate = %#v, want %#v", duplicateImmediate, immediate)
			}
			immediateResult, err := fixture.store.ExecuteCompletionCandidate(
				ctx,
				duplicateImmediate.Candidate,
				runtimerunlifecycle.NewTerminalCatalog(
					nil,
					map[string][]string{semanticRunFixtureFlow: {"completed"}},
				),
			)
			if err != nil {
				t.Fatalf("execute immediate candidate: %v", err)
			}
			if immediateResult.Outcome != runtimerunlifecycle.OutcomeAwaitMutation {
				t.Fatalf("immediate candidate outcome = %s", immediateResult.Outcome)
			}

			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(
				t, fixture, ctx, runID,
				time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC),
			)

			rawDueAt := time.Date(2030, 1, 1, 12, 0, 0, 123456789, time.UTC)
			wantDueAt := runtimerunlifecycle.CanonicalTimestamp(rawDueAt)
			first := forceRunLifecycleCandidateParity(t, fixture, ctx, runID, rawDueAt)
			if !first.DueAt.Equal(wantDueAt) || first.DueAt.Nanosecond()%1000 != 0 {
				t.Fatalf("persisted due_at = %s, want canonical %s", first.DueAt, wantDueAt)
			}
			listed := loadRunLifecycleCandidate(t, fixture, ctx, runID)
			if listed.Revision != first.Revision || !listed.DueAt.Equal(wantDueAt) {
				t.Fatalf("listed candidate = %#v, want %#v", listed, first)
			}

			duplicate, err := fixture.store.RequestCompletionCandidate(
				ctx,
				runtimerunlifecycle.CandidateAtTime(runID, rawDueAt),
			)
			if err != nil {
				t.Fatalf("request exact candidate duplicate: %v", err)
			}
			if duplicate != runtimerunlifecycle.CandidateAlreadyCurrent {
				t.Fatalf("exact candidate duplicate disposition = %s", duplicate)
			}
			unchanged := loadRunLifecycleCandidate(t, fixture, ctx, runID)
			if unchanged.Revision != first.Revision || !unchanged.DueAt.Equal(wantDueAt) {
				t.Fatalf("exact candidate duplicate churned = %#v", unchanged)
			}
			mismatchedDue := unchanged
			mismatchedDue.DueAt = mismatchedDue.DueAt.Add(time.Microsecond)
			mismatchedResult, err := fixture.store.ExecuteCompletionCandidate(
				ctx, mismatchedDue, runtimerunlifecycle.TerminalCatalog{},
			)
			if err != nil {
				t.Fatalf("execute same-revision candidate with mismatched due coordinate: %v", err)
			}
			if mismatchedResult.Outcome != runtimerunlifecycle.OutcomeExactNoop {
				t.Fatalf("mismatched due coordinate outcome = %s, want exact_noop", mismatchedResult.Outcome)
			}
			afterMismatch := loadRunLifecycleCandidate(t, fixture, ctx, runID)
			if afterMismatch.Revision != first.Revision || !afterMismatch.DueAt.Equal(wantDueAt) {
				t.Fatalf("mismatched due callback mutated candidate = %#v, want %#v", afterMismatch, first)
			}
			noncanonicalRequest := runtimerunlifecycle.CandidateRequest{
				RunID: runID, Timing: runtimerunlifecycle.CandidateAt, DueAt: rawDueAt,
			}
			if _, err := fixture.store.RequestCompletionCandidate(ctx, noncanonicalRequest); err == nil {
				t.Fatal("noncanonical scheduled candidate request was accepted")
			}
			afterRejected := loadRunLifecycleCandidate(t, fixture, ctx, runID)
			if afterRejected.Revision != first.Revision || !afterRejected.DueAt.Equal(wantDueAt) {
				t.Fatalf("rejected noncanonical request churned candidate = %#v", afterRejected)
			}

			rearm, err := fixture.store.ExecuteCompletionCandidate(
				ctx, unchanged, runtimerunlifecycle.TerminalCatalog{},
			)
			if err != nil {
				t.Fatalf("execute future candidate: %v", err)
			}
			if rearm.Outcome != runtimerunlifecycle.OutcomeRearmAt ||
				!rearm.Candidate.DueAt.Equal(wantDueAt) {
				t.Fatalf("future candidate result = %#v", rearm)
			}

			noncanonical := unchanged
			noncanonical.DueAt = noncanonical.DueAt.Add(time.Nanosecond)
			if err := noncanonical.Validate(); err == nil {
				t.Fatal("noncanonical candidate timestamp passed input validation")
			}
		})
	}
}

func TestRunLifecycleCompletionRetriesBeforeRunStartParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			startedAt := time.Now().UTC().Add(time.Hour)
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)
			candidate := requestRunLifecycleCandidateParity(t, fixture, ctx, runID).Candidate

			result, err := fixture.store.ExecuteCompletionCandidate(
				ctx,
				candidate,
				runtimerunlifecycle.TerminalCatalog{},
			)
			if err != nil {
				t.Fatalf("execute future-start completion candidate: %v", err)
			}
			var beforeStart *runtimerunlifecycle.SelectedStoreBeforeRunStartError
			if result.Outcome != runtimerunlifecycle.OutcomeRetryCurrent ||
				!errors.As(result.Retryable, &beforeStart) ||
				beforeStart.RunID != runID ||
				!beforeStart.StartedAt.Equal(runtimerunlifecycle.CanonicalTimestamp(startedAt)) {
				t.Fatalf("future-start completion result = %#v", result)
			}
			snapshot, err := fixture.store.LoadRunLifecycleSnapshot(ctx, runID)
			if err != nil {
				t.Fatalf("load future-start lifecycle: %v", err)
			}
			if snapshot.Status != string(runtimerunlifecycle.StateRunning) || snapshot.EndedAt != nil {
				t.Fatalf("future-start lifecycle mutated = %#v", snapshot)
			}
			state, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID)
			if state != string(runtimerunlifecycle.StateRunning) || !duePresent || revision != candidate.Revision {
				t.Fatalf("future-start candidate facts = state:%s due:%v revision:%d", state, duePresent, revision)
			}
		})
	}
}

func TestRunLifecycleTerminalCandidateDuplicateConflictAndRollbackParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			startedAt := time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)

			t.Run("exact_duplicate_and_conflict", func(t *testing.T) {
				runID := uuid.NewString()
				ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)
				request := requestRunLifecycleCandidateParity(t, fixture, ctx, runID)
				first, disposition, err := terminalizeRunLifecycleCandidateParity(
					fixture, ctx, runID, runtimerunlifecycle.StateCancelled, startedAt.Add(time.Minute),
				)
				if err != nil || disposition != runtimerunlifecycle.MutationApplied {
					t.Fatalf("first cancellation = %#v/%s/%v", first, disposition, err)
				}
				repeated, disposition, err := terminalizeRunLifecycleCandidateParity(
					fixture, ctx, runID, runtimerunlifecycle.StateCancelled, startedAt.Add(2*time.Minute),
				)
				if err != nil || disposition != runtimerunlifecycle.MutationExactNoop ||
					repeated.EndedAt == nil || first.EndedAt == nil || !repeated.EndedAt.Equal(*first.EndedAt) {
					t.Fatalf("exact duplicate = %#v/%s/%v, first=%#v", repeated, disposition, err, first)
				}
				if _, _, err := terminalizeRunLifecycleCandidateParity(
					fixture, ctx, runID, runtimerunlifecycle.StateFailed, startedAt.Add(3*time.Minute),
				); err == nil {
					t.Fatal("conflicting terminal transition succeeded")
				}
				state, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID)
				if state != "cancelled" || duePresent || revision != request.Candidate.Revision {
					t.Fatalf("post-conflict facts = state:%s due:%v revision:%d", state, duePresent, revision)
				}
			})

			t.Run("rollback_preserves_candidate", func(t *testing.T) {
				runID := uuid.NewString()
				ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)
				request := requestRunLifecycleCandidateParity(t, fixture, ctx, runID)
				injected := errors.New("injected terminal side-effect failure")
				err := runLifecycleCandidateRollback(
					fixture, ctx, runID, runtimerunlifecycle.StateCancelled, startedAt.Add(time.Minute), injected,
				)
				if !errors.Is(err, injected) {
					t.Fatalf("rollback error = %v", err)
				}
				state, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID)
				if state != "running" || !duePresent || revision != request.Candidate.Revision {
					t.Fatalf("rolled-back facts = state:%s due:%v revision:%d", state, duePresent, revision)
				}
			})
		})
	}
}

func TestRunLifecycleCandidateTerminalRaceParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			startedAt := time.Date(2026, 7, 29, 13, 30, 0, 0, time.UTC)

			t.Run("current_revision", func(t *testing.T) {
				runID := uuid.NewString()
				ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)
				current := requestRunLifecycleCandidateParity(t, fixture, ctx, runID).Candidate
				if _, _, err := terminalizeRunLifecycleCandidateParity(
					fixture, ctx, runID, runtimerunlifecycle.StateCancelled, startedAt.Add(time.Minute),
				); err != nil {
					t.Fatalf("terminalize current candidate: %v", err)
				}
				result, err := fixture.store.ExecuteCompletionCandidate(
					ctx, current, runtimerunlifecycle.TerminalCatalog{},
				)
				if err != nil || result.Outcome != runtimerunlifecycle.OutcomeExactNoop {
					t.Fatalf("current callback after terminalization = %#v/%v", result, err)
				}
				state, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID)
				if state != string(runtimerunlifecycle.StateCancelled) || duePresent ||
					revision != current.Revision {
					t.Fatalf("current callback race facts = state:%s due:%v revision:%d", state, duePresent, revision)
				}
			})

			t.Run("stale_revision", func(t *testing.T) {
				runID := uuid.NewString()
				ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)
				stale := requestRunLifecycleCandidateParity(t, fixture, ctx, runID).Candidate
				current := forceRunLifecycleCandidateParity(t, fixture, ctx, runID, startedAt.Add(time.Hour))
				if current.Revision <= stale.Revision {
					t.Fatalf("forced candidate revision = %d, stale = %d", current.Revision, stale.Revision)
				}
				result, err := fixture.store.ExecuteCompletionCandidate(
					ctx, stale, runtimerunlifecycle.TerminalCatalog{},
				)
				if err != nil || result.Outcome != runtimerunlifecycle.OutcomeExactNoop {
					t.Fatalf("stale callback = %#v/%v", result, err)
				}
				state, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID)
				if state != string(runtimerunlifecycle.StateRunning) || !duePresent ||
					revision != current.Revision {
					t.Fatalf("stale callback changed current candidate = state:%s due:%v revision:%d", state, duePresent, revision)
				}
				if _, _, err := terminalizeRunLifecycleCandidateParity(
					fixture, ctx, runID, runtimerunlifecycle.StateCancelled, startedAt.Add(2*time.Hour),
				); err != nil {
					t.Fatalf("terminalize newer candidate: %v", err)
				}
				result, err = fixture.store.ExecuteCompletionCandidate(
					ctx, current, runtimerunlifecycle.TerminalCatalog{},
				)
				if err != nil || result.Outcome != runtimerunlifecycle.OutcomeExactNoop {
					t.Fatalf("newer callback after terminalization = %#v/%v", result, err)
				}
			})
		})
	}
}

func TestRunLifecycleTerminalAbsorbsCandidatePostgres(t *testing.T) {
	fixture := openRunLifecycleCandidateParityFixture(t, "postgres")
	ctx := testAuthorActivityBundleSourceContext()
	startedAt := time.Date(2026, 7, 29, 13, 45, 0, 0, time.UTC)
	sourceRunID := uuid.NewString()
	childRunID := uuid.NewString()
	ensureRunLifecycleCandidateParityRun(t, fixture, ctx, sourceRunID, startedAt)
	ensureRunLifecycleCandidateParityRun(t, fixture, ctx, childRunID, startedAt.Add(time.Second))
	sourceCandidate := requestRunLifecycleCandidateParity(t, fixture, ctx, sourceRunID).Candidate

	var (
		snapshot    runtimerunlifecycle.Snapshot
		disposition runtimerunlifecycle.MutationDisposition
	)
	var err error
	snapshot, disposition, err = fixture.store.ForkRunSource(
		ctx,
		runtimerunlifecycle.ForkSourceRequest{
			RunID: sourceRunID, ContinuedAsRunID: childRunID,
			EndedAt: startedAt.Add(time.Minute),
		},
	)
	if err != nil {
		t.Fatalf("fork source lifecycle transition: %v", err)
	}
	if disposition != runtimerunlifecycle.MutationApplied ||
		snapshot.State != runtimerunlifecycle.StateForked ||
		snapshot.ContinuedAsRunID != childRunID {
		t.Fatalf("fork source transition = %#v/%s", snapshot, disposition)
	}
	state, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, sourceRunID)
	if state != string(runtimerunlifecycle.StateForked) || duePresent ||
		revision != sourceCandidate.Revision {
		t.Fatalf("forked source candidate facts = state:%s due:%v revision:%d", state, duePresent, revision)
	}
	absorbed := requestRunLifecycleCandidateParity(t, fixture, ctx, sourceRunID)
	if absorbed.Disposition != runtimerunlifecycle.CandidateAbsorbedTerminal {
		t.Fatalf("forked source candidate request = %s", absorbed.Disposition)
	}
}

func TestRunLifecycleCreateIsInsertOrExactNoopParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			source, err := runtimecorrelation.NewEphemeralBundleSourceFact(runLifecycleCandidateParityBundleHash)
			if err != nil {
				t.Fatal(err)
			}
			at := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)

			t.Run("exact_duplicate", func(t *testing.T) {
				origin, err := runtimerunlifecycle.EventRunOrigin(uuid.NewString(), "test.started")
				if err != nil {
					t.Fatal(err)
				}
				request := runtimerunlifecycle.CreateRequest{
					RunID: uuid.NewString(), Origin: origin,
					Source: source, StartedAt: at,
				}
				if got, err := createRunLifecycleParity(fixture, ctx, request); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("first create = %s/%v", got, err)
				}
				if _, found := findRunLifecycleCandidate(t, fixture, ctx, runLifecycleCandidateParityBundleHash, request.RunID); found {
					t.Fatal("ordinary run creation unexpectedly seeded a completion candidate")
				}
				request.StartedAt = at.Add(time.Hour)
				if got, err := createRunLifecycleParity(fixture, ctx, request); err != nil || got != runtimerunlifecycle.MutationExactNoop {
					t.Fatalf("exact duplicate create = %s/%v", got, err)
				}
				if _, found := findRunLifecycleCandidate(t, fixture, ctx, runLifecycleCandidateParityBundleHash, request.RunID); found {
					t.Fatal("exact duplicate ordinary run creation unexpectedly seeded a completion candidate")
				}
			})

			t.Run("trigger_conflict_is_not_backfilled", func(t *testing.T) {
				runID := uuid.NewString()
				request := runtimerunlifecycle.CreateRequest{
					RunID: runID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(),
					Source: source, StartedAt: at,
				}
				if got, err := createRunLifecycleParity(fixture, ctx, request); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("first create = %s/%v", got, err)
				}
				request.Origin, err = runtimerunlifecycle.EventRunOrigin(uuid.NewString(), "test.late_trigger")
				if err != nil {
					t.Fatal(err)
				}
				if got, err := createRunLifecycleParity(fixture, ctx, request); err == nil || got != "" {
					t.Fatalf("conflicting create = %s/%v", got, err)
				}
				kind, triggerID, triggerType := loadRunLifecycleOriginFacts(t, fixture, ctx, runID)
				if kind != string(runtimerunlifecycle.OriginScenarioSetup) || triggerID != "" || triggerType != "" {
					t.Fatalf("conflicting create rewrote origin = %s/%s/%s", kind, triggerID, triggerType)
				}
			})
		})
	}
}

func TestRunLifecycleEligibilityOriginParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			at := time.Date(2026, 7, 29, 15, 0, 0, 0, time.UTC)

			t.Run("operator_continue_and_duplicate_resume", func(t *testing.T) {
				runID := uuid.NewString()
				ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, at)
				if got, err := transitionRunLifecycleParity(
					fixture, ctx, runID, runtimerunlifecycle.StatePaused,
				); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("pause = %s/%v", got, err)
				}
				if _, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID); duePresent || revision != 0 {
					t.Fatalf("paused candidate = due:%v revision:%d", duePresent, revision)
				}
				if got, err := transitionRunLifecycleParity(
					fixture, ctx, runID, runtimerunlifecycle.StateRunning,
				); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("continue = %s/%v", got, err)
				}
				if _, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID); !duePresent || revision != 1 {
					t.Fatalf("continued candidate = due:%v revision:%d", duePresent, revision)
				}
				if got, err := transitionRunLifecycleParity(
					fixture, ctx, runID, runtimerunlifecycle.StateRunning,
				); err != nil || got != runtimerunlifecycle.MutationExactNoop {
					t.Fatalf("duplicate continue = %s/%v", got, err)
				}
				if _, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID); !duePresent || revision != 1 {
					t.Fatalf("duplicate continue churned candidate = due:%v revision:%d", duePresent, revision)
				}
			})

			t.Run("paused_request_is_deferred_until_resume", func(t *testing.T) {
				runID := uuid.NewString()
				ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, at)
				if got, err := transitionRunLifecycleParity(
					fixture, ctx, runID, runtimerunlifecycle.StatePaused,
				); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("pause = %s/%v", got, err)
				}
				disposition, err := fixture.store.RequestCompletionCandidate(
					ctx,
					runtimerunlifecycle.ImmediateCandidate(runID),
				)
				if err != nil {
					t.Fatalf("request paused candidate: %v", err)
				}
				if disposition != runtimerunlifecycle.CandidateDeferredPaused {
					t.Fatalf("paused candidate disposition = %s", disposition)
				}
				if _, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID); duePresent || revision != 0 {
					t.Fatalf("paused candidate facts = due:%v revision:%d", duePresent, revision)
				}
				if _, found := findRunLifecycleCandidate(
					t,
					fixture,
					ctx,
					runLifecycleCandidateParityBundleHash,
					runID,
				); found {
					t.Fatal("paused candidate was exposed for execution")
				}

				if got, err := transitionRunLifecycleParity(
					fixture, ctx, runID, runtimerunlifecycle.StateRunning,
				); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("resume = %s/%v", got, err)
				}
				if _, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID); !duePresent || revision != 1 {
					t.Fatalf("resumed candidate facts = due:%v revision:%d", duePresent, revision)
				}
			})

			t.Run("fresh_schema_rejects_paused_candidate", func(t *testing.T) {
				runID := uuid.NewString()
				ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, at)
				if got, err := transitionRunLifecycleParity(
					fixture, ctx, runID, runtimerunlifecycle.StatePaused,
				); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("pause = %s/%v", got, err)
				}
				query := `UPDATE runs SET completion_revision = 1, completion_due_at = ? WHERE run_id = ?`
				if fixture.postgres {
					query = `UPDATE runs SET completion_revision = 1, completion_due_at = $1 WHERE run_id = $2::uuid`
				}
				if _, err := fixture.db.ExecContext(ctx, query, at, runID); err == nil {
					t.Fatal("fresh schema accepted a completion candidate on a paused run")
				}
			})

			t.Run("source_revision_and_exact_duplicate", func(t *testing.T) {
				runID := uuid.NewString()
				ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, at)
				stale := requestRunLifecycleCandidateParity(t, fixture, ctx, runID).Candidate
				replacement, err := runtimecorrelation.NewEphemeralBundleSourceFact(runLifecycleCandidateParityReplacementHash)
				if err != nil {
					t.Fatal(err)
				}
				if got, err := reviseRunLifecycleSourceParity(fixture, ctx, runID, replacement); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("revise source = %s/%v", got, err)
				}
				if _, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID); !duePresent || revision != 1 {
					t.Fatalf("source revision candidate = due:%v revision:%d", duePresent, revision)
				}
				if candidate, found := findRunLifecycleCandidate(
					t, fixture, ctx, runLifecycleCandidateParityBundleHash, runID,
				); found {
					t.Fatalf("source revision retained stale-scope candidate %#v", candidate)
				}
				current, found := findRunLifecycleCandidate(
					t, fixture, ctx, runLifecycleCandidateParityReplacementHash, runID,
				)
				if !found || current.BundleHash != runLifecycleCandidateParityReplacementHash ||
					current.Revision != stale.Revision {
					t.Fatalf("source revision current candidate = %#v found=%v", current, found)
				}
				result, err := fixture.store.ExecuteCompletionCandidate(
					ctx, stale, runtimerunlifecycle.TerminalCatalog{},
				)
				if err != nil || result.Outcome != runtimerunlifecycle.OutcomeExactNoop {
					t.Fatalf("previous-source callback = %#v/%v", result, err)
				}
				if got, err := reviseRunLifecycleSourceParity(fixture, ctx, runID, replacement); err != nil || got != runtimerunlifecycle.MutationExactNoop {
					t.Fatalf("duplicate source revision = %s/%v", got, err)
				}
				if _, duePresent, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID); !duePresent || revision != 1 {
					t.Fatalf("duplicate source revision churned candidate = due:%v revision:%d", duePresent, revision)
				}
			})
		})
	}
}

func openRunLifecycleCandidateParityFixture(t *testing.T, backend string) runLifecycleCandidateParityFixture {
	t.Helper()
	if backend == "sqlite" {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		store.nowFn = func() time.Time {
			return time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
		}
		return runLifecycleCandidateParityFixture{store: store, db: store.backend.ConstructionHandle()}
	}
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	return runLifecycleCandidateParityFixture{store: store, db: db, postgres: true}
}

func ensureRunLifecycleCandidateParityRun(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	source, err := runtimecorrelation.NewEphemeralBundleSourceFact(runLifecycleCandidateParityBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, source)
	if _, ok := runtimeauthoractivity.ScopeFromContext(ctx); !ok {
		ctx = runtimeauthoractivity.WithScope(
			ctx,
			runtimeauthoractivity.BundleScope(uuid.NewString(), runLifecycleCandidateParityBundleHash),
		)
	}
	if _, err := fixture.store.CreateRun(ctx, runtimerunlifecycle.CreateRequest{
		RunID: runID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(),
		Source: source, StartedAt: startedAt,
	}); err != nil {
		t.Fatalf("ensure run %s: %v", runID, err)
	}
}

func createRunLifecycleParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	return fixture.store.CreateRun(ctx, request)
}

func transitionRunLifecycleParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	state runtimerunlifecycle.State,
) (runtimerunlifecycle.MutationDisposition, error) {
	return fixture.store.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: runID, State: state,
	})
}

func reviseRunLifecycleSourceParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	source runtimecorrelation.BundleSourceFact,
) (runtimerunlifecycle.MutationDisposition, error) {
	return fixture.store.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{
		RunID: runID, Source: source,
	})
}

func requestRunLifecycleCandidateParity(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
) runtimerunlifecycle.CandidateRequestResult {
	t.Helper()
	disposition, err := fixture.store.RequestCompletionCandidate(
		ctx,
		runtimerunlifecycle.ImmediateCandidate(runID),
	)
	if err != nil {
		t.Fatalf("request completion candidate: %v", err)
	}
	if disposition == runtimerunlifecycle.CandidateAbsorbedTerminal {
		return runtimerunlifecycle.CandidateRequestResult{Disposition: disposition}
	}
	candidate := loadRunLifecycleCandidate(t, fixture, ctx, runID)
	return runtimerunlifecycle.CandidateRequestResult{Disposition: disposition, Candidate: candidate}
}

func forceRunLifecycleCandidateParity(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	dueAt time.Time,
) runtimerunlifecycle.Candidate {
	t.Helper()
	var result runtimerunlifecycle.CandidateRequestResult
	var err error
	switch store := fixture.store.(type) {
	case *PostgresStore:
		err = store.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
			result, err = requestPostgresCompletionCandidateTx(txctx, tx, runID, &dueAt, true)
			return err
		})
	case *SQLiteRuntimeStore:
		err = store.runPrivateAuthorActivityMutation(ctx, "test force sqlite completion candidate revision", func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
			result, err = requestSQLiteCompletionCandidateTx(txctx, tx, runID, &dueAt, store.now(), true)
			return err
		})
	default:
		err = errors.New("unsupported run lifecycle candidate parity store")
	}
	if err != nil {
		t.Fatalf("force completion candidate revision: %v", err)
	}
	if result.Disposition != runtimerunlifecycle.CandidateRequested {
		t.Fatalf("forced completion candidate disposition = %s", result.Disposition)
	}
	return result.Candidate
}

func loadRunLifecycleCandidate(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
) runtimerunlifecycle.Candidate {
	t.Helper()
	candidate, found := findRunLifecycleCandidate(
		t, fixture, ctx, runLifecycleCandidateParityBundleHash, runID,
	)
	if found {
		return candidate
	}
	t.Fatalf("candidate %s is not durable", runID)
	return runtimerunlifecycle.Candidate{}
}

func findRunLifecycleCandidate(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	bundleHash string,
	runID string,
) (runtimerunlifecycle.Candidate, bool) {
	t.Helper()
	page, err := fixture.store.ListCompletionCandidates(
		ctx,
		runtimerunlifecycle.CandidateScope{BundleHash: bundleHash},
		runtimerunlifecycle.CandidateCursor{},
		128,
	)
	if err != nil {
		t.Fatalf("list completion candidates: %v", err)
	}
	for _, candidate := range page.Candidates {
		if candidate.RunID == runID {
			return candidate, true
		}
	}
	return runtimerunlifecycle.Candidate{}, false
}

func terminalizeRunLifecycleCandidateParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	state runtimerunlifecycle.State,
	endedAt time.Time,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if state == runtimerunlifecycle.StateCompleted {
		return completeRunLifecycleCandidateParity(fixture, ctx, runID, endedAt)
	}
	failure := (*runtimefailures.Envelope)(nil)
	if state == runtimerunlifecycle.StateFailed {
		failure = testRetryableFailure()
	}
	return fixture.store.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{
		RunID: runID, State: state, Failure: failure, EndedAt: endedAt,
	})
}

func completeRunLifecycleCandidateParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	endedAt time.Time,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	var snapshot runtimerunlifecycle.Snapshot
	var disposition runtimerunlifecycle.MutationDisposition
	var inner error
	switch store := fixture.store.(type) {
	case *PostgresStore:
		err := store.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			snapshot, disposition, inner = store.runLifecyclePostgresOwner.CompleteRunTx(txctx, tx, story, privaterunforkrevision.NewEffects(), runID, endedAt)
			return inner
		})
		return snapshot, disposition, err
	case *SQLiteRuntimeStore:
		err := store.runPrivateAuthorActivityMutation(ctx, "test successful completion", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			snapshot, disposition, inner = store.runLifecycleSQLiteOwner.CompleteRunTx(txctx, tx, story, privaterunforkrevision.NewEffects(), runID, endedAt)
			return inner
		})
		return snapshot, disposition, err
	default:
		return snapshot, disposition, errors.New("unsupported run lifecycle candidate parity store")
	}
}

func runLifecycleCandidateRollback(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	state runtimerunlifecycle.State,
	endedAt time.Time,
	injected error,
) error {
	switch store := fixture.store.(type) {
	case *PostgresStore:
		return store.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			if _, _, err := store.runLifecyclePostgresOwner.MarkRunTerminalStateTx(txctx, tx, story, privaterunforkrevision.NewEffects(), terminalRunMutation{RunID: runID, State: state, EndedAt: endedAt}); err != nil {
				return err
			}
			return injected
		})
	case *SQLiteRuntimeStore:
		return store.runPrivateAuthorActivityMutation(ctx, "test terminal lifecycle rollback", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			if _, _, err := store.runLifecycleSQLiteOwner.MarkRunTerminalStateTx(txctx, tx, story, privaterunforkrevision.NewEffects(), terminalRunMutation{RunID: runID, State: state, EndedAt: endedAt}); err != nil {
				return err
			}
			return injected
		})
	default:
		return errors.New("unsupported run lifecycle candidate parity store")
	}
}

func loadRunLifecycleCandidateFacts(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
) (string, bool, int64) {
	t.Helper()
	query := `SELECT status, completion_due_at, completion_revision FROM runs WHERE run_id = ?`
	if fixture.postgres {
		query = `SELECT status, completion_due_at, completion_revision FROM runs WHERE run_id = $1::uuid`
	}
	var state string
	var revision int64
	if fixture.postgres {
		var dueAt sql.NullTime
		if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&state, &dueAt, &revision); err != nil {
			t.Fatalf("load run lifecycle candidate facts: %v", err)
		}
		return state, dueAt.Valid, revision
	}
	var dueAt any
	if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&state, &dueAt, &revision); err != nil {
		t.Fatalf("load run lifecycle candidate facts: %v", err)
	}
	_, present, err := sqliteTimeValue(dueAt)
	if err != nil {
		t.Fatalf("decode run lifecycle candidate due_at: %v", err)
	}
	return state, present, revision
}

func loadRunLifecycleOriginFacts(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
) (string, string, string) {
	t.Helper()
	query := `SELECT origin_kind, COALESCE(trigger_event_id, ''), COALESCE(trigger_event_type, '') FROM runs WHERE run_id = ?`
	if fixture.postgres {
		query = `SELECT origin_kind, COALESCE(trigger_event_id::text, ''), COALESCE(trigger_event_type, '') FROM runs WHERE run_id = $1::uuid`
	}
	var kind, triggerID, triggerType string
	if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&kind, &triggerID, &triggerType); err != nil {
		t.Fatalf("load run lifecycle origin facts: %v", err)
	}
	return kind, triggerID, triggerType
}
