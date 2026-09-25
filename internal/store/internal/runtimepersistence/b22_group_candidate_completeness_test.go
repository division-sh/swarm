package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil/stagecatalogfixture"
)

func TestB22GroupMixedCandidateWakeAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, restart := range []bool{false, true} {
			name := "wake"
			if restart {
				name = "restart"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				const count = 16
				f, _ := newCollectorFixtureWithCardinality(t, backend, count)
				if err := materializeCompletedRunEntityForTest(f.ctx, f.raw, f.runID); err != nil {
					t.Fatal(err)
				}
				// This selected-owner fixture seeds its already-delivered trigger
				// separately from the group. Drain that fixture's one pipeline
				// obligation before counting the exact sixteen-member scenario.
				// Live group claims keep every tested member out of this scan.
				if bootstrap, err := f.bus.ReleaseRunQueue(f.ctx, f.runID, count+2); err != nil || bootstrap.Settled != 1 || !bootstrap.Exhausted {
					t.Fatalf("fixture trigger recovery=%+v err=%v", bootstrap, err)
				}
				interceptor := &b22MixedOutcomes{ordinals: map[string]int{}, visits: map[string]int{}}
				for i, event := range f.events {
					interceptor.ordinals[event.ID()] = i
				}
				f.bus.SetInterceptors(interceptor)
				f.bus.SetRuntimeIngressDispatchGate(&b22PrefixGate{stopAt: count/2 + 1})
				err := f.bus.DispatchFanOutPublications(f.ctx, f.group, f.committed)
				if !errors.Is(err, bus.ErrRuntimeIngressPaused) {
					t.Fatalf("retained incomplete suffix boundary: %v", err)
				}
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				store := f.raw.(runLifecycleCandidateParityStore)
				activityScope, _ := authoractivity.ScopeFromContext(f.ctx)
				scope := runlifecycle.CandidateScope{BundleHash: activityScope.BundleHash}
				catalog := stagecatalogfixture.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}})
				before, err := store.ListCompletionCandidates(f.ctx, scope, runlifecycle.CandidateCursor{}, 128)
				if err != nil || len(before.Candidates) != 1 || before.Candidates[0].RunID != f.runID {
					t.Fatalf("coalesced prefix candidate=%+v err=%v", before, err)
				}
				blocked, err := store.ExecuteCompletionCandidate(f.ctx, before.Candidates[0], catalog)
				if err != nil || !blocked.Committed || blocked.Outcome != runlifecycle.OutcomeAwaitMutation {
					t.Fatalf("pending suffix allowed premature exhaustion: %+v err=%v", blocked, err)
				}
				snapshot, err := store.LoadRunLifecycleSnapshot(f.ctx, f.runID)
				if err != nil || snapshot.Status != string(runlifecycle.StateRunning) || snapshot.EndedAt != nil {
					t.Fatalf("prefix terminalized run: %+v err=%v", snapshot, err)
				}
				blockedCandidates := map[runlifecycle.CandidateIdentity]struct{}{before.Candidates[0].Identity(): {}}
				observeCandidate := func(result pipelineCrashCandidateResult) bool {
					t.Helper()
					t.Logf("actual candidate revision=%d outcome=%s err=%v", result.candidate.Revision, result.outcome.Outcome, result.err)
					if result.err != nil || result.candidate.RunID != f.runID || !result.outcome.Committed {
						t.Fatalf("candidate execution=%+v", result)
					}
					switch result.outcome.Outcome {
					case runlifecycle.OutcomeAwaitMutation:
						blockedCandidates[result.candidate.Identity()] = struct{}{}
						return true
					case runlifecycle.OutcomeExactNoop:
						// A consumed exact identity may be delivered again. It
						// cannot stand in for observing the final blocked revision.
						if _, consumed := blockedCandidates[result.candidate.Identity()]; !consumed {
							t.Fatalf("no-op without an observed blocked identity: %+v", result)
						}
						return false
					default:
						t.Fatalf("mixed failures cannot become successful completion: %+v", result)
					}
					return false
				}
				process := worklifetime.NewProcess()
				work, err := process.NewRuntime(f.ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: "b22-candidate", BundleHash: scope.BundleHash})
				if err != nil {
					t.Fatal(err)
				}
				observed := &pipelineCrashCandidateObserver{CandidateStore: store, results: make(chan pipelineCrashCandidateResult, 32)}
				executor, err := runlifecycle.NewExecutor(observed, scope, catalog, work, runlifecycle.ExecutorOptions{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := executor.Retire(ctx); err != nil {
						t.Error(err)
					}
					if _, err := work.RetireAndWait(ctx); err != nil {
						t.Error(err)
					}
					process.Retire()
					if _, err := process.Join(ctx); err != nil {
						t.Error(err)
					}
				})
				if !restart {
					if err := executor.Start(f.ctx); err != nil {
						t.Fatal(err)
					}
					registration, err := f.raw.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(f.ctx, scope, executor)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(registration.Release)
					// Re-deliver the exact prefix candidate after its native owner
					// cleared due_at. Exercise a real executor/store duplicate,
					// without relying on concurrent suffix notifications to race.
					if err := executor.SubmitCompletionCandidate(f.ctx, before.Candidates[0]); err != nil {
						t.Fatal(err)
					}
					select {
					case duplicate := <-observed.results:
						if !duplicate.candidate.SameIdentity(before.Candidates[0]) || duplicate.outcome.Outcome != runlifecycle.OutcomeExactNoop {
							t.Fatalf("consumed prefix candidate duplicate=%+v", duplicate)
						}
						if observeCandidate(duplicate) {
							t.Fatal("duplicate counted as a new blocked candidate")
						}
					case <-time.After(5 * time.Second):
						t.Fatal("consumed prefix candidate duplicate was not executed")
					}
					unchanged, err := store.LoadRunLifecycleSnapshot(f.ctx, f.runID)
					if err != nil || !reflect.DeepEqual(unchanged, snapshot) {
						t.Fatalf("consumed candidate duplicate changed lifecycle: before=%+v after=%+v err=%v", snapshot, unchanged, err)
					}
				}
				f.bus.SetRuntimeIngressDispatchGate(nil)
				if result, err := f.bus.ReleaseRunQueue(f.ctx, f.runID, count); err != nil || result.Settled != count/2 {
					t.Fatalf("exact canonical suffix recovery=%+v err=%v", result, err)
				}
				if restart {
					// No predecessor sink submitted the final candidate. A new
					// executor must discover the actual durable revision itself.
					if err := executor.Start(f.ctx); err != nil {
						t.Fatal(err)
					}
				}
				var finalRevision int64
				if err := f.db.QueryRowContext(f.ctx, `SELECT completion_revision FROM runs WHERE run_id=$1`, f.runID).Scan(&finalRevision); err != nil {
					t.Fatal(err)
				}
				deadline := time.NewTimer(5 * time.Second)
				defer deadline.Stop()
				finalObserved := false
				for !finalObserved {
					select {
					case result := <-observed.results:
						finalObserved = observeCandidate(result) && result.candidate.Revision == finalRevision
					case <-deadline.C:
						summaries := readCompletionBlockerSummaries(t, runLifecycleCandidateParityFixture{store: store, db: f.db, postgres: f.postgres}, f.ctx, f.runID, time.Now().UTC())
						t.Fatalf("final mixed-member candidate revision%d was lost: %+v", finalRevision, summaries)
					}
				}
				if err := executor.Retire(f.ctx); err != nil {
					t.Fatal(err)
				}
				snapshot, err = store.LoadRunLifecycleSnapshot(f.ctx, f.runID)
				if err != nil || snapshot.Status != string(runlifecycle.StateRunning) || snapshot.EndedAt != nil {
					t.Fatalf("terminal failures were misreported as successful completion=%+v err=%v", snapshot, err)
				}
				for i, event := range f.events {
					if interceptor.visits[event.ID()] != 1 {
						t.Fatalf("member%d visits=%d", i, interceptor.visits[event.ID()])
					}
					want := "success"
					if i%2 == 0 {
						want = "dead_letter"
					}
					var got string
					if err := f.db.QueryRowContext(f.ctx, `SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, event.ID()).Scan(&got); err != nil || got != want {
						t.Fatalf("exact member%d receipt=%s want=%s err=%v", i, got, want, err)
					}
				}
				// Pipeline terminal failures are settled evidence, not successful
				// completion (platform-spec run_summary). Explicit run failure owns
				// terminality; it must atomically absorb the remaining candidate.
				failure, _ := failures.EnvelopeFromError(failures.New(failures.ClassComputeFailure, "b22_member_rejected", "b22", "dispatch", nil))
				terminal, mutation, err := store.MarkTerminalRun(f.ctx, runlifecycle.TerminalRequest{RunID: f.runID, State: runlifecycle.StateFailed, Failure: &failure, EndedAt: time.Now().UTC()})
				if err != nil || mutation != runlifecycle.MutationApplied || terminal.State != runlifecycle.StateFailed || terminal.EndedAt == nil {
					t.Fatalf("exact failure terminality=%+v mutation=%s err=%v", terminal, mutation, err)
				}
				disposition, err := store.RequestCompletionCandidate(f.ctx, runlifecycle.ImmediateCandidate(f.runID))
				if err != nil || disposition != runlifecycle.CandidateAbsorbedTerminal {
					t.Fatalf("terminal candidate was rearmed: %s err=%v", disposition, err)
				}
			})
		}
	}
}

type b22PrefixGate struct{ calls, stopAt int }

func (g *b22PrefixGate) QueueableIngressPaused(context.Context) (bool, error) {
	g.calls++
	if g.calls == g.stopAt {
		return false, bus.ErrRuntimeIngressPaused
	}
	return false, nil
}

type b22MixedOutcomes struct{ ordinals, visits map[string]int }

func (p *b22MixedOutcomes) Intercept(_ context.Context, event events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	ordinal, exists := p.ordinals[event.ID()]
	if !exists {
		return false, nil, pipelineobligation.Continue(), errors.New("unexpected candidate-proof event")
	}
	p.visits[event.ID()]++
	if ordinal%2 == 0 {
		failure, _ := failures.EnvelopeFromError(failures.New(failures.ClassComputeFailure, "b22_member_rejected", "b22", "dispatch", nil))
		return false, nil, pipelineobligation.DeadLetterExecution("b22_member_rejected", &failure), nil
	}
	return true, nil, pipelineobligation.Continue(), nil
}
