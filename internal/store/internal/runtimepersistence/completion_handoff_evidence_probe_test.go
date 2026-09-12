package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
)

type completionHandoffEvidenceProbeSink struct {
	submit func(runtimerunlifecycle.Candidate) error
}

func (s *completionHandoffEvidenceProbeSink) ReserveCompletionCandidate(context.Context) (runtimerunlifecycle.CandidateAdmission, error) {
	return s, nil
}
func (s *completionHandoffEvidenceProbeSink) Submit(c runtimerunlifecycle.Candidate) error {
	return s.submit(c)
}
func (*completionHandoffEvidenceProbeSink) Cancel() error { return nil }

// Injected sink failure after real durable settlement, not SIGKILL or a lost
// COMMIT response. Recovery uses the real lifecycle startup executor.
func TestCompletionCommittedEvidenceAfterHandoffFailureProbe(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fail := range []bool{false, true} {
			phase := "healthy"
			if fail {
				phase = "sink_failure"
			}
			t.Run(backend+"/"+phase, func(t *testing.T) {
				var fixture completionSettlementFixture
				if backend == "sqlite" {
					s := newBootstrappedSQLiteRuntimeStoreForTest(t)
					fixture = newCompletionSettlementFixture(t, s, s.backend.ConstructionHandle(), true)
				} else {
					_, db, _ := testutil.StartPostgres(t)
					fixture = newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false)
				}
				ctx := fixture.context
				handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "handoff-evidence-probe")
				attempt := handle.Attempt()
				query := "SELECT bundle_hash FROM runs WHERE run_id=?"
				stateQuery := "SELECT state FROM runtime_external_effect_attempts WHERE attempt_id=?"
				if !fixture.sqlite {
					query = "SELECT bundle_hash FROM runs WHERE run_id=$1::uuid"
					stateQuery = "SELECT state FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid"
				}
				var bundleHash string
				if err := fixture.db.QueryRow(query, fixture.authority.Target.RunID).Scan(&bundleHash); err != nil {
					t.Fatal(err)
				}
				injected := errors.New("injected postcommit completion sink submission failure")
				submits := 0
				durableAtSubmit := ""
				sink := &completionHandoffEvidenceProbeSink{submit: func(c runtimerunlifecycle.Candidate) error {
					submits++
					if c.RunID != fixture.authority.Target.RunID {
						t.Errorf("foreign candidate: %+v", c)
					}
					if err := fixture.db.QueryRow(stateQuery, attempt.AttemptID).Scan(&durableAtSubmit); err != nil {
						return err
					}
					if durableAtSubmit != string(runtimeeffects.StateSettled) {
						t.Errorf("sink entered before durable settlement: %s", durableAtSubmit)
					}
					if fail {
						return injected
					}
					return nil
				}}
				registrar := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
				registration, err := registrar.RegisterCompletionCandidateSink(ctx, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				settlement := completionSettlementForTest(t, attempt.Authority.Target, fixture, "claude_cli", "", "")
				settlement.ProviderHead = nil
				result, err := handle.SettleCompletion(ctx, settlement)
				if fail && !errors.Is(err, injected) {
					t.Fatalf("lost sink error: %v", err)
				}
				if !fail && err != nil {
					t.Fatalf("healthy settlement: %v", err)
				}
				if submits != 1 {
					t.Fatalf("sink submissions=%d want=1", submits)
				}
				requireCompletionSettlementRows(t, fixture, attempt.AttemptID, attempt.Authority.Target.ID, runtimeeffects.StateSettled, 1, 0)
				reader := fixture.store.(runtimerunlifecycle.CandidateStore)
				page, readErr := reader.ListCompletionCandidates(ctx, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, runtimerunlifecycle.CandidateCursor{}, 100)
				if readErr != nil {
					t.Fatal(readErr)
				}
				found := false
				for _, candidate := range page.Candidates {
					if candidate.RunID == fixture.authority.Target.RunID {
						found = true
					}
				}
				if !found {
					t.Error("canonical recovery reader lost committed run candidate")
				}
				t.Logf("backend=%s injected=%t submits=%d durable_at_submit=%s turn_rows=1 spend_rows=1 reservations=0 recovery_candidate=%t returned_committed=%t error=%v", backend, fail, submits, durableAtSubmit, found, result.Committed, err)
				if !result.Committed {
					t.Error("F2: acknowledged settlement COMMIT erased by postcommit handoff error")
				}
				if fail && result.Committed {
					registration.Release()
					process := worklifetime.NewProcess()
					occurrence := newRunLifecycleExecutorOccurrence(t, process)
					t.Cleanup(func() {
						retireRunLifecycleExecutorOccurrence(t, occurrence)
						retireRunLifecycleProcess(t, process)
					})
					observed := &pipelineCrashCandidateObserver{CandidateStore: reader, results: make(chan pipelineCrashCandidateResult, 8)}
					executor, err := runtimerunlifecycle.NewExecutor(observed, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash},
						runtimerunlifecycle.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}}), occurrence, runtimerunlifecycle.ExecutorOptions{})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := executor.Retire(context.Background()); err != nil {
							t.Error(err)
						}
					})
					if err := executor.Start(ctx); err != nil {
						t.Fatal(err)
					}
					select {
					case executed := <-observed.results:
						if executed.err != nil || executed.candidate.RunID != attempt.Authority.Target.RunID || executed.outcome.Outcome != runtimerunlifecycle.OutcomeRearmAt {
							t.Fatalf("durable candidate recovery: %#v, %v", executed, executed.err)
						}
						// The live origin lease is still present. Actual continuation
						// execution must durably rearm, not fabricate run completion.
						pending := pipelineCrashCandidate(t, ctx, reader, attempt.Authority.Target.RunID)
						if !pending.SameIdentity(executed.outcome.Candidate) || pending.Revision <= executed.candidate.Revision {
							t.Fatalf("recovered candidate did not durably rearm: %#v", pending)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("real executor did not consume missing live handoff")
					}
					if err := executor.Retire(ctx); err != nil {
						t.Fatal(err)
					}
					if observed.executions.Load() != 1 {
						t.Fatalf("continuation executions=%d", observed.executions.Load())
					}
					requireCompletionSettlementRows(t, fixture, attempt.AttemptID, attempt.Authority.Target.ID, runtimeeffects.StateSettled, 1, 0)
				}
			})
		}
	}
}
