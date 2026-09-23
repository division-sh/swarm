package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type failingEffectCandidateSink struct {
	err     error
	submits int
}

func (s *failingEffectCandidateSink) ReserveCompletionCandidate(context.Context) (runtimerunlifecycle.CandidateAdmission, error) {
	return s, nil
}

func (s *failingEffectCandidateSink) Submit(runtimerunlifecycle.Candidate) error {
	s.submits++
	return s.err
}

func (*failingEffectCandidateSink) Cancel() error { return nil }

func TestExternalEffectSettlementPostCommitErrorIsTypedOnBothStores(t *testing.T) {
	forEachTerminalEffectBackend(t, func(t *testing.T, fixture neutralEffectParityFixture) {
		registrar, ok := fixture.store.(interface {
			RegisterCompletionCandidateSink(context.Context, runtimerunlifecycle.CandidateScope, runtimerunlifecycle.CandidateSink) (runtimerunlifecycle.CandidateRegistration, error)
		})
		if !ok {
			t.Fatalf("selected store %T lacks candidate registration", fixture.store)
		}
		runID := fixture.authority.Normal.Identity.RunID
		query := `SELECT bundle_hash FROM runs WHERE run_id=?`
		if !fixture.sqlite {
			query = `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`
		}
		var bundleHash string
		if err := fixture.db.QueryRow(query, runID).Scan(&bundleHash); err != nil {
			t.Fatalf("load candidate bundle: %v", err)
		}
		injected := errors.New("injected postcommit candidate handoff failure")
		sink := &failingEffectCandidateSink{err: injected}
		registration, err := registrar.RegisterCompletionCandidateSink(testAuthorActivityContext(), runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, sink)
		if err != nil {
			t.Fatalf("register completion sink: %v", err)
		}
		defer registration.Release()

		handle := beginNeutralRecoveryAttempt(t, fixture, "authored_http_tool", "postcommit-settlement", false, false)
		attempt := handle.Attempt()
		failureErr := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "effect_test_prelaunch_failure", "external-effects", "settle_attempt", nil)
		failure, ok := runtimefailures.EnvelopeFromError(failureErr)
		if !ok {
			t.Fatal("construct settlement failure")
		}
		settlement := runtimeeffects.Settlement{
			OperationID: attempt.OperationID, AttemptID: attempt.AttemptID, Authority: attempt.Authority,
			State: runtimeeffects.StateTerminalFailure, Failure: &failure, Now: time.Now().UTC(),
		}
		err = fixture.store.SettleExternalAttempt(testAuthorActivityContext(), settlement)
		var committed *runtimeeffects.PostCommitMutationError
		if !errors.As(err, &committed) || !errors.Is(err, injected) {
			t.Fatalf("acknowledged settlement error = %T %v, want typed injected fault", err, err)
		}
		if committed.Phase != runtimeeffects.MutationSettlement || committed.OperationID != attempt.OperationID || committed.AttemptID != attempt.AttemptID {
			t.Fatalf("postcommit settlement identity = %#v", committed)
		}
		if sink.submits != 1 {
			t.Fatalf("candidate submissions = %d, want 1", sink.submits)
		}
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, attempt.AttemptID, runtimeeffects.StateTerminalFailure)
		requireExternalOperationState(t, fixture.db, fixture.sqlite, attempt.OperationID, runtimeeffects.StateTerminalFailure)

		settlement.State = runtimeeffects.StateOutcomeUncertain
		err = fixture.store.SettleExternalAttempt(testAuthorActivityContext(), settlement)
		if err == nil || errors.As(err, &committed) {
			t.Fatalf("unacknowledged conflicting settlement = %T %v, want ordinary error", err, err)
		}
		if sink.submits != 1 {
			t.Fatalf("conflict resubmitted candidate: %d", sink.submits)
		}
	})
}
