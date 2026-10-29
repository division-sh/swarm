package runtimepersistence

import (
	"errors"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestExternalEffectSettlementPostCommitErrorIsTypedOnBothStores(t *testing.T) {
	forEachTerminalEffectBackend(t, func(t *testing.T, fixture neutralEffectParityFixture) {
		injected := errors.New("injected cleanup failure after native settlement acknowledgement")
		owner := &managedProjectionPostCommitFaultStore{completionSettlementTestStore: fixture.store.(completionSettlementTestStore), settlementErr: injected}

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
		err := owner.SettleExternalAttempt(fixture.ctx, settlement)
		var committed *runtimeeffects.PostCommitMutationError
		if !errors.As(err, &committed) || !errors.Is(err, injected) {
			t.Fatalf("acknowledged settlement error = %T %v, want typed injected fault", err, err)
		}
		if committed.Phase != runtimeeffects.MutationSettlement || committed.OperationID != attempt.OperationID || committed.AttemptID != attempt.AttemptID {
			t.Fatalf("postcommit settlement identity = %#v", committed)
		}
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, attempt.AttemptID, runtimeeffects.StateTerminalFailure)
		requireExternalOperationState(t, fixture.db, fixture.sqlite, attempt.OperationID, runtimeeffects.StateTerminalFailure)

		settlement.State = runtimeeffects.StateOutcomeUncertain
		err = owner.SettleExternalAttempt(fixture.ctx, settlement)
		if err == nil || errors.As(err, &committed) {
			t.Fatalf("unacknowledged conflicting settlement = %T %v, want ordinary error", err, err)
		}
	})
}
