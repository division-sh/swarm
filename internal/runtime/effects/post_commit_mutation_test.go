package effects

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAuthorizedHandleFailsClosedAfterAcknowledgedCleanupError(t *testing.T) {
	cleanup := errors.New("database secret detail xyz")
	attempt := Attempt{OperationID: "operation", AttemptID: "attempt", AuthorizationAcknowledged: true}
	handle, err := authorizedHandle(&Controller{}, attempt, cleanup)
	if handle != nil {
		t.Fatal("acknowledged authorization cleanup error must not grant a launch handle")
	}
	var committed *PostCommitMutationError
	if !errors.As(err, &committed) || !errors.Is(err, cleanup) || !CommittedMutationPhase(err, MutationAuthorization, attempt) {
		t.Fatalf("committed authorization identity/diagnostic lost: %v", err)
	}
	if committed.AttemptID != attempt.AttemptID || committed.OperationID != attempt.OperationID {
		t.Fatalf("committed authorization identity = %+v", committed)
	}
	if strings.Contains(err.Error(), cleanup.Error()) {
		t.Fatal("public post-commit error exposed backend cleanup detail")
	}

	attempt.AuthorizationAcknowledged = false
	handle, err = authorizedHandle(&Controller{}, attempt, cleanup)
	if handle != nil || err != cleanup || CommittedMutationPhase(err, MutationAuthorization, attempt) {
		t.Fatalf("unacknowledged authorization masqueraded as committed: handle=%v err=%v", handle, err)
	}
}

func TestEffectHandlePreservesCommittedPhaseErrors(t *testing.T) {
	cleanup := errors.New("cleanup failed")
	attempt := Attempt{OperationID: "operation", AttemptID: "attempt", AuthorizationAcknowledged: true}
	probe := &effectStoreProbe{
		launchErr:  NewPostCommitMutationError(MutationLaunch, attempt, cleanup),
		observeErr: NewPostCommitMutationError(MutationObservation, attempt, cleanup),
		settleErr:  NewPostCommitMutationError(MutationSettlement, attempt, cleanup),
	}
	handle := &Handle{controller: NewController(probe), attempt: attempt}
	for _, tc := range []struct {
		phase MutationPhase
		call  func() error
	}{
		{MutationLaunch, func() error { return handle.MarkLaunched(context.Background()) }},
		{MutationObservation, func() error {
			return handle.MarkResponseObserved(context.Background(), map[string]any{"received": true})
		}},
		{MutationSettlement, func() error { return handle.Settle(context.Background(), StateSettled, nil, nil) }},
	} {
		err := tc.call()
		var committed *PostCommitMutationError
		if !CommittedMutationPhase(err, tc.phase, attempt) || !errors.As(err, &committed) ||
			committed.AttemptID != attempt.AttemptID || !errors.Is(err, cleanup) {
			t.Fatalf("%s did not retain committed phase and diagnostic: %v", tc.phase, err)
		}
	}
	if probe.launches != 1 {
		t.Fatalf("launch marker attempted %d times", probe.launches)
	}

	probe.launchErr = cleanup
	if err := handle.MarkLaunched(context.Background()); CommittedMutationPhase(err, MutationLaunch, attempt) {
		t.Fatalf("unacknowledged launch error claimed commit: %v", err)
	}
	joined := errors.Join(
		NewPostCommitMutationError(MutationLaunch, attempt, cleanup),
		NewPostCommitMutationError(MutationObservation, attempt, cleanup),
	)
	if CommittedMutationPhase(joined, MutationLaunch, attempt) || CommittedMutationPhase(joined, MutationObservation, attempt) ||
		CommittedMutationPhase(errors.Join(probe.launchErr, context.Canceled), MutationLaunch, attempt) {
		t.Fatalf("joined failure was mistaken for isolated committed continuation authority: %v", joined)
	}
	foreign := attempt
	foreign.AttemptID = "foreign"
	if CommittedMutationPhase(probe.launchErr, MutationLaunch, foreign) {
		t.Fatal("foreign attempt inherited committed launch authority")
	}
}
