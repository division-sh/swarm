package effectpersistence

import (
	"errors"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestEffectMutationErrorPreservesAcknowledgedPhaseAndCause(t *testing.T) {
	attempt := runtimeeffects.Attempt{OperationID: "operation-id", AttemptID: "attempt-id"}
	cause := errors.New("injected cleanup failure")
	for _, phase := range []runtimeeffects.MutationPhase{
		runtimeeffects.MutationLaunch,
		runtimeeffects.MutationObservation,
		runtimeeffects.MutationSettlement,
		runtimeeffects.MutationHeartbeat,
		runtimeeffects.MutationProjection,
	} {
		t.Run(string(phase), func(t *testing.T) {
			err := effectMutationError(true, cause, phase, attempt)
			var committed *runtimeeffects.PostCommitMutationError
			if !errors.As(err, &committed) || !errors.Is(err, cause) {
				t.Fatalf("acknowledged error = %T %v, want typed cause", err, err)
			}
			if committed.Phase != phase || committed.OperationID != attempt.OperationID || committed.AttemptID != attempt.AttemptID {
				t.Fatalf("acknowledged phase identity = %#v", committed)
			}
			if got := effectMutationError(false, cause, phase, attempt); got != cause {
				t.Fatalf("unacknowledged error = %T %v, want original cause", got, got)
			}
			if got := effectMutationError(true, nil, phase, attempt); got != nil {
				t.Fatalf("clean acknowledged error = %v, want nil", got)
			}
		})
	}
}
