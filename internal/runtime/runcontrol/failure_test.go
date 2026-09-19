package runcontrol

import (
	"errors"
	"strings"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestStopFailurePreservesDiagnosticAndTypedCause(t *testing.T) {
	cause := errors.New("restore the standing declaration, then use its service control")
	err := StopFailure("standing_admission", cause)
	failure, typed := runtimefailures.EnvelopeFromError(err)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), cause.Error()) || !typed || failure.Detail.Attributes["stage"] != "standing_admission" {
		t.Fatalf("stop erased original diagnostic or typed cause: %v %+v", err, failure)
	}
	if again := StopFailure("transition", err); again != err {
		t.Fatalf("outer classification replaced narrower envelope: %v", again)
	}
}
