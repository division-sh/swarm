package runcontrol

import (
	"fmt"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

// StopFailure preserves a narrower owner's failure and its original cause.
// Stages are chosen at the operation boundary, never from database error text.
func StopFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	if _, typed := runtimefailures.EnvelopeFromError(err); typed {
		return err
	}
	failure := runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "run_stop_failed", "runtime.run_control", "stop", map[string]any{"stage": stage}, err)
	return fmt.Errorf("%v: %w", err, failure)
}
