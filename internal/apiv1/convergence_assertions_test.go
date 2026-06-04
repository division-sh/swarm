package apiv1

import (
	"testing"
	"time"
)

const (
	apiv1ConvergenceTimeout      = 5 * time.Second
	apiv1ConvergencePollInterval = 25 * time.Millisecond
)

func requireAPIV1Convergence(t *testing.T, description string, check func() (bool, error)) {
	t.Helper()
	timeout := time.NewTimer(apiv1ConvergenceTimeout)
	defer timeout.Stop()
	poll := time.NewTicker(apiv1ConvergencePollInterval)
	defer poll.Stop()

	var lastErr error
	for {
		ok, err := check()
		if ok {
			return
		}
		if err != nil {
			lastErr = err
		}

		select {
		case <-timeout.C:
			if lastErr != nil {
				t.Fatalf("%s did not converge: %v", description, lastErr)
			}
			t.Fatalf("%s did not converge", description)
		case <-poll.C:
		}
	}
}
