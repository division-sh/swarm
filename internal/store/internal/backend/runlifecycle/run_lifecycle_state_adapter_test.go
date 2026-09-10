package runlifecycle

import (
	"testing"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestTerminalMutationDeliveryEffectsAreClosed(t *testing.T) {
	for _, tc := range []struct {
		state                runtimerunlifecycle.State
		terminalize, invalid bool
	}{
		{runtimerunlifecycle.StateFailed, true, false},
		{runtimerunlifecycle.StateCancelled, true, false},
		{runtimerunlifecycle.StateCompleted, false, false},
		{runtimerunlifecycle.StateForked, false, false},
		{runtimerunlifecycle.StateRunning, false, true},
		{runtimerunlifecycle.StatePaused, false, true},
		{"", false, true},
		{"future_terminal", false, true},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			got, err := (terminalRunMutation{State: tc.state}).terminalizesDeliveries()
			if got != tc.terminalize || (err != nil) != tc.invalid {
				t.Fatalf("delivery effects = %t, %v; want terminalize=%t invalid=%t", got, err, tc.terminalize, tc.invalid)
			}
		})
	}
}
