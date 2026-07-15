package manager

import (
	"context"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestAuthBreakerConsumesRuntimeIngressSafetyPauseOwner(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	pauseCalls := 0
	var pauseReason string
	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		RuntimeIngressSafetyPause: func(_ context.Context, reason string, _ *runtimefailures.Envelope) error {
			pauseCalls++
			pauseReason = reason
			return nil
		},
	})

	am.maybeTripAuthCircuitBreaker(testAuthorActivityContext(context.Background()), "agent-1", "evt-1", testAuthFailure())
	am.maybeTripAuthCircuitBreaker(testAuthorActivityContext(context.Background()), "agent-1", "evt-2", testAuthFailure())

	if pauseCalls != 1 {
		t.Fatalf("runtime ingress safety pause calls = %d, want 1", pauseCalls)
	}
	if pauseReason != "authentication_intervention_required" {
		t.Fatalf("runtime ingress safety pause reason = %q, want authentication_intervention_required", pauseReason)
	}
}
