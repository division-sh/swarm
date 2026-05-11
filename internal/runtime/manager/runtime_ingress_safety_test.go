package manager

import (
	"context"
	"errors"
	"testing"

	runtimebus "swarm/internal/runtime/bus"
)

func TestAuthBreakerConsumesRuntimeIngressSafetyPauseOwner(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	pauseCalls := 0
	var pauseReason string
	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		RuntimeIngressSafetyPause: func(_ context.Context, reason string) error {
			pauseCalls++
			pauseReason = reason
			return nil
		},
	})

	am.maybeTripAuthCircuitBreaker(context.Background(), "agent-1", "evt-1", errors.New("claude auth required"))
	am.maybeTripAuthCircuitBreaker(context.Background(), "agent-1", "evt-2", errors.New("claude auth required"))

	if pauseCalls != 1 {
		t.Fatalf("runtime ingress safety pause calls = %d, want 1", pauseCalls)
	}
	if pauseReason != "claude_auth_required" {
		t.Fatalf("runtime ingress safety pause reason = %q, want claude_auth_required", pauseReason)
	}
}
