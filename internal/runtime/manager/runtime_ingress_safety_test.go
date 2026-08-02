package manager

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestAuthBreakerConsumesRuntimeIngressSafetyPauseOwner(t *testing.T) {
	bus, err := newTestManagerEventBus(t)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	pauseCalls := 0
	var pauseReason string
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{
		RuntimeIngressSafetyPause: func(_ context.Context, reason string, _ *runtimefailures.Envelope) error {
			pauseCalls++
			pauseReason = reason
			return nil
		},
	})

	identity := managerAgentDeliveryRoute("agent-1").AgentIdentity
	am.maybeTripAuthCircuitBreaker(testAuthorActivityContext(context.Background()), identity, eventtest.RunCreatingRootIngress("evt-1", events.EventType("work.requested"), "source", "", nil, 0, "run-1", "", events.EventEnvelope{}, time.Time{}), testAuthFailure())
	am.maybeTripAuthCircuitBreaker(testAuthorActivityContext(context.Background()), identity, eventtest.RunCreatingRootIngress("evt-2", events.EventType("work.requested"), "source", "", nil, 0, "run-1", "", events.EventEnvelope{}, time.Time{}), testAuthFailure())

	if pauseCalls != 1 {
		t.Fatalf("runtime ingress safety pause calls = %d, want 1", pauseCalls)
	}
	if pauseReason != "authentication_intervention_required" {
		t.Fatalf("runtime ingress safety pause reason = %q, want authentication_intervention_required", pauseReason)
	}
}
