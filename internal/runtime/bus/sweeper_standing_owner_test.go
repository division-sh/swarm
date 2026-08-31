package bus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type recoveryOriginStore struct {
	InMemoryEventStore
	origin      runtimerunlifecycle.RunOrigin
	disposition runtimepipeline.StandingRestartDisposition
	loads       int
}

func (s *recoveryOriginStore) StandingRunRestartDisposition(context.Context, string) (runtimepipeline.StandingRestartDisposition, error) {
	if s.disposition.Kind != "" {
		return s.disposition, nil
	}
	return runtimepipeline.ClassifyStandingRestart(runtimepipeline.StandingRestartFact{})
}

func (s *recoveryOriginStore) LoadRunOrigin(context.Context, string) (runtimerunlifecycle.RunOrigin, error) {
	s.loads++
	return s.origin, nil
}

func TestStandingPipelineRecoveryBlocksUntilExactOwnerIsInstalled(t *testing.T) {
	runID := uuid.NewString()
	standing, err := runtimerunlifecycle.StandingGenerationRunOrigin(uuid.NewString(), 1)
	if err != nil {
		t.Fatal(err)
	}
	disposition, err := runtimepipeline.ClassifyStandingRestart(runtimepipeline.StandingRestartFact{
		ExactCurrent: true, ServiceID: standing.ServiceID(), RunID: runID, Generation: 1,
		DeclarationPresent: true, EffectiveState: "active", OperatorOverride: "none", RunState: "running",
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &recoveryOriginStore{origin: standing, disposition: disposition}
	bus := &EventBus{store: store, durable: DurableDependencies{RunOrigins: store, StandingRestarts: store}}
	event := eventtest.ExistingRunRootIngress(
		uuid.NewString(),
		events.EventType("test.standing.recovery"),
		"test",
		"",
		json.RawMessage(`{}`),
		0,
		runID,
		events.EventEnvelope{},
		time.Now().UTC(),
	)

	_, lease, err := bus.bindClaimedRunWork(context.Background(), event)
	if !errors.Is(err, ErrRunDispatchBlocked) {
		t.Fatalf("bind standing recovery without owner error = %v, want ErrRunDispatchBlocked", err)
	}
	if lease != nil {
		t.Fatal("standing recovery without owner acquired a lease")
	}
	if store.loads != 1 {
		t.Fatalf("typed run origin reads = %d, want 1", store.loads)
	}
}

func TestStandingPipelineRecoveryParksNonExecutableDispositionBeforeLease(t *testing.T) {
	runID := uuid.NewString()
	standing, err := runtimerunlifecycle.StandingGenerationRunOrigin(uuid.NewString(), 1)
	if err != nil {
		t.Fatal(err)
	}
	store := &recoveryOriginStore{
		origin:      standing,
		disposition: runtimepipeline.StandingRestartDisposition{Kind: runtimepipeline.StandingRestartTerminalDeclared},
	}
	bus := &EventBus{store: store, durable: DurableDependencies{RunOrigins: store, StandingRestarts: store}}
	event := eventtest.ExistingRunRootIngress(
		uuid.NewString(),
		events.EventType("test.standing.terminal-recovery"),
		"test",
		"",
		json.RawMessage(`{}`),
		0,
		runID,
		events.EventEnvelope{},
		time.Now().UTC(),
	)

	_, lease, err := bus.bindClaimedRunWork(context.Background(), event)
	if !errors.Is(err, errStandingRestartParked) {
		t.Fatalf("bind terminal standing recovery error = %v, want parked disposition", err)
	}
	if lease != nil {
		t.Fatal("terminal standing recovery acquired a lease")
	}
}

func TestNonStandingPipelineRecoveryDoesNotRequireStandingOwner(t *testing.T) {
	store := &recoveryOriginStore{origin: runtimerunlifecycle.ScenarioSetupRunOrigin()}
	bus := &EventBus{store: store, durable: DurableDependencies{RunOrigins: store, StandingRestarts: store}}
	event := eventtest.ExistingRunRootIngress(
		uuid.NewString(),
		events.EventType("test.ordinary.recovery"),
		"test",
		"",
		json.RawMessage(`{}`),
		0,
		uuid.NewString(),
		events.EventEnvelope{},
		time.Now().UTC(),
	)

	_, lease, err := bus.bindClaimedRunWork(context.Background(), event)
	if err != nil {
		t.Fatalf("bind non-standing recovery without owner: %v", err)
	}
	if lease != nil {
		t.Fatal("non-standing recovery acquired a standing lease")
	}
	if store.loads != 1 {
		t.Fatalf("typed run origin reads = %d, want 1", store.loads)
	}
}
