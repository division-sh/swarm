package manager

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type deliveryContextKey struct{}

type deliveryContextEffectStore struct{}

func (deliveryContextEffectStore) IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error) {
	return true, nil
}
func (deliveryContextEffectStore) AuthorizeExternalAttempt(context.Context, runtimeeffects.Authority, runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	return runtimeeffects.Attempt{}, nil
}
func (deliveryContextEffectStore) MarkExternalAttemptLaunched(context.Context, runtimeeffects.Attempt, time.Time) error {
	return nil
}
func (deliveryContextEffectStore) MarkExternalAttemptResponseObserved(context.Context, runtimeeffects.Attempt, map[string]any, time.Time) error {
	return nil
}
func (deliveryContextEffectStore) SettleExternalAttempt(context.Context, runtimeeffects.Settlement) error {
	return nil
}

func TestAgentDeliveryExecutionContextPreservesDeliveryTreeAndGenerationAuthority(t *testing.T) {
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: "manager-delivery-runtime", BundleHash: "manager-delivery-bundle"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	deliveryBase := context.WithValue(context.Background(), deliveryContextKey{}, "delivery-tree")
	event := eventtest.PersistedProjectionForProducer(
		uuid.NewString(), events.EventType("manager.delivery.root"), eventtest.Producer(events.EventProducerPlatform, "test"), "",
		[]byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	delivery, err := runtimeOwner.NewEventDelivery(deliveryBase, event)
	if err != nil {
		t.Fatalf("new event delivery: %v", err)
	}
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-1", Generation: 3}
	loopCtx := runtimeeffects.WithLifecycleToken(context.Background(), token)
	controller := runtimeeffects.NewController(deliveryContextEffectStore{})
	loopCtx = runtimeeffects.WithController(loopCtx, controller)
	loopCtx = managedExecutionTestContext(t, loopCtx)
	deliveryOwner, ok := worklifetime.OccurrenceFromContext(delivery.Context())
	if !ok {
		t.Fatal("delivery context has no occurrence owner")
	}

	got := agentDeliveryExecutionContext(delivery.Context(), loopCtx, token, deliveryOwner)
	if got.Value(deliveryContextKey{}) != "delivery-tree" {
		t.Fatal("agent execution context dropped delivery-tree values")
	}
	if owner, ok := worklifetime.OccurrenceFromContext(got); !ok || owner != runtimeOwner {
		t.Fatalf("agent execution occurrence = %v, %v; want delivery owner", owner, ok)
	}
	if gotToken, ok := runtimeeffects.LifecycleTokenFromContext(got); !ok || gotToken != token {
		t.Fatalf("agent execution lifecycle token = %#v, %v; want %#v", gotToken, ok, token)
	}
	if gotController, ok := runtimeeffects.ControllerFromContext(got); !ok || gotController != controller {
		t.Fatalf("agent execution effect controller = %p, %v; want %p", gotController, ok, controller)
	}
	if _, ok := managedexecution.FromContext(got); !ok {
		t.Fatal("agent execution context dropped managed-execution admission")
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete event delivery: %v", err)
	}
	if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime occurrence: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("join process: %v", err)
	}
}

func TestAgentManagerWorkUsesContextualStandingOccurrence(t *testing.T) {
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "manager-standing-runtime",
		BundleHash:        "manager-standing-bundle",
	})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	standing, err := runtimeOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{
		ServiceID:  "manager-standing-service",
		RunID:      uuid.NewString(),
		Generation: 1,
	})
	if err != nil {
		t.Fatalf("new standing occurrence: %v", err)
	}
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{WorkOwner: runtimeOwner})
	if _, started, err := am.lifecycle.beginRun(context.Background(), AgentRunModeStandard, runtimeOwner); err != nil || !started {
		t.Fatalf("begin manager run: started=%v err=%v", started, err)
	}
	lease, err := am.beginWork(worklifetime.WithOccurrence(context.Background(), standing), "standing manager work")
	if err != nil {
		t.Fatalf("begin standing manager work: %v", err)
	}
	if owner, ok := worklifetime.OccurrenceFromContext(lease.Context()); !ok {
		t.Fatal("manager work context has no composed occurrence owner")
	} else if _, ok := owner.(*worklifetime.ManagerWorkOccurrence); !ok {
		t.Fatalf("manager work owner = %T, want ManagerWorkOccurrence", owner)
	}

	standing.Retire()
	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("standing retirement did not cancel manager work")
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWait()
	if err := standing.Wait(waitCtx); err == nil {
		t.Fatal("standing wait completed while manager work remained active")
	}
	if err := lease.Done(); err != nil {
		t.Fatalf("settle standing manager work: %v", err)
	}
	if err := am.lifecycle.abortRunStart(context.Canceled); err != nil {
		t.Fatalf("retire manager run: %v", err)
	}
	if err := standing.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire standing occurrence: %v", err)
	}
	if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime occurrence: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("join process: %v", err)
	}
}
