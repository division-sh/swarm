package manager

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type deliveryContextKey struct{}

type deliveryContextEffectStore struct{}

type deliveryTreeAgent struct {
	id   string
	root events.EventType
	leaf events.Event
}

func (a *deliveryTreeAgent) ID() string                        { return a.id }
func (*deliveryTreeAgent) Type() string                        { return "delivery-tree" }
func (a *deliveryTreeAgent) Subscriptions() []events.EventType { return []events.EventType{a.root} }
func (a *deliveryTreeAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return []events.Event{a.leaf}, nil
}

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
	am := &AgentManager{workOwner: runtimeOwner}
	lease, err := am.beginWork(worklifetime.WithOccurrence(context.Background(), standing), "standing manager work")
	if err != nil {
		t.Fatalf("begin standing manager work: %v", err)
	}
	if owner, ok := worklifetime.OccurrenceFromContext(lease.Context()); !ok || owner != standing {
		t.Fatalf("manager work owner = %v, %v; want standing occurrence", owner, ok)
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

func TestAgentOutputRemainsInPublishAndWaitDeliveryTree(t *testing.T) {
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: "manager-tree-runtime", BundleHash: "manager-tree-bundle"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{WorkOwner: runtimeOwner})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rootType := events.EventType("manager.delivery.root")
	leafType := events.EventType("manager.delivery.leaf")
	runID := uuid.NewString()
	rootEvent := eventtest.PersistedProjectionForProducer(
		uuid.NewString(), rootType, eventtest.Producer(events.EventProducerExternal, "test"), "",
		[]byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	leafEvent := eventtest.ChildWithLineage(
		uuid.NewString(), leafType, "tree-agent", "", []byte(`{}`), 1,
		events.EventLineage{RunID: runID, ParentEventID: rootEvent.ID(), ExecutionMode: executionmode.Live}, events.EventEnvelope{}, time.Now().UTC(),
	)
	agent := &deliveryTreeAgent{id: "tree-agent", root: rootType, leaf: leafEvent}
	baseCtx := managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))
	am := NewAgentManagerWithOptions(eb, func(runtimeactors.AgentConfig) (Agent, error) { return agent, nil }, AgentManagerOptions{
		BaseContext: baseCtx,
		WorkOwner:   runtimeOwner,
	})
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: agent.id, ExecutionMode: "live", Subscriptions: []string{string(rootType)}}); err != nil {
		t.Fatalf("spawn delivery-tree agent: %v", err)
	}
	am.Run(baseCtx)
	leafDeliveries := managerInternalDeliveriesForTest(t, eb, "leaf-proof", leafType)
	publishDone := make(chan error, 1)
	go func() { publishDone <- eb.PublishAndWait(baseCtx, rootEvent) }()
	var leafDelivery *worklifetime.EventDelivery
	select {
	case leafDelivery = <-leafDeliveries:
	case err := <-publishDone:
		t.Fatalf("PublishAndWait returned before agent descendant delivery: %v", err)
	case <-time.After(time.Second):
		t.Fatal("agent descendant delivery was not accepted")
	}
	select {
	case err := <-publishDone:
		t.Fatalf("PublishAndWait returned before agent descendant completion: %v", err)
	default:
	}
	if err := leafDelivery.Complete(); err != nil {
		t.Fatalf("complete agent descendant delivery: %v", err)
	}
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("PublishAndWait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("PublishAndWait did not join the completed agent descendant")
	}
	if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
		t.Fatalf("shutdown agent manager: %v", err)
	}
	if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime occurrence: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("join process: %v", err)
	}
}
