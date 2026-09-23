package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type review2460PublicationProbe struct {
	seen   int
	failAt int
}

func (p *review2460PublicationProbe) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.EventPersisted {
		p.seen++
		if p.seen == p.failAt {
			panic("first publication notification failed")
		}
	}
}

type review2460ReadFaultStore struct {
	*targetRouteMemoryStore
	failID         string
	fault          error
	panicOnFailure bool
	reads          []string
}

func pendingOutboxOperationForTest(bus *EventBus, eventID string) (pendingOutboxOperation, bool) {
	bus.mu.RLock()
	defer bus.mu.RUnlock()
	operations := bus.pendingOutboxByID[eventID]
	if len(operations) == 0 {
		return pendingOutboxOperation{}, false
	}
	return operations[0], true
}

func (s *review2460ReadFaultStore) LoadPreparedPublishEvent(ctx context.Context, id string) (PreparedPublishEvent, bool, error) {
	if s.failID != "" {
		s.reads = append(s.reads, id)
		if id == s.failID {
			if s.panicOnFailure {
				panic("selected read panic")
			}
			return PreparedPublishEvent{}, false, s.fault
		}
	}
	return s.targetRouteMemoryStore.LoadPreparedPublishEvent(ctx, id)
}

func TestReview2460ImmediateDispatchContinuesAfterFirstError(t *testing.T) {
	review2460DispatchContinues(t, false)
}
func TestReview2460InterceptedDispatchContinuesAfterFirstError(t *testing.T) {
	review2460DispatchContinues(t, true)
}
func review2460DispatchContinues(t *testing.T, intercepted bool) {
	ctx := testAuthorActivityContext(context.Background())
	store := &review2460ReadFaultStore{targetRouteMemoryStore: newTargetRouteMemoryStore(), fault: errors.New("first dispatch read failed")}
	bus, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	intents := make([]runtimeengine.EmitIntent, 2)
	for i := range intents {
		intents[i].Event = eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.emitted", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	}
	plans, err := bus.PrepareEnginePublications(ctx, intents)
	if err != nil {
		t.Fatal(err)
	}
	evidence := make([]runtimeengine.CommittedDurablePublication, 0, 2)
	for _, raw := range plans {
		plan := raw.(EnginePublicationPlan)
		committed, err := store.CommitPublication(ctx, plan.PublicationCommand())
		if err != nil {
			t.Fatal(err)
		}
		proof, err := NewCommittedEnginePublication(plan, committed)
		if err != nil {
			t.Fatal(err)
		}
		evidence = append(evidence, proof)
	}
	if err := bus.FinalizeEnginePublications(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	store.failID = intents[0].Event.ID()
	if intercepted {
		err = (engineDispatcher{bus: bus}).dispatchCommittedInterceptorPublications(ctx, []events.Event{intents[0].Event, intents[1].Event})
	} else {
		err = bus.EngineDispatcher().DispatchPostCommit(ctx, intents)
	}
	if !errors.Is(err, store.fault) {
		t.Fatalf("expected read fault, got %v", err)
	}
	if _, pending := pendingOutboxOperationForTest(bus, intents[1].Event.ID()); pending {
		t.Fatalf("second committed publication skipped and its claim left staged; read attempts: %v", store.reads)
	}
}

func TestReview2460PublicationFinalizationContinuesAfterFirstPanic(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	store := &InMemoryEventStore{}
	probe := &review2460PublicationProbe{failAt: 1}
	bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	intents := make([]runtimeengine.EmitIntent, 2)
	for i := range intents {
		intents[i].Event = eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.emitted", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	}
	plans, err := bus.PrepareEnginePublications(ctx, intents)
	if err != nil {
		t.Fatal(err)
	}
	evidence := make([]runtimeengine.CommittedDurablePublication, 0, 2)
	for _, raw := range plans {
		plan := raw.(EnginePublicationPlan)
		committed, err := store.CommitPublication(ctx, plan.PublicationCommand())
		if err != nil {
			t.Fatal(err)
		}
		proof, err := NewCommittedEnginePublication(plan, committed)
		if err != nil {
			t.Fatal(err)
		}
		evidence = append(evidence, proof)
	}
	var finalErr error
	func() {
		defer func() {
			if value := recover(); value != nil {
				finalErr = fmt.Errorf("%v", value)
			}
		}()
		finalErr = bus.FinalizeEnginePublications(ctx, evidence)
	}()
	if finalErr == nil {
		t.Fatal("fault was not reported")
	}
	if probe.seen != 2 {
		t.Errorf("notified %d publications, want 2 independent committed outputs", probe.seen)
	}
	for _, intent := range intents {
		if _, ok := pendingOutboxOperationForTest(bus, intent.Event.ID()); !ok {
			t.Errorf("committed publication %s lost its staged handoff", intent.Event.ID())
		}
	}
}

func TestCommittedPublicationFinalizationVisitsEveryOutputAcrossPanicPositions(t *testing.T) {
	for failAt := 1; failAt <= 3; failAt++ {
		t.Run(fmt.Sprintf("position_%d", failAt), func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			store := &InMemoryEventStore{}
			probe := &review2460PublicationProbe{failAt: failAt}
			bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
			if err != nil {
				t.Fatal(err)
			}
			runID := uuid.NewString()
			intents := make([]runtimeengine.EmitIntent, 3)
			for i := range intents {
				intents[i].Event = eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.emitted", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			}
			plans, err := bus.PrepareEnginePublications(ctx, intents)
			if err != nil {
				t.Fatal(err)
			}
			var evidence []runtimeengine.CommittedDurablePublication
			for _, raw := range plans {
				plan := raw.(EnginePublicationPlan)
				committed, err := store.CommitPublication(ctx, plan.PublicationCommand())
				if err != nil {
					t.Fatal(err)
				}
				proof, err := NewCommittedEnginePublication(plan, committed)
				if err != nil {
					t.Fatal(err)
				}
				evidence = append(evidence, proof)
			}
			if err := bus.FinalizeEnginePublications(ctx, evidence); err == nil {
				t.Fatal("notification panic was not reported")
			}
			if probe.seen != len(intents) {
				t.Fatalf("visited %d outputs, want %d", probe.seen, len(intents))
			}
			for _, intent := range intents {
				operation, ok := pendingOutboxOperationForTest(bus, intent.Event.ID())
				if !ok || operation.finalizationErr != nil {
					t.Fatalf("committed output %s lost its exact ready operation: found=%t error=%v", intent.Event.ID(), ok, operation.finalizationErr)
				}
				if err := operation.publicationClaim.Release(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestImmediateCommittedDispatchVisitsEveryOutputAcrossReadFaultPositions(t *testing.T) {
	for _, panicOnFailure := range []bool{false, true} {
		for failAt := 0; failAt < 3; failAt++ {
			t.Run(fmt.Sprintf("panic_%t/position_%d", panicOnFailure, failAt+1), func(t *testing.T) {
				ctx := testAuthorActivityContext(context.Background())
				store := &review2460ReadFaultStore{targetRouteMemoryStore: newTargetRouteMemoryStore(), fault: errors.New("selected read fault"), panicOnFailure: panicOnFailure}
				bus, err := newScopedTestEventBus(store)
				if err != nil {
					t.Fatal(err)
				}
				runID := uuid.NewString()
				intents := make([]runtimeengine.EmitIntent, 3)
				for i := range intents {
					intents[i].Event = eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.emitted", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
				}
				plans, err := bus.PrepareEnginePublications(ctx, intents)
				if err != nil {
					t.Fatal(err)
				}
				var evidence []runtimeengine.CommittedDurablePublication
				for _, raw := range plans {
					plan := raw.(EnginePublicationPlan)
					committed, err := store.CommitPublication(ctx, plan.PublicationCommand())
					if err != nil {
						t.Fatal(err)
					}
					proof, err := NewCommittedEnginePublication(plan, committed)
					if err != nil {
						t.Fatal(err)
					}
					evidence = append(evidence, proof)
				}
				if err := bus.FinalizeEnginePublications(ctx, evidence); err != nil {
					t.Fatal(err)
				}
				claims := make([]*pipelinePublicationClaim, len(intents))
				for i, intent := range intents {
					operation, ok := pendingOutboxOperationForTest(bus, intent.Event.ID())
					if !ok {
						t.Fatalf("committed output %s was not staged", intent.Event.ID())
					}
					claims[i] = operation.publicationClaim
				}
				store.failID = intents[failAt].Event.ID()
				err = bus.EngineDispatcher().DispatchPostCommit(ctx, intents)
				if panicOnFailure {
					if err == nil || !strings.Contains(err.Error(), "selected read panic") {
						t.Fatalf("dispatch panic diagnostic = %v", err)
					}
				} else if !errors.Is(err, store.fault) {
					t.Fatalf("dispatch error = %v", err)
				}
				for _, intent := range intents {
					if _, pending := pendingOutboxOperationForTest(bus, intent.Event.ID()); pending {
						t.Fatalf("committed output %s retained its operation", intent.Event.ID())
					}
				}
				for i, claim := range claims {
					if !claim.released.Load() {
						t.Fatalf("committed output %s retained its publication claim", intents[i].Event.ID())
					}
				}
			})
		}
	}
}

func TestCommittedDispatchRejectsSameIDForeignPayloadWithoutConsumingClaim(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	store := newTargetRouteMemoryStore()
	bus, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatal(err)
	}
	runID, eventID := uuid.NewString(), uuid.NewString()
	original := runtimeengine.EmitIntent{Event: eventtest.RunCreatingRootIngress(eventID, "custom.emitted", "", "", []byte(`{"value":1}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())}
	plans, err := bus.PrepareEnginePublications(ctx, []runtimeengine.EmitIntent{original})
	if err != nil {
		t.Fatal(err)
	}
	plan := plans[0].(EnginePublicationPlan)
	committed, err := store.CommitPublication(ctx, plan.PublicationCommand())
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewCommittedEnginePublication(plan, committed)
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.FinalizeEnginePublications(ctx, []runtimeengine.CommittedDurablePublication{proof}); err != nil {
		t.Fatal(err)
	}
	foreign := runtimeengine.EmitIntent{Event: eventtest.RunCreatingRootIngress(eventID, "custom.emitted", "", "", []byte(`{"value":2}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())}
	if _, err := (engineDispatcher{bus: bus}).dispatchPendingOutboxOperation(ctx, foreign); !errors.Is(err, events.ErrEventIdentityConflict) {
		t.Fatalf("foreign same-ID dispatch = %v", err)
	}
	wrongTime := runtimeengine.EmitIntent{Event: eventtest.RunCreatingRootIngress(eventID, "custom.emitted", "", "", []byte(`{"value":1}`), 0, runID, "", events.EventEnvelope{}, original.Event.CreatedAt().Add(time.Second))}
	if _, err := (engineDispatcher{bus: bus}).dispatchPendingOutboxOperation(ctx, wrongTime); !errors.Is(err, events.ErrEventIdentityConflict) {
		t.Fatalf("foreign same-ID timestamp dispatch = %v", err)
	}
	if _, err := (engineDispatcher{bus: bus}).dispatchPendingOutboxOperation(ctx, original); err != nil {
		t.Fatalf("exact committed dispatch lost after foreign callback: %v", err)
	}
}

func TestCommittedDispatchMatchesOnlyStagedSourceOrProjectedEvent(t *testing.T) {
	bus, err := newScopedTestEventBus(newTargetRouteMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	runID, eventID := uuid.NewString(), uuid.NewString()
	stamp := time.Now().UTC()
	source := eventtest.RunCreatingRootIngress(eventID, "custom.emitted", "", "", []byte(`{"value":1}`), 0, runID, "", events.EventEnvelope{}, stamp)
	projected := eventtest.RunCreatingRootIngress(eventID, "custom.emitted", "", "", []byte(`{"value":1}`), 0, runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()), stamp)
	foreign := eventtest.RunCreatingRootIngress(eventID, "custom.emitted", "", "", []byte(`{"value":1}`), 0, runID, "", events.EventEnvelope{}, stamp.Add(time.Second))
	intent := runtimeengine.EmitIntent{Event: projected}
	for _, accepted := range []events.Event{source, projected} {
		bus.stageCommittedOutboxOperationWithFinalization(intent, source, EventAppendInserted, nil, nil, nil)
		if _, ok, err := bus.takeMatchingPendingOutboxOperation(foreign); !errors.Is(err, events.ErrEventIdentityConflict) || ok {
			t.Fatalf("foreign staged view accepted: ok=%t err=%v", ok, err)
		}
		operation, ok, err := bus.takeMatchingPendingOutboxOperation(accepted)
		if err != nil || !ok || operation.intent.Event.ID() != eventID {
			t.Fatalf("exact staged view rejected: ok=%t err=%v", ok, err)
		}
	}
}

func TestCommittedDispatchFencedPreflightReleasesEveryExactClaim(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: "review-committed-dispatch"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := owner.RetireAndWait(context.Background()); err != nil {
			t.Error(err)
		}
		if _, err := process.Join(context.Background()); err != nil {
			t.Error(err)
		}
	})
	store := newTargetRouteMemoryStore()
	bus, err := newScopedTestEventBus(store, EventBusOptions{WorkOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	intents := make([]runtimeengine.EmitIntent, 2)
	for i := range intents {
		intents[i].Event = eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.emitted", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	}
	plans, err := bus.PrepareEnginePublications(ctx, intents)
	if err != nil {
		t.Fatal(err)
	}
	var evidence []runtimeengine.CommittedDurablePublication
	for _, raw := range plans {
		plan := raw.(EnginePublicationPlan)
		committed, err := store.CommitPublication(ctx, plan.PublicationCommand())
		if err != nil {
			t.Fatal(err)
		}
		proof, err := NewCommittedEnginePublication(plan, committed)
		if err != nil {
			t.Fatal(err)
		}
		evidence = append(evidence, proof)
	}
	if err := bus.FinalizeEnginePublications(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	if err := owner.Fence(); err != nil {
		t.Fatal(err)
	}
	if err := bus.EngineDispatcher().DispatchPostCommit(ctx, intents); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("fenced dispatch = %v", err)
	}
	for _, intent := range intents {
		if _, pending := pendingOutboxOperationForTest(bus, intent.Event.ID()); pending {
			t.Fatalf("fenced output %s retained process-local claim", intent.Event.ID())
		}
	}
}

func TestCommittedFlowActivationFinalizerVisitsSiblingsAfterErrorOrPanic(t *testing.T) {
	for _, failure := range []string{"error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			seen := 0
			bus := &EventBus{flowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(context.Context, runtimepipeline.CommittedFlowInstanceActivation) error {
				seen++
				if seen == 1 {
					if failure == "panic" {
						panic("first activation failed")
					}
					return errors.New("first activation failed")
				}
				return nil
			})}
			activations := make([]CommittedFlowInstanceActivation, 3)
			if err := bus.finalizeCommittedFlowInstanceActivations(context.Background(), activations); err == nil {
				t.Fatal("missing first activation diagnostic")
			}
			if seen != len(activations) {
				t.Fatalf("visited %d committed activations, want %d", seen, len(activations))
			}
		})
	}
}
