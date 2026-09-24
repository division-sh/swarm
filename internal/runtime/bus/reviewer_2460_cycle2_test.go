package bus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

func TestReviewCycle2DeferredDispatchFailureOwnership(t *testing.T) {
	for _, mode := range []string{"immediate", "intercepted", "deferred"} {
		for _, fault := range []string{"read_panic", "predecessor_error"} {
			for failAt := 0; failAt < 3; failAt++ {
				t.Run(fmt.Sprintf("%s/%s/position_%d", mode, fault, failAt+1), func(t *testing.T) {
					ctx := testAuthorActivityContext(context.Background())
					store := &review2460ReadFaultStore{targetRouteMemoryStore: newTargetRouteMemoryStore(), fault: errors.New("read failure")}
					bus, err := newScopedTestEventBus(store)
					if err != nil {
						t.Fatal(err)
					}
					intents := make([]runtimeengine.EmitIntent, 3)
					runID := uuid.NewString()
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
						operation, _ := pendingOutboxOperationForTest(bus, intent.Event.ID())
						claims[i] = operation.publicationClaim
					}
					if fault == "read_panic" {
						store.failID = intents[failAt].Event.ID()
						store.panicOnFailure = true
					} else {
						ctx = context.WithValue(ctx, deliveryDispatchScopeKey{}, &deliveryDispatchScope{publicationSettlement: &failingPublicationBoundary{err: errors.New("predecessor failed")}})
					}
					var got error
					func() {
						defer func() {
							if p := recover(); p != nil {
								got = fmt.Errorf("escaped panic: %v", p)
							}
						}()
						switch mode {
						case "immediate":
							got = bus.EngineDispatcher().DispatchPostCommit(ctx, intents)
						case "intercepted":
							got = (engineDispatcher{bus: bus}).dispatchCommittedInterceptorPublications(ctx, []events.Event{intents[0].Event, intents[1].Event, intents[2].Event})
						case "deferred":
							for _, intent := range intents {
								got = errors.Join(got, bus.publishDeferred(ctx, intent.Event))
							}
						}
					}()
					if got == nil {
						t.Fatal("fault not reported")
					}
					for i, intent := range intents {
						if _, pending := pendingOutboxOperationForTest(bus, intent.Event.ID()); pending {
							t.Errorf("output %d still staged after %s: %v", i, fault, got)
						}
						if !claims[i].released.Load() {
							t.Errorf("output %d claim not released after %s", i, fault)
						}
					}
				})
			}
		}
	}
}

func TestReviewCycle2InboundFinalizationKeepsCommittedSiblings(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("position_%d", failAt), func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			store := &InMemoryEventStore{}
			probe := &review2460PublicationProbe{failAt: failAt}
			expected := inboundBatchCurrentAuthorization()
			bus, err := newScopedTestEventBus(store, EventBusOptions{ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected}, TestLifecycleProbe: probe})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := bus.PrepareInboundDeliveryBatch(ctx, inboundBatchPreflightBatch(expected))
			if err != nil {
				t.Fatal(err)
			}
			var committed []CommittedPublication
			for _, command := range plan.CommitCommands() {
				result, err := store.CommitPublication(ctx, command)
				if err != nil {
					t.Fatal(err)
				}
				committed = append(committed, result)
			}
			_, got := bus.ApplyInboundDeliveryCommit(ctx, plan, committed)
			if got == nil {
				t.Fatal("missing diagnostic")
			}
			if probe.seen != len(committed) {
				t.Errorf("visited %d of %d committed publications: %v", probe.seen, len(committed), got)
			}
			for i, prepared := range plan.PreparedPublications() {
				if !prepared.publicationClaim.released.Load() {
					t.Errorf("publication %d has neither returned dispatch evidence nor released claim", i)
				}
			}
			_ = bus.AbandonInboundDeliveryPlan(ctx, plan)
		})
	}
}

func TestOrdinaryCommittedPublicationDiagnosticRetainsDispatchAuthority(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	store := newTargetRouteMemoryStore()
	probe := &review2460PublicationProbe{failAt: 1}
	bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
	if err != nil {
		t.Fatal(err)
	}
	internal := subscribeInternalDeliveriesForTest(t, bus, "committed-diagnostic-internal", events.EventType("custom.emitted"))
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.emitted", "", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	prepared, ready, diagnostic := bus.commitPublish(ctx, eventBusCommitPublishPlan{bus: bus, event: event})
	if !ready || diagnostic == nil || probe.seen != 1 {
		t.Fatalf("ready=%t diagnostic=%v notifications=%d", ready, diagnostic, probe.seen)
	}
	if prepared.Event.ID() != event.ID() {
		t.Fatalf("committed event=%s, want %s", prepared.Event.ID(), event.ID())
	}
	if len(prepared.plan.InternalRecipientIDs()) != 1 {
		t.Fatalf("internal recipients=%v, want exact internal subscriber", prepared.plan.InternalRecipientIDs())
	}
	if pending := bus.pendingInternalDeliveryForEvent(event.ID()); len(pending.recipients) != 0 {
		t.Fatalf("ordinary publication staged engine-only internal cache: %v", pending.recipients)
	}
	if err := bus.dispatchPreparedPublish(context.WithoutCancel(ctx), prepared); err != nil {
		t.Fatalf("dispatch exact committed event: %v", err)
	}
	delivery := requireBusEvent(t, internal, "ordinary committed internal recipient")
	if delivery.ID() != event.ID() {
		t.Fatalf("internal delivery=%s, want %s", delivery.ID(), event.ID())
	}
	if !prepared.publicationClaim.released.Load() {
		t.Fatal("acknowledged publication retained its local claim after dispatch")
	}
	if _, found, err := store.LoadPreparedPublishEvent(ctx, event.ID()); err != nil || !found {
		t.Fatalf("committed readback found=%t err=%v", found, err)
	}
}

func TestCommittedDispatchCancellationSettlesExactBatchClaims(t *testing.T) {
	for _, mode := range []string{"immediate", "intercepted", "deferred"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			store := newTargetRouteMemoryStore()
			bus, err := newScopedTestEventBus(store)
			if err != nil {
				t.Fatal(err)
			}
			intents := make([]runtimeengine.EmitIntent, 2)
			runID := uuid.NewString()
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
					t.Fatal("missing staged committed output")
				}
				claims[i] = operation.publicationClaim
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			switch mode {
			case "immediate":
				err = bus.EngineDispatcher().DispatchPostCommit(cancelled, intents)
			case "intercepted":
				err = (engineDispatcher{bus: bus}).dispatchCommittedInterceptorPublications(cancelled, []events.Event{intents[0].Event, intents[1].Event})
			case "deferred":
				for _, intent := range intents {
					err = errors.Join(err, bus.publishDeferred(cancelled, intent.Event))
				}
			}
			if err != nil {
				t.Fatalf("committed dispatch after caller cancellation: %v", err)
			}
			for i, intent := range intents {
				if _, pending := pendingOutboxOperationForTest(bus, intent.Event.ID()); pending || !claims[i].released.Load() {
					t.Errorf("output %d pending=%t released=%t", i, pending, claims[i].released.Load())
				}
				store.mu.Lock()
				got := store.receipts[intent.Event.ID()]
				store.mu.Unlock()
				if got != "processed" {
					t.Errorf("output %d durable receipt=%q, want processed", i, got)
				}
			}
		})
	}
}
