package bus_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func assertNativeOutboxClaimReleased(t *testing.T, f *nativeOutboxFixture, eventID string) {
	t.Helper()
	claim, err := f.store.PipelineObligations().ClaimPublication(f.ctx, eventID)
	if err != nil {
		t.Fatalf("publication claim was not released: %v", err)
	}
	if err := f.store.PipelineObligations().Release(f.ctx, claim); err != nil {
		t.Fatal(err)
	}
}

func proveNativeOutboxPostWriteRollback(t *testing.T, backend string) {
	t.Helper()
	f := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "reviewer", "", ""))
	receiver := runtimebustest.Subscribe(t, f.bus, "reviewer")
	a, b := f.intent("native-rollback-first"), f.intent("native-rollback-second")
	before, found, err := f.coordinator.Load(f.ctx, f.identity)
	if err != nil || !found {
		t.Fatalf("load original workflow: found=%v err=%v", found, err)
	}
	// Arm removal before installation: a durable DDL write followed by
	// an uncertain acknowledgment must never skip fault cleanup.
	t.Cleanup(func() {
		if err := storetest.SetOutboxAfterFirstEventWriteFault(context.Background(), f.store, f.identity.RunID, a.Event.ID(), b.Event.ID(), false); err != nil {
			t.Error(err)
		}
	})
	if err := storetest.SetOutboxAfterFirstEventWriteFault(f.ctx, f.store, f.identity.RunID, a.Event.ID(), b.Event.ID(), true); err != nil {
		t.Fatal(err)
	}
	result, err := f.commit([]engine.EmitIntent{a, b})
	if result.Committed || err == nil || !strings.Contains(err.Error(), "outbox_after_first_event_write") {
		t.Fatalf("after-first-write cut was not reached: result=%+v err=%v", result, err)
	}
	if f.eventCount(t) != 0 {
		t.Fatal("partially written event escaped native rollback")
	}
	for _, intent := range []engine.EmitIntent{a, b} {
		recipients, err := f.store.ListEventDeliveryRecipients(f.ctx, intent.Event.ID())
		if err != nil || len(recipients) != 0 {
			t.Fatalf("rolled-back deliveries=%v err=%v", recipients, err)
		}
		assertNativeOutboxClaimReleased(t, f, intent.Event.ID())
	}
	after, found, err := f.coordinator.Load(f.ctx, f.identity)
	if err != nil || !found || !reflect.DeepEqual(before, after) {
		t.Fatalf("partial publication changed complete workflow state: found=%v err=%v\nbefore=%+v\nafter=%+v", found, err, before, after)
	}
	requireNoBusEvent(t, receiver, "native rollback dispatch")
}

func TestNativeOutboxAllDuplicateBatchPreservesDurableImageBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "reviewer", "", ""))
			receiver := runtimebustest.Subscribe(t, f.bus, "reviewer")
			intent := f.intent("native-preexisting-A")
			first, err := f.commit([]engine.EmitIntent{intent})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, first.EmitIntents); err != nil {
				t.Fatal(err)
			}
			requireBusEvent(t, receiver, "first inserted operation")
			before := f.eventImage(t, intent.Event.ID())
			readinessCalls := 0
			f.bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error { readinessCalls++; return nil }))
			result, err := f.commit([]engine.EmitIntent{intent, intent, intent})
			if err != nil || !result.Committed || len(result.EmitIntents) != 3 {
				t.Fatalf("duplicate result=%+v err=%v", result, err)
			}
			f.assertAppendOutcomes(t, false, false, false)
			if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, result.EmitIntents); err != nil {
				t.Fatal(err)
			}
			if readinessCalls != 0 || f.eventCount(t) != 1 || !reflect.DeepEqual(before, f.eventImage(t, intent.Event.ID())) {
				t.Fatalf("duplicate batch repeated readiness or changed durable event: readiness=%d", readinessCalls)
			}
			assertNativeOutboxClaimReleased(t, f, intent.Event.ID())
			requireNoBusEvent(t, receiver, "all-preexisting duplicates")
		})
	}
}

func TestNativeOutboxPartialPreparationFailureReleasesOwnedClaimsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, failure := range []string{"guard", "cancel"} {
			t.Run(backend+"/"+failure, func(t *testing.T) {
				fault := errors.New("second publication preparation fault")
				var f *nativeOutboxFixture
				var cancel context.CancelFunc
				guardCalls := 0
				f = newNativeOutboxFixtureWithOptions(t, backend, runtimebus.EventBusOptions{RecipientPlanGuard: func(_ context.Context, _ events.Event, _ runtimebus.PublishRecipientPlan) error {
					guardCalls++
					if guardCalls != 2 {
						return nil
					}
					if failure == "cancel" {
						cancel()
						return context.Canceled
					}
					return fault
				}})
				ctx, finish := context.WithCancel(f.ctx)
				cancel = finish
				defer finish()
				a, b := f.intent("partial-preparation-A"), f.intent("partial-preparation-B")
				a.Recipients, b.Recipients = nil, nil
				result, err := f.coordinator.CommitConstructedOutboxFixture(ctx, f.identity, []engine.EmitIntent{a, b})
				want := error(fault)
				if failure == "cancel" {
					want = context.Canceled
				}
				if !errors.Is(err, want) || result.Committed || guardCalls != 2 || f.eventCount(t) != 0 {
					t.Fatalf("partial preparation result=%+v calls=%d err=%v", result, guardCalls, err)
				}
				assertNativeOutboxClaimReleased(t, f, a.Event.ID())
				assertNativeOutboxClaimReleased(t, f, b.Event.ID())
			})
		}
	}
}

func TestNativeOutboxDuplicateFinalizationCannotReleaseInsertedClaimBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "reviewer", "", ""))
			receiver := runtimebustest.Subscribe(t, f.bus, "reviewer")
			intent := f.intent("native-duplicate-finalization")
			calls := 0
			fault := errors.New("duplicate attempted inserted-only readiness")
			f.bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
				calls++
				if calls > 1 {
					return fault
				}
				return nil
			}))
			result, err := f.commit([]engine.EmitIntent{intent, intent, intent})
			if err != nil || calls != 1 {
				t.Fatalf("duplicate finalization calls=%d result=%+v err=%v", calls, result, err)
			}
			if _, err := f.store.PipelineObligations().ClaimPublication(f.ctx, intent.Event.ID()); !errors.Is(err, pipelineobligation.ErrBusy) {
				t.Fatalf("duplicate finalization released inserted claim: %v", err)
			}
			if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, result.EmitIntents); err != nil {
				t.Fatal(err)
			}
			requireBusEvent(t, receiver, "inserted event with duplicate readiness refusal armed")
			requireNoBusEvent(t, receiver, "duplicate readiness delivery")
			assertNativeOutboxClaimReleased(t, f, intent.Event.ID())
		})
	}
}

func TestNativeOutboxExhaustedBatchNeverReconstructsMissingOutcomeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "reviewer", "", ""))
			receiver := runtimebustest.Subscribe(t, f.bus, "reviewer")
			intent := f.intent("native-exhausted-outcome")
			result, err := f.commit([]engine.EmitIntent{intent, intent})
			if err != nil {
				t.Fatal(err)
			}
			withMissing := append(result.EmitIntents, intent)
			if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, withMissing); err == nil || !strings.Contains(err.Error(), "no remaining ordered outbox outcome") {
				t.Fatalf("missing ordered outcome was reconstructed: %v", err)
			}
			requireBusEvent(t, receiver, "one genuinely inserted outcome")
			requireNoBusEvent(t, receiver, "exhausted fallback dispatch")
			assertNativeOutboxClaimReleased(t, f, intent.Event.ID())
		})
	}
}

func TestNativeOutboxBatchFailureAndShutdownPreserveCommittedEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"finalization", "shutdown"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				proveNativeOutboxBatchCleanup(t, backend, cut)
			})
		}
	}
}

func proveNativeOutboxBatchCleanup(t *testing.T, backend, cut string) {
	t.Helper()
	f := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "reviewer", "", ""))
	receiver := runtimebustest.Subscribe(t, f.bus, "reviewer")
	intent := f.intent("native-batch-cleanup")
	fault := errors.New("inserted publication readiness failed")
	readinessCalls := 0
	if cut == "finalization" {
		f.bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
			readinessCalls++
			return fault
		}))
	}
	result, err := f.commit([]engine.EmitIntent{intent, intent, intent})
	if !result.Committed || len(result.EmitIntents) != 3 {
		t.Fatalf("native batch lost commit evidence: result=%+v err=%v", result, err)
	}
	if cut == "finalization" {
		if !errors.Is(err, fault) || readinessCalls != 1 {
			t.Fatalf("finalization fault=%v calls=%d", err, readinessCalls)
		}
		if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, result.EmitIntents); !errors.Is(err, fault) {
			t.Fatalf("dispatch lost committed finalization failure: %v", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	before := f.eventImage(t, intent.Event.ID())
	if err := f.bus.ResetInMemoryState(); err != nil {
		t.Fatal(err)
	}
	assertNativeOutboxClaimReleased(t, f, intent.Event.ID())
	if f.eventCount(t) != 1 || !reflect.DeepEqual(before, f.eventImage(t, intent.Event.ID())) {
		t.Fatal("batch cleanup mutated committed durable evidence")
	}
	requireNoBusEvent(t, receiver, "failed or shutdown batch")
}
