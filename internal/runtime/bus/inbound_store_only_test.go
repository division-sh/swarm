package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func TestStoredInboundPlanRequiresRecoverySealAndCannotAttachExecution(t *testing.T) {
	source, catalog, batch := authenticatedTelegramBatchFixture(t, "telegram-ingress", true)
	store := newTargetRouteMemoryStore()
	bus, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source, ProviderOutputVerifier: catalog,
		Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := inboundBatchAuthorContext(t, source)
	if _, err := bus.PrepareStoredInboundDeliveryBatch(ctx, batch); err == nil {
		t.Fatal("store-only planning preceded recovery join")
	}
	if err := bus.SealPipelineRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	plan, err := bus.PrepareStoredInboundDeliveryBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := bus.AbandonInboundDeliveryPlan(context.WithoutCancel(ctx), plan); err != nil {
			t.Error(err)
		}
	}()
	if len(plan.commands) != 2 {
		t.Fatalf("ordinary raw/normalized facts=%d, want2", len(plan.commands))
	}
	for index, command := range plan.commands {
		if err := command.ValidateInbound(plan.Admission()); err != nil {
			t.Fatalf("ordinary admission %d: %v", index, err)
		}
		prepared := plan.prepared[index]
		if !prepared.storeOnly || prepared.requiresReceiver() {
			t.Fatalf("store-only plan acquired executable receiver: %+v", prepared)
		}
		if err := bus.DispatchPreparedPublish(ctx, prepared); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
			t.Fatalf("direct dispatch bypass=%v", err)
		}
		if err := bus.DispatchPreparedPublishAsync(ctx, prepared); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
			t.Fatalf("async dispatch bypass=%v", err)
		}
	}
	if _, err := bus.ApplyInboundDeliveryCommit(ctx, plan, nil); err == nil {
		t.Fatal("store-only plan entered activation/readiness finalizers")
	}
	if err := bus.CompleteStoredInboundDeliveryCommit(ctx, plan, nil); err == nil {
		t.Fatal("missing native commit evidence accepted")
	}
	if err := bus.CompleteStoredInboundDeliveryCommit(ctx, InboundDeliveryPlan{}, nil); err == nil {
		t.Fatal("ordinary plan converted to store-only completion")
	}
}
