package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

// This double detects a second settlement; native catalog journeys prove the
// transaction, rollback, receipt, receiver and replay consequences on both stores.
type committedRejectionStore struct {
	*targetRouteMemoryStore
	settleCalls int
}

func (s *committedRejectionStore) Settle(context.Context, pipelineobligation.Claim, pipelineobligation.Disposition) (pipelineobligation.SettlementOutcome, error) {
	s.settleCalls++
	return pipelineobligation.SettlementOutcome{}, errors.New("already settled publication must not be settled again")
}

func TestIssue2564CommittedRouteRejectionIsNotAcknowledgedAgain(t *testing.T) {
	for _, mode := range []string{"ordinary", "engine", "continuation", "fan_out"} {
		t.Run(mode, func(t *testing.T) {
			store := &committedRejectionStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
			bus, err := newScopedTestEventBus(store, EventBusOptions{PipelineObligations: store})
			if err != nil {
				t.Fatal(err)
			}
			ctx := testAuthorActivityContext(context.Background())
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "child/output.done", "", "", []byte(`{}`), 0, uuid.NewString(), "",
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: uuid.NewString(), FlowInstance: "missing-flow"}), time.Now().UTC())
			intent := engine.EmitIntent{Event: event}
			plans, err := bus.PrepareEnginePublications(ctx, []engine.EmitIntent{intent})
			if err != nil || len(plans) != 1 {
				t.Fatalf("prepare rejected publication: plans=%d err=%v", len(plans), err)
			}
			plan := plans[0].(EnginePublicationPlan)
			if !plan.prepared.targetFailure || plan.command.Commit.Disposition == nil {
				t.Fatal("fixture must have a canonical publication-time route rejection")
			}
			committed, err := store.CommitPublication(ctx, plan.PublicationCommand())
			if err != nil {
				t.Fatal(err)
			}
			proof, err := NewCommittedEnginePublication(plan, committed.WithCommitAcknowledgment())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "ordinary" {
				consequences, finalizationErr := bus.finalizeCommittedPublicationConsequences(ctx, plan.prepared, proof.committed, false)
				if finalizationErr != nil || !consequences.ready {
					t.Fatalf("finalize rejected publication: ready=%t err=%v", consequences.ready, finalizationErr)
				}
				err = bus.dispatchPreparedPublish(ctx, consequences.prepared)
			} else {
				if err := bus.FinalizeEnginePublications(ctx, []engine.CommittedDurablePublication{proof}); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "engine":
					err = bus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{plan.intent})
				case "continuation":
					err = (engineDispatcher{bus: bus}).DispatchCommittedPublication(ctx, proof)
				case "fan_out":
					operation, found, takeErr := bus.takeCommittedOutboxOperation(proof)
					if takeErr != nil || !found {
						t.Fatalf("take rejected publication: found=%t err=%v", found, takeErr)
					}
					group := &publicationSettlementProbe{}
					collector := &fanOutPublicationSettlement{ctx: ctx, bus: bus, group: group}
					err = (engineDispatcher{bus: bus}).dispatchFanOutOperation(ctx, operation, plan.prepared.plan, collector, false)
					if len(group.settlements) != 0 {
						t.Fatal("rejected publication was collected for another group settlement")
					}
				}
			}
			if err != nil || store.settleCalls != 0 || store.receipts[event.ID()] != "dead_letter" || len(store.routes[event.ID()]) != 0 {
				t.Fatalf("committed rejection changed: err=%v settlements=%d receipt=%q routes=%d", err, store.settleCalls, store.receipts[event.ID()], len(store.routes[event.ID()]))
			}
			if !plan.prepared.publicationClaim.released.Load() || len(store.claims) != 0 {
				t.Fatal("rejected publication retained its claim after dispatch")
			}
		})
	}
}

func TestIssue2564CommittedRejectionCannotReplaceAcceptedEvidence(t *testing.T) {
	store := newTargetRouteMemoryStore()
	bus, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatal(err)
	}
	ctx := testAuthorActivityContext(context.Background())
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.emitted", "", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	plans, err := bus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
	if err != nil || len(plans) != 1 {
		t.Fatalf("prepare accepted publication: plans=%d err=%v", len(plans), err)
	}
	plan := plans[0].(EnginePublicationPlan)
	defer func() { _ = plan.prepared.publicationClaim.Release(ctx) }()
	if plan.prepared.targetFailure {
		t.Fatal("fixture must have accepted route evidence")
	}
	committed, err := store.CommitPublication(ctx, plan.PublicationCommand())
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewCommittedEnginePublication(plan, committed.WithCommitAcknowledgment())
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.FinalizeEnginePublications(ctx, []engine.CommittedDurablePublication{proof}); err != nil {
		t.Fatal(err)
	}
	bus.mu.Lock()
	bus.pendingOutboxByID[event.ID()][0].targetFailure = true
	bus.mu.Unlock()
	if _, found, err := bus.takeCommittedOutboxOperation(proof); err == nil || found {
		t.Fatalf("forged rejection replaced exact accepted evidence: found=%t err=%v", found, err)
	}
	if len(bus.pendingOutboxByID[event.ID()]) != 1 {
		t.Fatal("refusal consumed the legitimate pending operation")
	}
}
