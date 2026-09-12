package manager

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type receiptOutcomeStore struct {
	runtimedelivery.Store
	authority          runtimedelivery.ExecutionAuthority
	failure            error
	uncommitted        bool
	foreign            bool
	settlements        atomic.Int32
	postCommitRenewals atomic.Int32
	committed          atomic.Bool
	returned           runtimedelivery.Snapshot
}

func (s *receiptOutcomeStore) managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority {
	if s.authority.Validate() == nil {
		return s.authority
	}
	return s.Store.(interface {
		managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority
	}).managerTestDeliveryAuthority()
}

func (s *receiptOutcomeStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	if s.committed.Load() {
		s.postCommitRenewals.Add(1)
	}
	return s.Store.RenewClaim(ctx, claim)
}

func (s *receiptOutcomeStore) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, effects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.Snapshot, error) {
	s.settlements.Add(1)
	if s.uncommitted {
		return runtimedelivery.Snapshot{}, s.failure
	}
	snapshot, err := s.Store.SettleSuccess(ctx, claim, effects, duration, selection)
	return s.afterCommit(snapshot, err)
}

func (s *receiptOutcomeStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	s.settlements.Add(1)
	if s.uncommitted {
		return runtimedelivery.Snapshot{}, s.failure
	}
	snapshot, err := s.Store.SettleFailure(ctx, claim, settlement)
	return s.afterCommit(snapshot, err)
}

func (s *receiptOutcomeStore) afterCommit(snapshot runtimedelivery.Snapshot, err error) (runtimedelivery.Snapshot, error) {
	if snapshot.DeliveryID == "" {
		return snapshot, err
	}
	s.committed.Store(true)
	if s.foreign {
		snapshot.ClaimVersion++
	}
	s.returned = snapshot
	return snapshot, errors.Join(err, s.failure)
}

type receiptOutcomeBus struct {
	recordingReceiptBus
	failure  error
	released []string
}

func (b *receiptOutcomeBus) RetainDeliveryContinuation(snapshot runtimedelivery.Snapshot) error {
	b.retainedContinuations = append(b.retainedContinuations, snapshot)
	return b.failure
}

func (b *receiptOutcomeBus) ReleaseDeliveryContinuation(deliveryID string) error {
	b.released = append(b.released, deliveryID)
	return b.failure
}

// Real SQLite delivery-adapter COMMIT precedes the injected owner return error.
// This tests the exact manager consumer, not driver cleanup or wire failure.
func TestWriteReceiptPreservesCommittedSettlementAndContinuation(t *testing.T) {
	for _, status := range []ReceiptStatus{ReceiptStatusProcessed, ReceiptStatusError, ReceiptStatusDeadLetter, ReceiptStatusTerminal} {
		for _, phase := range []string{"healthy", "commit_error", "continuation_error", "both_errors", "uncommitted", "foreign_snapshot"} {
			t.Run(string(status)+"/"+phase, func(t *testing.T) {
				failure, cleanup := errors.New("postcommit owner failure"), errors.New("continuation cleanup failure")
				store := &receiptOutcomeStore{Store: newManagerDeliveryTestStore(t), uncommitted: phase == "uncommitted", foreign: phase == "foreign_snapshot"}
				bus := &receiptOutcomeBus{}
				if phase == "commit_error" || phase == "both_errors" || phase == "uncommitted" {
					store.failure = failure
				}
				if phase == "continuation_error" || phase == "both_errors" {
					bus.failure = cleanup
				}
				am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: store})
				evt := eventtest.RunCreatingRootIngress(eventtest.UUID("receipt-outcome"), "work.requested", "", "", nil, 0, eventtest.UUID("receipt-outcome-run"), "", events.EventEnvelope{}, time.Time{})
				ctx := managerClaimedDeliveryContext(t, am, testAuthorActivityContext(context.Background()), evt, "agent-a")
				claim, _ := runtimedelivery.ClaimFromContext(ctx)
				snapshot, err := am.writeReceipt(ctx, evt, status, testFailure("receipt_failure"))
				if store.failure != nil && !errors.Is(err, failure) || bus.failure != nil && !errors.Is(err, cleanup) {
					t.Fatalf("lost independent errors: %v", err)
				}
				if phase == "healthy" && err != nil {
					t.Fatal(err)
				}
				if store.settlements.Load() != 1 {
					t.Fatalf("settlements=%d", store.settlements.Load())
				}
				acknowledged := phase != "uncommitted" && phase != "foreign_snapshot"
				if !acknowledged {
					if err == nil || snapshot.DeliveryID != "" || len(bus.retainedContinuations)+len(bus.released) != 0 {
						t.Fatalf("accepted unacknowledged/foreign result: %+v err=%v", snapshot, err)
					}
					return
				}
				if !reflect.DeepEqual(snapshot, store.returned) || !snapshot.MatchesSettlementClaim(claim) {
					t.Fatalf("committed snapshot lost: %+v want=%+v", snapshot, store.returned)
				}
				if store.postCommitRenewals.Load() != 0 {
					t.Fatalf("renewed a committed claim %d times", store.postCommitRenewals.Load())
				}
				if status == ReceiptStatusError {
					if len(bus.retainedContinuations) != 1 || len(bus.released) != 0 || !reflect.DeepEqual(bus.retainedContinuations[0], snapshot) {
						t.Fatalf("retry continuation not exact: %+v", bus)
					}
				} else if len(bus.released) != 1 || bus.released[0] != claim.DeliveryID() || len(bus.retainedContinuations) != 0 {
					t.Fatalf("terminal continuation not released exactly: %+v", bus)
				}
				outcomes, err := store.Outcomes(context.Background(), claim.DeliveryID())
				if err != nil || len(outcomes) != 1 {
					t.Fatalf("durable outcomes=%+v err=%v", outcomes, err)
				}
				// Reusing the settled claim must fail before a second settlement.
				_, err = am.writeReceipt(ctx, evt, status, testFailure("receipt_failure"))
				if err == nil || store.settlements.Load() != 1 {
					t.Fatalf("settled claim replayed: settlements=%d err=%v", store.settlements.Load(), err)
				}
			})
		}
	}
}

func TestProcessEventDoesNotResettleAcknowledgedReceiptError(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "success"
		if failed {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			failure, cleanup := errors.New("postcommit owner failure"), errors.New("continuation failure")
			store := &receiptOutcomeStore{Store: newManagerDeliveryTestStore(t), failure: failure}
			bus := &receiptOutcomeBus{failure: cleanup}
			am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: store})
			agent := &countingFailureAgent{failureReturningAgent: failureReturningAgent{id: "agent-a"}}
			if failed {
				agent.err = errors.New("handler failed")
			}
			evt := eventtest.RunCreatingRootIngress(eventtest.UUID("process-receipt"), "work.requested", "", "", nil, 0, eventtest.UUID("process-receipt-run"), "", events.EventEnvelope{}, time.Time{})
			ctx := managerClaimedDeliveryContext(t, am, testAuthorActivityContext(context.Background()), evt, agent.ID())
			result := am.processEventDetailed(ctx, agent, evt)
			if !errors.Is(result.err, failure) || !errors.Is(result.err, cleanup) || agent.calls != 1 || store.settlements.Load() != 1 {
				t.Fatalf("result=%v handler=%d settlements=%d", result.err, agent.calls, store.settlements.Load())
			}
			result = am.processEventDetailed(ctx, agent, evt)
			if result.err == nil || agent.calls != 1 || store.settlements.Load() != 1 {
				t.Fatalf("settled claim replay: result=%v handler=%d settlements=%d", result.err, agent.calls, store.settlements.Load())
			}
		})
	}
}

// External selected-store tests use this test-only bridge, as the terminal
// retirement tests do, to avoid a manager/store import cycle.
func ProveSelectedStoreReceiptOutcome(t *testing.T, ctx context.Context, selected runtimedelivery.Store, evt events.Event, claimed runtimedelivery.ClaimedObligation, status ReceiptStatus, handoffErr error) {
	t.Helper()
	store := &receiptOutcomeStore{Store: selected, authority: claimed.Snapshot.Authority}
	// Retry settlement has no completion-candidate handoff. Inject its owner
	// return error only after the real selected-store settlement returns.
	if status == ReceiptStatusError {
		store.failure = handoffErr
	}
	bus := &receiptOutcomeBus{}
	if handoffErr != nil {
		bus.failure = errors.New("independent continuation failure")
	}
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: store})
	ctx = runtimedelivery.WithRoute(ctx, claimed.Snapshot.Route)
	ctx = runtimedelivery.WithClaim(ctx, claimed.Claim)
	snapshot, err := am.writeReceipt(ctx, evt, status, testFailure("selected_store_receipt"))
	if handoffErr == nil && err != nil || handoffErr != nil && (!errors.Is(err, handoffErr) || !errors.Is(err, bus.failure)) {
		t.Fatalf("lost receipt errors: %v", err)
	}
	if !snapshot.MatchesSettlementClaim(claimed.Claim) || !reflect.DeepEqual(snapshot, store.returned) {
		t.Fatalf("lost exact committed snapshot: %+v returned=%+v", snapshot, store.returned)
	}
	if store.settlements.Load() != 1 || store.postCommitRenewals.Load() != 0 {
		t.Fatalf("settlements=%d postcommit renewals=%d", store.settlements.Load(), store.postCommitRenewals.Load())
	}
	if status == ReceiptStatusError {
		if len(bus.retainedContinuations) != 1 || len(bus.released) != 0 || !reflect.DeepEqual(snapshot, bus.retainedContinuations[0]) {
			t.Fatalf("retry continuation changed: %+v", bus)
		}
	} else if len(bus.retainedContinuations) != 0 || len(bus.released) != 1 || bus.released[0] != claimed.Claim.DeliveryID() {
		t.Fatalf("terminal continuation changed: %+v", bus)
	}
	durable, err := selected.Snapshot(context.WithoutCancel(ctx), claimed.Claim.DeliveryID())
	if err != nil || !reflect.DeepEqual(snapshot, durable) {
		t.Fatalf("durable snapshot mismatch: %+v err=%v", durable, err)
	}
	outcomes, err := selected.Outcomes(context.WithoutCancel(ctx), claimed.Claim.DeliveryID())
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("durable outcomes=%+v err=%v", outcomes, err)
	}
	_, err = am.writeReceipt(ctx, evt, status, testFailure("selected_store_receipt"))
	if err == nil || store.settlements.Load() != 1 {
		t.Fatalf("same claim resettled: count=%d err=%v", store.settlements.Load(), err)
	}
}
