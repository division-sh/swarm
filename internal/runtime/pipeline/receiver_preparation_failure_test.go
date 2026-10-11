package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type failingReceiverPersistenceReader struct {
	WorkflowTargetPersistenceReader
	failure error
	calls   int
	before  func()
}

func (r *failingReceiverPersistenceReader) LoadWorkflowTargetPersistence(ctx context.Context, route runtimeflowidentity.RunScopedFlowInstance, entity runtimeidentity.EntityID) (WorkflowTargetPersistenceRecord, error) {
	r.calls++
	if r.before != nil {
		r.before()
	}
	if r.failure != nil {
		return WorkflowTargetPersistenceRecord{}, r.failure
	}
	return r.WorkflowTargetPersistenceReader.LoadWorkflowTargetPersistence(ctx, route, entity)
}

type receiverSettlementFaultStore struct {
	runtimedelivery.Store
	renewCalls       atomic.Int32
	claim            atomic.Pointer[runtimedelivery.Claim]
	failRenewAt      int32
	failClaimRenewal bool
	failSettlement   bool
}

func (s *receiverSettlementFaultStore) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	result, err := s.Store.ClaimDelivery(ctx, authority, event, route)
	if claim, acquired := result.Acquired(); acquired {
		s.claim.Store(&claim.Claim)
		if s.failClaimRenewal {
			result.Renewal.Acknowledged = false
		}
	}
	return result, err
}

func (s *receiverSettlementFaultStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.ClaimCommit, error) {
	s.claim.Store(&claim)
	if s.renewCalls.Add(1) == s.failRenewAt {
		return runtimedelivery.ClaimCommit{}, errors.New("injected claim renewal failure")
	}
	return s.Store.RenewClaim(ctx, claim)
}

func (s *receiverSettlementFaultStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	if s.failSettlement {
		return runtimedelivery.Snapshot{}, errors.New("injected failure settlement rollback")
	}
	return s.Store.SettleFailure(ctx, claim, settlement)
}

func VerifyNativeReceiverPreparationUnsettledAuthorityBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []string{"heartbeat_start", "settlement_renewal", "settlement_rollback", "cancellation", "newer_claim"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				fixture, pc, ctx, evt, route := nativeReceiverPreparationFixtureForTest(t, backend, open)
				owner := fixture.Store
				bus := observeNativePipelineDeliveryBusForTest(t, pc)
				id, err := runtimedelivery.DeliveryID(evt.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				faultStore := &receiverSettlementFaultStore{Store: owner}
				pc.deliveryStore = faultStore
				reader := &failingReceiverPersistenceReader{WorkflowTargetPersistenceReader: pc.workflowStore.targetReader, failure: errors.New("injected receiver preparation failure")}
				pc.workflowStore.targetReader = reader
				attemptCtx, cancel := context.WithCancel(withWorkflowNodeDeliveryRoute(ctx, route))
				defer cancel()
				switch fault {
				case "heartbeat_start", "newer_claim":
					faultStore.failClaimRenewal = true
				case "settlement_renewal":
					faultStore.failRenewAt = 1
				case "settlement_rollback":
					faultStore.failSettlement = true
				case "cancellation":
					reader.before = cancel
				}
				if handled, err := pc.dispatchWorkflowNodeEventResult(attemptCtx, evt); err == nil || handled {
					t.Fatalf("uncommitted receiver failure reported handled=%t err=%v", handled, err)
				}
				snapshot, err := owner.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if snapshot.Status != runtimedelivery.StatusInProgress || snapshot.ClaimExpiresAt.IsZero() {
					t.Fatalf("lost accountable claim: %#v", snapshot)
				}
				outcomes, err := owner.Outcomes(ctx, id)
				if err != nil || len(outcomes) != 0 {
					t.Fatalf("invented settlement: %#v %v", outcomes, err)
				}
				acquisition, acquisitionErr := fixture.Continuations.Acquire(id)
				retained := acquisitionErr == nil && acquisition.Validate(id) == nil && acquisition.Disposition() == worklifetime.DeliveryAlreadyOwned
				if !retained || bus.publishedCount() != 0 {
					t.Fatal("unsettled attempt lost continuation or emitted business work")
				}
				// Resume the original immutable claim through the recovered entrypoint;
				// neither the failure nor recovery is allowed to acquire a new target.
				claim := faultStore.claim.Load()
				if claim == nil {
					t.Fatal("actual acquired claim was never observed")
				}
				pc.deliveryStore = owner
				reader.failure, reader.before = nil, nil
				if fault == "newer_claim" {
					failure := runtimefailures.FromError(errors.New("controlled original-claim handoff"), "receiver-test", "handoff")
					if _, err := owner.SettleFailure(ctx, *claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, ReasonCode: "receiver_test_handoff", Failure: &failure.Failure, RetryBase: time.Millisecond, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()}); err != nil {
						t.Fatal(err)
					}
					if err := fixture.RetryEligible(ctx, evt, route); err != nil {
						t.Fatal(err)
					}
					result, err := owner.ClaimDelivery(ctx, snapshot.Authority, evt, route)
					if err != nil {
						t.Fatal(err)
					}
					acquired, ok := result.Acquired()
					if !ok {
						t.Fatalf("newer claim not acquired: %#v", result)
					}
					if acquired.Claim.Same(*claim) {
						t.Fatal("new claim reused old authority")
					}
					if _, err := pc.dispatchWorkflowNodeEventResult(runtimedelivery.WithClaim(withWorkflowNodeDeliveryRoute(ctx, route), *claim), evt); err == nil {
						t.Fatal("stale receiver claim was accepted")
					}
					current, err := owner.Snapshot(ctx, id)
					if err != nil || current.Status != runtimedelivery.StatusInProgress || current.ClaimVersion <= snapshot.ClaimVersion {
						t.Fatalf("stale attempt damaged newer claim: %#v %v", current, err)
					}
					acquisition, acquisitionErr := fixture.Continuations.Acquire(id)
					retained := acquisitionErr == nil && acquisition.Validate(id) == nil && acquisition.Disposition() == worklifetime.DeliveryAlreadyOwned
					if !retained {
						t.Fatal("stale attempt released newer claimant continuation")
					}
					claim = &acquired.Claim
				}
				if _, err := pc.dispatchWorkflowNodeEventResult(runtimedelivery.WithClaim(withWorkflowNodeDeliveryRoute(ctx, route), *claim), evt); err != nil {
					t.Fatal(err)
				}
				after, err := owner.Snapshot(ctx, id)
				if err != nil || after.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("retained claim did not recover: %#v %v", after, err)
				}
				beforeDuplicate, err := owner.Outcomes(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt); err != nil {
					t.Fatal(err)
				}
				afterDuplicate, err := owner.Outcomes(ctx, id)
				if err != nil || len(beforeDuplicate) != len(afterDuplicate) {
					t.Fatal("duplicate execution resettled recovered receiver")
				}
			})
		}
	}
}

func VerifyNativeReceiverPreparationFailureClaimMatrixBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, recovered := range []bool{false, true} {
			for _, transient := range []bool{false, true} {
				name := backend
				if recovered {
					name += "/recovered"
				} else {
					name += "/fresh"
				}
				if transient {
					name += "/target_read_failure"
				} else {
					name += "/missing_target"
				}
				t.Run(name, func(t *testing.T) {
					fixture, pc, ctx, evt, route := nativeReceiverPreparationFixtureForTest(t, backend, open)
					owner := fixture.Store
					bus := observeNativePipelineDeliveryBusForTest(t, pc)
					runID := runtimecorrelation.RunIDFromContext(ctx)
					entityID := runID
					id, err := runtimedelivery.DeliveryID(evt.ID(), route)
					if err != nil {
						t.Fatal(err)
					}
					reader := &failingReceiverPersistenceReader{WorkflowTargetPersistenceReader: pc.workflowStore.targetReader}
					if transient {
						reader.failure = runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "receiver_read_unavailable", "receiver-test", "load_target", nil, errors.New("injected independent target read failure"))
					} else {
						// The delivery was valid at publication; storage disappears afterwards.
						if removed, err := fixture.MissingFields(ctx, runID, entityID); err != nil || removed != 1 {
							t.Fatalf("remove exact receiver fields: %d/%v", removed, err)
						}
						if removed, err := fixture.MissingHeader(ctx, runID, runID); err != nil || removed != 1 {
							t.Fatalf("remove exact receiver header: %d/%v", removed, err)
						}
					}
					pc.workflowStore.targetReader = reader
					attemptCtx := withWorkflowNodeDeliveryRoute(ctx, route)
					if recovered {
						consumeNativePipelineDeliveryCarrierForTest(t, fixture, ctx, id)
						snapshot, err := owner.Snapshot(ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						claimed, err := owner.ClaimDelivery(ctx, snapshot.Authority, evt, route)
						if err != nil {
							t.Fatal(err)
						}
						acquired, ok := claimed.Acquired()
						if !ok {
							t.Fatalf("claim: %#v", claimed)
						}
						attemptCtx = runtimedelivery.WithClaim(attemptCtx, acquired.Claim)
					}
					handled, err := pc.dispatchWorkflowNodeEventResult(attemptCtx, evt)
					if !handled {
						t.Fatalf("claim was not handled: %v", err)
					}
					if (transient || recovered) && err != nil {
						t.Fatalf("accounted failure escaped: %v", err)
					}
					if !transient && !recovered && err == nil {
						t.Fatal("semantic failure was hidden")
					}
					if reader.calls == 0 {
						t.Fatal("receiver preparation was not reached")
					}
					snapshot, err := owner.Snapshot(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					want := runtimedelivery.StatusDeadLetter
					if transient {
						want = runtimedelivery.StatusFailed
					}
					if snapshot.Status != want {
						t.Fatalf("snapshot = %#v, want %s", snapshot, want)
					}
					outcomes, err := owner.Outcomes(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if len(outcomes) != 1 {
						t.Fatalf("outcomes: %#v", outcomes)
					}
					acquisition, acquisitionErr := fixture.Continuations.Acquire(id)
					if acquisitionErr != nil || acquisition.Validate(id) != nil {
						t.Fatalf("observe native failure continuation: %v", acquisitionErr)
					}
					if transient && acquisition.Disposition() != worklifetime.DeliveryAlreadyOwned {
						t.Fatal("retry has no retained native attempt")
					}
					if !transient && acquisition.Disposition() != worklifetime.DeliveryTerminallyFenced {
						t.Fatal("terminal receiver failure retained executable continuation authority")
					}
					if got := bus.publishedCount(); got != 0 {
						t.Fatalf("failed receiver emitted %d events", got)
					}
					if transient {
						reader.failure = nil
						if err := fixture.RetryEligible(ctx, evt, route); err != nil {
							t.Fatal(err)
						}
						recoverNativePipelineRetryForTest(t, fixture, ctx)
						if _, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt); err != nil {
							t.Fatal(err)
						}
						snapshot, err = owner.Snapshot(ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						if snapshot.Status != runtimedelivery.StatusDelivered {
							t.Fatalf("retry did not execute exact original receiver: %#v", snapshot)
						}
					}
				})
			}
		}
	}
}
