package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
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
	renewCalls     atomic.Int32
	claim          atomic.Pointer[runtimedelivery.Claim]
	failRenewAt    int32
	failSettlement bool
}

func (s *receiverSettlementFaultStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	s.claim.Store(&claim)
	if s.renewCalls.Add(1) == s.failRenewAt {
		return runtimedelivery.Snapshot{}, errors.New("injected claim renewal failure")
	}
	return s.Store.RenewClaim(ctx, claim)
}

func (s *receiverSettlementFaultStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	if s.failSettlement {
		return runtimedelivery.Snapshot{}, errors.New("injected failure settlement rollback")
	}
	return s.Store.SettleFailure(ctx, claim, settlement)
}

func TestReceiverPreparationUnsettledAuthorityBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		for _, fault := range []string{"heartbeat_start", "settlement_renewal", "settlement_rollback", "cancellation", "newer_claim"} {
			t.Run(backend.name+"/"+fault, func(t *testing.T) {
				store, ctx := backend.open(t)
				pc, bus := newDeliveryAuthorityCoordinator(t, store.testDB())
				pc.workflowStore = store
				owner := newPipelineTestDeliveryOwnerForDB(t, store.testDB())
				pc.deliveryStore = owner
				configurePipelineTestDeliveryOwner(t, pc)
				runID := runtimecorrelation.RunIDFromContext(ctx)
				entityID := uuid.NewString()
				evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "source.evt", "src", "", []byte("{}"), 0, runID, "", handlerTestWorkflowEnvelope(".", runID, entityID), time.Now().UTC())
				dialect := authoractivityfixture.DialectPostgres
				if store.isSQLite() {
					dialect = authoractivityfixture.DialectSQLite
				}
				seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
				if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: "v-test", CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{}})); err != nil {
					t.Fatal(err)
				}
				node := pipelineNode(t, ".", "node-a")
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
				if err := owner.commitInitial(ctx, evt, route); err != nil {
					t.Fatal(err)
				}
				id, err := runtimedelivery.DeliveryID(evt.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				faultStore := &receiverSettlementFaultStore{Store: owner}
				pc.deliveryStore = faultStore
				reader := &failingReceiverPersistenceReader{WorkflowTargetPersistenceReader: store.targetReader, failure: errors.New("injected receiver preparation failure")}
				store.targetReader = reader
				attemptCtx, cancel := context.WithCancel(withWorkflowNodeDeliveryRoute(ctx, route))
				defer cancel()
				switch fault {
				case "heartbeat_start", "newer_claim":
					faultStore.failRenewAt = 1
				case "settlement_renewal":
					faultStore.failRenewAt = 2
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
				bus.deliveryContinuations.mu.Lock()
				_, retained := bus.deliveryContinuations.held[id]
				bus.deliveryContinuations.mu.Unlock()
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
					if _, err := owner.SettleFailure(ctx, *claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, ReasonCode: "receiver_test_handoff", Failure: &failure.Failure, RetryBase: time.Millisecond, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection()}); err != nil {
						t.Fatal(err)
					}
					if err := owner.makeRetryEligible(ctx, id); err != nil {
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
					bus.deliveryContinuations.mu.Lock()
					_, retained := bus.deliveryContinuations.held[id]
					bus.deliveryContinuations.mu.Unlock()
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

func TestReceiverPreparationFailureClaimMatrixBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		for _, recovered := range []bool{false, true} {
			for _, transient := range []bool{false, true} {
				name := backend.name
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
					store, ctx := backend.open(t)
					pc, bus := newDeliveryAuthorityCoordinator(t, store.testDB())
					pc.workflowStore = store
					owner := newPipelineTestDeliveryOwnerForDB(t, store.testDB())
					pc.deliveryStore = owner
					configurePipelineTestDeliveryOwner(t, pc)
					runID := runtimecorrelation.RunIDFromContext(ctx)
					entityID := uuid.NewString()
					evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "source.evt", "src", "", []byte("{}"), 0, runID, "", handlerTestWorkflowEnvelope(".", runID, entityID), time.Now().UTC())
					dialect := authoractivityfixture.DialectPostgres
					if store.isSQLite() {
						dialect = authoractivityfixture.DialectSQLite
					}
					seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
					if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
						InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: "v-test",
						CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{},
					})); err != nil {
						t.Fatal(err)
					}
					node := pipelineNode(t, ".", "node-a")
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
					if err := owner.commitInitial(ctx, evt, route); err != nil {
						t.Fatal(err)
					}
					id, err := runtimedelivery.DeliveryID(evt.ID(), route)
					if err != nil {
						t.Fatal(err)
					}
					reader := &failingReceiverPersistenceReader{WorkflowTargetPersistenceReader: store.targetReader}
					if transient {
						reader.failure = runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "receiver_read_unavailable", "receiver-test", "load_target", nil, errors.New("injected independent target read failure"))
					} else {
						// The delivery was valid at publication; storage disappears afterwards.
						if _, err := store.testDB().Exec(`DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID); err != nil {
							t.Fatal(err)
						}
						if _, err := store.testDB().Exec(`DELETE FROM flow_instances WHERE instance_id=$1`, runID); err != nil {
							t.Fatal(err)
						}
					}
					store.targetReader = reader
					attemptCtx := withWorkflowNodeDeliveryRoute(ctx, route)
					if recovered {
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
					bus.deliveryContinuations.mu.Lock()
					held, present := bus.deliveryContinuations.held[id]
					bus.deliveryContinuations.mu.Unlock()
					if transient && (!present || !held) {
						t.Fatal("retry has no retained continuation authority")
					}
					if !transient && present {
						t.Fatal("terminal receiver failure retained its continuation")
					}
					if got := bus.publishedCount(); got != 0 {
						t.Fatalf("failed receiver emitted %d events", got)
					}
					if transient {
						reader.failure = nil
						if err := owner.makeRetryEligible(ctx, id); err != nil {
							t.Fatal(err)
						}
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
