package pipeline

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	delivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type noopSettlementEvidenceStore struct {
	delivery.Store
	claim       delivery.Claim
	renewal     delivery.Snapshot
	snapshot    delivery.Snapshot
	ack         bool
	failure     error
	renewals    atomic.Int32
	settlements atomic.Int32
}

func (s *noopSettlementEvidenceStore) RenewClaim(context.Context, delivery.Claim) (delivery.ClaimCommit, error) {
	s.renewals.Add(1)
	return delivery.ClaimCommit{Snapshot: s.renewal, Acknowledged: true}, nil
}

func (s *noopSettlementEvidenceStore) SettleWorkflowNodeSuccess(_ context.Context, claim delivery.Claim, effects []string, _ time.Duration, _ delivery.HandlerRuleSelectionFact) (delivery.ClaimCommit, error) {
	s.settlements.Add(1)
	if !claim.Same(s.claim) || len(effects) != 1 || effects[0] != "handler_completed" {
		return delivery.ClaimCommit{}, errors.New("wrong settlement claim or effects")
	}
	return delivery.ClaimCommit{Snapshot: s.snapshot, Acknowledged: s.ack}, s.failure
}

type noopSettlementContinuationOwner struct {
	WorkflowDeliveryRuntime
	expected string
	releases atomic.Int32
	failure  error
}

func (o *noopSettlementContinuationOwner) ReleaseDeliveryContinuation(id string) error {
	if id != o.expected {
		return errors.New("foreign continuation")
	}
	o.releases.Add(1)
	if o.WorkflowDeliveryRuntime != nil {
		return errors.Join(o.WorkflowDeliveryRuntime.ReleaseDeliveryContinuation(id), o.failure)
	}
	return o.failure
}

type observedNoopSettlementStore struct {
	delivery.Store
	renewals    atomic.Int32
	settlements atomic.Int32
}

func (s *observedNoopSettlementStore) RenewClaim(ctx context.Context, claim delivery.Claim) (delivery.ClaimCommit, error) {
	s.renewals.Add(1)
	return s.Store.RenewClaim(ctx, claim)
}

func (s *observedNoopSettlementStore) SettleWorkflowNodeSuccess(ctx context.Context, claim delivery.Claim, effects []string, duration time.Duration, selection delivery.HandlerRuleSelectionFact) (delivery.ClaimCommit, error) {
	s.settlements.Add(1)
	return s.Store.SettleWorkflowNodeSuccess(ctx, claim, effects, duration, selection)
}

func TestObsoleteJoinOccurrenceUsesAtomicNodeSettlementBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				h := newExactWorkflowJoinHarness(t, storeCase, flowID, "awaiting", []any{"a", "b"})
				schedule := h.armInitial()
				h.transition("dispatching", "manual.abort")
				before := h.instance()
				h.restart()
				nodes, err := LoadWorkflowNodes(h.source)
				if err != nil {
					t.Fatal(err)
				}
				h.pc.module.(*pipelineFixtureWorkflowModule).workflowNodes = nodes
				late := h.scheduleEvent(schedule, "obsolete-join-settlement")
				recipient, target, _, _, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(h.source, late)
				if err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: recipient, Target: events.MustExistingEntityTarget(target)}
				ctx, err := persistWorkflowJoinPublicationForTest(t, h.pc, h.ctx, late, route, false)
				if err != nil {
					t.Fatal(err)
				}
				ctx, err = h.pc.receiverExecution.Bind(ctx, executionmode.Live)
				if err != nil {
					t.Fatal(err)
				}
				id, err := delivery.DeliveryID(late.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				store := &observedNoopSettlementStore{Store: h.pc.deliveryStore}
				continuations := &noopSettlementContinuationOwner{WorkflowDeliveryRuntime: h.pc.deliveryRuntime, expected: id}
				h.pc.deliveryStore, h.pc.deliveryRuntime = store, continuations
				carrier, err := events.NewDeliveryEvent(late, route)
				if err != nil {
					t.Fatal(err)
				}
				continued, emitted, outcome, err := h.pc.InterceptDeliveryRoute(ctx, carrier, route)
				if err != nil || !continued || len(emitted) != 0 || outcome.Committed || !outcome.ContinueDispatch() {
					t.Fatalf("obsolete join: continued=%t emitted=%d outcome=%#v err=%v", continued, len(emitted), outcome, err)
				}
				snapshot, err := store.Snapshot(ctx, id)
				if err != nil || snapshot.Status != delivery.StatusDelivered {
					t.Fatalf("obsolete join settlement: snapshot=%#v err=%v", snapshot, err)
				}
				if store.renewals.Load() != 0 || store.settlements.Load() != 1 || continuations.releases.Load() != 1 {
					t.Fatalf("obsolete join ownership: renewals=%d settlements=%d releases=%d", store.renewals.Load(), store.settlements.Load(), continuations.releases.Load())
				}
				if after := h.instance(); !reflect.DeepEqual(before, after) {
					t.Fatalf("obsolete join changed workflow state or header\nbefore=%#v\nafter=%#v", before, after)
				}
				_, cancellations := committedWorkflowSchedulesForTest(t, h.store)
				if len(cancellations) != 1 || cancellations[0].Command.ScheduleKey != schedule.Command.ScheduleKey {
					t.Fatalf("obsolete join changed schedule retirement: %#v", cancellations)
				}
			})
		}
	}
}

func TestWorkflowNodeNoopSettlementConsumesOnlyExactAcknowledgedEvidence(t *testing.T) {
	for _, phase := range []string{"healthy", "cleanup_failed", "ack_lost", "missing_ack_with_snapshot", "wrong_claim", "wrong_status", "continuation_failed"} {
		t.Run(phase, func(t *testing.T) {
			event, route, id := workflowNodeCarrierTestEventAndRoute(t)
			node := pipelineNode(t, "", "node-a")
			identity, err := route.Identity()
			if err != nil {
				t.Fatal(err)
			}
			claim, err := delivery.AdmitPersistedClaim(id, event.RunID(), events.EncodeDeliveryRouteIdentity(identity), uuid.NewString(), 1, delivery.SubscriberNode, node.Key())
			if err != nil {
				t.Fatal(err)
			}
			snapshot := delivery.Snapshot{DeliveryID: id, EventID: event.ID(), Route: route, RunID: claim.RunID(), RouteIdentity: identity, ClaimVersion: claim.Version(), SubscriberClass: claim.SubscriberClass(), SubscriberID: claim.SubscriberID(), Status: delivery.StatusInProgress, UpdatedAt: time.Now().UTC(), ClaimExpiresAt: time.Now().UTC().Add(delivery.DefaultLeaseTTL)}
			store := &noopSettlementEvidenceStore{claim: claim, renewal: snapshot, snapshot: snapshot, ack: true}
			owner := pipelineTestWorkOwner(t)
			heartbeat, err := delivery.StartClaimHeartbeatFromClaim(context.Background(), owner, store, claim, delivery.ClaimCommit{Snapshot: snapshot, Acknowledged: true})
			if err != nil {
				t.Fatal(err)
			}
			defer heartbeat.Stop()
			store.snapshot.Status = delivery.StatusDelivered
			store.snapshot.FinalSelection = delivery.PresentSelection(delivery.NotApplicableHandlerRuleSelection())
			failure := errors.New("no-op " + phase)
			continuations := &noopSettlementContinuationOwner{expected: id}
			switch phase {
			case "cleanup_failed":
				store.failure = failure
			case "ack_lost":
				store.ack, store.snapshot, store.failure = false, delivery.Snapshot{}, failure
			case "missing_ack_with_snapshot":
				store.ack = false
			case "wrong_claim":
				store.snapshot.ClaimVersion++
			case "wrong_status":
				store.snapshot.Status = delivery.StatusInProgress
			case "continuation_failed":
				continuations.failure = failure
			}
			pc := &PipelineCoordinator{deliveryStore: store, deliveryRuntime: continuations}
			handled, finishErr, probeErr := pc.finishClaimedNodeAttempt(claimedNodeAttempt{ctx: heartbeat.Context(), statusCtx: context.Background(), node: node, event: event, claim: claim, heartbeat: heartbeat, started: time.Now(), result: contractHandlerExecutionResult{Handled: true, RuleSelection: handlerselection.Resolved(handlerselection.NotApplicable())}})
			if !handled || probeErr != nil || (finishErr != nil) != (phase != "healthy") {
				t.Fatalf("no-op result: handled=%t finish=%v probe=%v", handled, finishErr, probeErr)
			}
			wantReleased := int32(0)
			if phase == "healthy" || phase == "cleanup_failed" || phase == "continuation_failed" {
				wantReleased = 1
			}
			wantRenewals := int32(1) - wantReleased
			if store.renewals.Load() != wantRenewals || store.settlements.Load() != 1 {
				t.Fatalf("renewals=%d settlements=%d; want %d/1", store.renewals.Load(), store.settlements.Load(), wantRenewals)
			}
			if continuations.releases.Load() != wantReleased {
				t.Fatalf("continuations released=%d want=%d", continuations.releases.Load(), wantReleased)
			}
			if err := heartbeat.Stop(); err != nil || continuations.releases.Load() != wantReleased {
				t.Fatalf("joined heartbeat repeated continuation disposal: %v", err)
			}
			if phase == "cleanup_failed" || phase == "ack_lost" || phase == "continuation_failed" {
				if !errors.Is(finishErr, failure) {
					t.Fatalf("lost cleanup/commit evidence: %v", finishErr)
				}
			}
			if err := owner.WaitForQuiescence(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
