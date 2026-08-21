package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	deliveryfixture "github.com/division-sh/swarm/internal/store/testutil/deliveryfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type deliveryLifecycleConformanceBackend struct {
	name     string
	store    runtimedelivery.Store
	restart  runtimedelivery.Store
	selected any
	db       *sql.DB
	postgres bool
}

func deliveryLifecycleConformanceRoute(t testing.TB, subscriberType, subscriberID string) events.DeliveryRoute {
	t.Helper()
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(conformanceNode(t, "", subscriberID))}
	if subscriberType == string(runtimedelivery.SubscriberAgent) {
		route.Recipient = events.MustAgentDeliveryRecipient(subscriberID)
		route.AgentIdentity = agentidentitytest.RootRuntime(t, subscriberID, "delivery-lifecycle-conformance")
	} else {
		route.Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "delivery-lifecycle-conformance"})
	}
	return route
}

func claimDeliveryResult(
	ctx context.Context,
	selected runtimedelivery.Store,
	event events.Event,
	route events.DeliveryRoute,
) (runtimedelivery.ClaimResult, error) {
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	snapshot, err := selected.Snapshot(ctx, deliveryID)
	if err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	return selected.ClaimDelivery(ctx, snapshot.Authority, event, route)
}

func TestExecutableDeliveryLifecycleParity(t *testing.T) {
	for _, backend := range deliveryLifecycleConformanceBackends(t) {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			t.Run("exact_route_claim_settlement_and_outcome", func(t *testing.T) {
				event := deliveryLifecycleEvent("exact-" + backend.name)
				agent := deliveryLifecycleConformanceRoute(t, "agent", "agent-a")
				sibling := agent
				sibling.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:delivery-lifecycle-sibling"}}
				node := deliveryLifecycleConformanceRoute(t, "node", "node-a")
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{agent, sibling, node}, runtimepipelineobligation.ScopeSubscribed)

				agentProof, err := backend.store.ProveHandoff(ctx, event.ID(), agent)
				if err != nil {
					t.Fatalf("prove agent handoff: %v", err)
				}
				siblingProof, err := backend.store.ProveHandoff(ctx, event.ID(), sibling)
				if err != nil {
					t.Fatalf("prove sibling handoff: %v", err)
				}
				if agentProof.DeliveryID() == siblingProof.DeliveryID() {
					t.Fatal("distinct exact routes collapsed to one delivery obligation")
				}

				claimed, err := storetest.ClaimDelivery(ctx, backend.store, event, agent)
				if err != nil {
					t.Fatalf("claim agent delivery: %v", err)
				}
				if claimed.Snapshot.Status != runtimedelivery.StatusInProgress || claimed.Snapshot.MaxRetries != runtimedelivery.AgentMaxRetries {
					t.Fatalf("claimed agent snapshot = %#v", claimed.Snapshot)
				}
				secondClaim, err := claimDeliveryResult(ctx, backend.store, event, agent)
				if err != nil || secondClaim.Disposition != runtimedelivery.ClaimBusy {
					t.Fatalf("second live claim = %#v, err=%v; want busy", secondClaim, err)
				}
				sessionID := uuid.NewString()
				seedDeliveryAgentSession(t, ctx, backend, sessionID, event.RunID(), agent.Recipient.ID())
				bound, err := backend.store.BindAgentSession(ctx, claimed.Claim, sessionID)
				if err != nil {
					t.Fatalf("bind agent session: %v", err)
				}
				if bound.ActiveSessionID != sessionID {
					t.Fatalf("bound session = %q, want %q", bound.ActiveSessionID, sessionID)
				}
				settled, err := backend.store.SettleSuccess(ctx, claimed.Claim, []string{"message.sent"}, 25*time.Millisecond)
				if err != nil {
					t.Fatalf("settle agent success: %v", err)
				}
				if settled.Status != runtimedelivery.StatusDelivered || !settled.Terminal() {
					t.Fatalf("settled status = %q", settled.Status)
				}
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("stale settlement error = %v, want ErrConflict", err)
				}
				outcomes, err := backend.store.Outcomes(ctx, agentProof.DeliveryID())
				if err != nil {
					t.Fatalf("read exact outcomes: %v", err)
				}
				if len(outcomes) != 1 || outcomes[0].Outcome != "delivered" || outcomes[0].ClaimVersion != claimed.Claim.Version() || len(outcomes[0].SideEffects) != 1 || outcomes[0].SideEffects[0] != "message.sent" {
					t.Fatalf("agent outcomes = %#v", outcomes)
				}

				// Exact duplicate event admission cannot mint, replace, or reset obligations.
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{agent, sibling, node}, runtimepipelineobligation.ScopeSubscribed)
				afterDuplicate, err := backend.store.Snapshot(ctx, agentProof.DeliveryID())
				if err != nil {
					t.Fatalf("snapshot after exact duplicate: %v", err)
				}
				if afterDuplicate.Status != runtimedelivery.StatusDelivered || afterDuplicate.ClaimVersion != claimed.Claim.Version() {
					t.Fatalf("exact duplicate changed lifecycle: %#v", afterDuplicate)
				}
			})

			t.Run("delivery_continuation_restart_preserves_exact_payload_bytes", func(t *testing.T) {
				payload := json.RawMessage("{\n  \"numeric\": 1.0, \"ordered\": {\"b\": 2, \"a\": 1}\n}")
				event := deliveryLifecycleEventWithPayload("continuation-payload-"+backend.name, payload)
				route := deliveryLifecycleConformanceRoute(t, "agent", "continuation-payload")
				storetest.CommitSemanticEventWithInitialFacts(
					t,
					ctx,
					backend.selected,
					event,
					[]events.DeliveryRoute{route},
					runtimepipelineobligation.ScopeSubscribed,
					storetest.AcknowledgedPipelineDisposition(),
				)
				deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := backend.store.Snapshot(ctx, deliveryID)
				if err != nil {
					t.Fatalf("load continuation authority: %v", err)
				}
				page, err := backend.restart.ScanDeliveryContinuations(
					ctx,
					snapshot.Authority,
					runtimedelivery.ContinuationCursor{},
					10,
				)
				if err != nil {
					t.Fatalf("scan delivery continuations after restart: %v", err)
				}
				var restored *runtimedelivery.ContinuationItem
				for i := range page.Items {
					if page.Items[i].DeliveryID == deliveryID {
						restored = &page.Items[i]
						break
					}
				}
				if restored == nil {
					t.Fatalf("continuation page = %#v, want delivery %s", page, deliveryID)
				}
				if restored.Disposition != runtimedelivery.ClaimAcquired {
					t.Fatalf("continuation disposition = %s, want acquired", restored.Disposition)
				}
				if !bytes.Equal(restored.Event.Payload(), payload) {
					t.Fatalf("continuation payload = %q, want exact admitted bytes %q", restored.Event.Payload(), payload)
				}
			})

			t.Run("normal_authority_activation_reclaims_predecessor_attempt", func(t *testing.T) {
				event := deliveryLifecycleEvent("activation-reclaim-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, "agent", "activation-reclaim-agent")
				storetest.CommitSemanticEventWithInitialFacts(
					t,
					ctx,
					backend.selected,
					event,
					[]events.DeliveryRoute{route},
					runtimepipelineobligation.ScopeSubscribed,
					storetest.AcknowledgedPipelineDisposition(),
				)
				claimed, err := storetest.ClaimDelivery(ctx, backend.store, event, route)
				if err != nil {
					t.Fatalf("claim predecessor delivery: %v", err)
				}
				predecessor := claimed.Snapshot.Authority
				successor, err := runtimedelivery.NewNormalExecutionAuthority(
					predecessor.BundleSource(),
					"delivery-recovery-successor-"+backend.name,
					predecessor.Generation()+1,
				)
				if err != nil {
					t.Fatalf("construct successor authority: %v", err)
				}
				if err := backend.restart.ActivateDeliveryAuthority(ctx, successor); err != nil {
					t.Fatalf("activate successor authority: %v", err)
				}

				deadline := time.Now().Add(time.Second)
				for {
					observation, err := backend.restart.ObserveDeliveryContinuation(ctx, successor, claimed.Snapshot.DeliveryID)
					if err != nil {
						t.Fatalf("observe successor continuation: %v", err)
					}
					if observation.Disposition == runtimedelivery.ClaimReclaimable {
						break
					}
					if observation.Disposition != runtimedelivery.ClaimBusy {
						t.Fatalf("successor observation = %s, want busy or reclaimable", observation.Disposition)
					}
					after, ok := observation.Wake.After()
					if !ok || after > time.Until(deadline) {
						t.Fatalf("successor busy wake = %s, %v; want bounded store wake", after, ok)
					}
					timer := time.NewTimer(after)
					select {
					case <-timer.C:
					case <-time.After(time.Until(deadline)):
						timer.Stop()
						t.Fatal("successor claim did not become reclaimable")
					}
				}

				reclaimed, err := backend.restart.ClaimDelivery(ctx, successor, event, route)
				if err != nil {
					t.Fatalf("reclaim successor delivery: %v", err)
				}
				if _, ok := reclaimed.Acquired(); !ok || reclaimed.Previous != runtimedelivery.ClaimReclaimable {
					t.Fatalf("successor claim = %#v, want acquired from reclaimable", reclaimed)
				}
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("predecessor settlement error = %v, want stale claim conflict", err)
				}
				if _, err := backend.restart.SettleSuccess(ctx, reclaimed.Claimed.Claim, nil, 0); err != nil {
					t.Fatalf("settle successor delivery: %v", err)
				}
			})

			t.Run("class_retry_budgets_are_structural", func(t *testing.T) {
				assertDeliveryRetryBudget(t, ctx, backend, runtimedelivery.SubscriberAgent, "retry-agent", runtimedelivery.AgentMaxRetries)
				assertDeliveryRetryBudget(t, ctx, backend, runtimedelivery.SubscriberNode, "retry-node", runtimedelivery.NodeMaxRetries)
			})

			t.Run("closed_claim_and_cursor_disposition_matrix", func(t *testing.T) {
				pendingEvent := deliveryLifecycleEvent("claim-matrix-pending-" + backend.name)
				pendingRoute := deliveryLifecycleConformanceRoute(t, "agent", "claim-matrix-pending")
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, pendingEvent, []events.DeliveryRoute{pendingRoute}, runtimepipelineobligation.ScopeSubscribed)
				pendingID, err := runtimedelivery.DeliveryID(pendingEvent.ID(), pendingRoute)
				if err != nil {
					t.Fatal(err)
				}
				pending, err := backend.store.Snapshot(ctx, pendingID)
				if err != nil {
					t.Fatal(err)
				}

				absentEvent := deliveryLifecycleEvent("claim-matrix-absent-" + backend.name)
				absent, err := backend.store.ClaimDelivery(ctx, pending.Authority, absentEvent, pendingRoute)
				if err != nil || absent.Disposition != runtimedelivery.ClaimAbsent {
					t.Fatalf("absent claim = %#v, err=%v", absent, err)
				}

				wrongAuthority, err := runtimedelivery.NewNormalExecutionAuthority(
					pending.Authority.BundleSource(),
					"wrong-authority-"+backend.name,
					pending.Authority.Generation(),
				)
				if err != nil {
					t.Fatal(err)
				}
				wrong, err := backend.store.ClaimDelivery(ctx, wrongAuthority, pendingEvent, pendingRoute)
				if err != nil || wrong.Disposition != runtimedelivery.ClaimWrongAuthority {
					t.Fatalf("wrong-authority claim = %#v, err=%v", wrong, err)
				}

				acquired, err := backend.store.ClaimDelivery(ctx, pending.Authority, pendingEvent, pendingRoute)
				if err != nil || acquired.Disposition != runtimedelivery.ClaimAcquired {
					t.Fatalf("pending claim = %#v, err=%v", acquired, err)
				}
				busy, err := backend.restart.ClaimDelivery(ctx, pending.Authority, pendingEvent, pendingRoute)
				if err != nil || busy.Disposition != runtimedelivery.ClaimBusy {
					t.Fatalf("busy claim = %#v, err=%v", busy, err)
				}
				expireDeliveryClaimForConformance(t, ctx, backend, pendingID)
				reclaimed, err := backend.restart.ClaimDelivery(ctx, pending.Authority, pendingEvent, pendingRoute)
				if err != nil || reclaimed.Disposition != runtimedelivery.ClaimAcquired ||
					reclaimed.Previous != runtimedelivery.ClaimReclaimable {
					t.Fatalf("reclaim claim = %#v, err=%v", reclaimed, err)
				}
				reclaimedObligation, ok := reclaimed.Acquired()
				if !ok {
					t.Fatalf("reclaim result has no exact claim: %#v", reclaimed)
				}
				delivered, err := backend.restart.SettleSuccess(ctx, reclaimedObligation.Claim, nil, 0)
				if err != nil || !delivered.Terminal() {
					t.Fatalf("settle reclaimed delivery = %#v, err=%v", delivered, err)
				}
				terminal, err := backend.store.ClaimDelivery(ctx, pending.Authority, pendingEvent, pendingRoute)
				if err != nil || terminal.Disposition != runtimedelivery.ClaimTerminal {
					t.Fatalf("terminal claim = %#v, err=%v", terminal, err)
				}

				deferredEvent := deliveryLifecycleEvent("claim-matrix-deferred-" + backend.name)
				deferredRoute := deliveryLifecycleConformanceRoute(t, "agent", "claim-matrix-deferred")
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, deferredEvent, []events.DeliveryRoute{deferredRoute}, runtimepipelineobligation.ScopeSubscribed)
				deferredClaim, err := storetest.ClaimDelivery(ctx, backend.store, deferredEvent, deferredRoute)
				if err != nil {
					t.Fatal(err)
				}
				deferredSnapshot, err := backend.store.SettleFailure(ctx, deferredClaim.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureRetry,
					Failure:     testFailure("handler_failed"),
					RetryBase:   time.Hour,
				})
				if err != nil || deferredSnapshot.Status != runtimedelivery.StatusFailed {
					t.Fatalf("schedule deferred retry = %#v, err=%v", deferredSnapshot, err)
				}
				deferred, err := backend.restart.ClaimDelivery(ctx, deferredSnapshot.Authority, deferredEvent, deferredRoute)
				if err != nil || deferred.Disposition != runtimedelivery.ClaimDeferred {
					t.Fatalf("deferred claim = %#v, err=%v", deferred, err)
				}
				beforeHandoff, err := backend.store.ScanDeliveryContinuations(
					ctx,
					deferredSnapshot.Authority,
					runtimedelivery.ContinuationCursor{},
					1,
				)
				if err != nil || !beforeHandoff.Exhausted || len(beforeHandoff.Items) != 0 {
					t.Fatalf("pre-pipeline-receipt continuation page = %#v, err=%v; want no coordinator-owned work", beforeHandoff, err)
				}
				acknowledgeDeliveryLifecyclePipelineEvent(t, ctx, backend.selected, deferredEvent.ID())

				invalidEvent := deliveryLifecycleEvent("claim-matrix-invalid-" + backend.name)
				invalidRoute := deliveryLifecycleConformanceRoute(t, "agent", "claim-matrix-invalid")
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, invalidEvent, []events.DeliveryRoute{invalidRoute}, runtimepipelineobligation.ScopeSubscribed)
				invalidID, err := runtimedelivery.DeliveryID(invalidEvent.ID(), invalidRoute)
				if err != nil {
					t.Fatal(err)
				}
				corruptQuery := `UPDATE event_deliveries SET delivery_target_route='{"flow_id":7}'::jsonb WHERE delivery_id=$1::uuid`
				if !backend.postgres {
					corruptQuery = `UPDATE event_deliveries SET delivery_target_route='{"flow_id":7}' WHERE delivery_id=?`
				}
				if _, err := backend.db.ExecContext(ctx, corruptQuery, invalidID); err != nil {
					t.Fatalf("seed structurally invalid delivery: %v", err)
				}
				invalid, err := backend.store.ClaimDelivery(ctx, pending.Authority, invalidEvent, invalidRoute)
				if err != nil || invalid.Disposition != runtimedelivery.ClaimInvariantInvalid ||
					!errors.Is(invalid.Invariant, runtimedelivery.ErrConflict) {
					t.Fatalf("invariant-invalid claim = %#v, err=%v", invalid, err)
				}

				firstPage, err := backend.store.ScanDeliveryContinuations(
					ctx,
					deferredSnapshot.Authority,
					runtimedelivery.ContinuationCursor{},
					1,
				)
				if err != nil || !firstPage.Exhausted || len(firstPage.Items) != 1 ||
					firstPage.Items[0].Disposition != runtimedelivery.ClaimDeferred {
					t.Fatalf("explicit exhausted continuation page = %#v, err=%v", firstPage, err)
				}
				if _, err := backend.store.ScanDeliveryContinuations(ctx, wrongAuthority, firstPage.Next, 1); err == nil {
					t.Fatal("continuation cursor crossed execution authority")
				}
			})

			t.Run("recovery_inventory_and_opaque_wake_are_store_owned", func(t *testing.T) {
				event := deliveryLifecycleEvent("recovery-inventory-" + backend.name)
				pendingRoute := deliveryLifecycleConformanceRoute(t, "agent", "inventory-pending")
				failedRoute := deliveryLifecycleConformanceRoute(t, "agent", "inventory-failed")
				busyRoute := deliveryLifecycleConformanceRoute(t, "agent", "inventory-busy")
				storetest.CommitSemanticEventWithInitialFacts(
					t,
					ctx,
					backend.selected,
					event,
					[]events.DeliveryRoute{pendingRoute, failedRoute, busyRoute},
					runtimepipelineobligation.ScopeSubscribed,
					storetest.AcknowledgedPipelineDisposition(),
				)

				pendingID, err := runtimedelivery.DeliveryID(event.ID(), pendingRoute)
				if err != nil {
					t.Fatal(err)
				}
				pendingSnapshot, err := backend.store.Snapshot(ctx, pendingID)
				if err != nil {
					t.Fatalf("snapshot pending inventory route: %v", err)
				}
				source := pendingSnapshot.Authority.BundleSource()
				before, err := backend.store.InspectDeliveryRecovery(ctx, source)
				if err != nil {
					t.Fatalf("inspect initial delivery recovery inventory: %v", err)
				}

				failedClaim, err := storetest.ClaimDelivery(ctx, backend.store, event, failedRoute)
				if err != nil {
					t.Fatalf("claim failed inventory route: %v", err)
				}
				failedSnapshot, err := backend.store.SettleFailure(ctx, failedClaim.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureRetry,
					Failure:     testFailure("inventory_retry"),
					RetryBase:   10 * time.Second,
				})
				if err != nil {
					t.Fatalf("schedule inventory retry: %v", err)
				}
				busyClaim, err := storetest.ClaimDelivery(ctx, backend.store, event, busyRoute)
				if err != nil {
					t.Fatalf("claim busy inventory route: %v", err)
				}

				after, err := backend.restart.InspectDeliveryRecovery(ctx, source)
				if err != nil {
					t.Fatalf("inspect transitioned delivery recovery inventory: %v", err)
				}
				want := runtimedelivery.RecoveryInventory{
					Pending:    before.Pending - 2,
					Failed:     before.Failed + 1,
					InProgress: before.InProgress + 1,
				}
				if after != want {
					t.Fatalf("delivery recovery inventory = %#v, want %#v", after, want)
				}

				foreignSource, err := runtimecorrelation.NewEphemeralBundleSourceFact(
					"bundle-v1:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				)
				if err != nil {
					t.Fatal(err)
				}
				foreign, err := backend.restart.InspectDeliveryRecovery(ctx, foreignSource)
				if err != nil {
					t.Fatalf("inspect foreign delivery recovery inventory: %v", err)
				}
				if foreign.HasWork() {
					t.Fatalf("foreign delivery recovery inventory = %#v, want empty exact scope", foreign)
				}

				page, err := backend.restart.ScanDeliveryContinuations(
					ctx,
					pendingSnapshot.Authority,
					runtimedelivery.ContinuationCursor{},
					10,
				)
				if err != nil {
					t.Fatalf("scan exact delivery recovery scope: %v", err)
				}
				if !page.Exhausted || len(page.Items) != 3 {
					t.Fatalf("delivery recovery page = %#v, want three exact exhausted items", page)
				}
				wantDisposition := map[string]runtimedelivery.ClaimDisposition{
					pendingID:                     runtimedelivery.ClaimAcquired,
					failedSnapshot.DeliveryID:     runtimedelivery.ClaimDeferred,
					busyClaim.Snapshot.DeliveryID: runtimedelivery.ClaimBusy,
				}
				for _, item := range page.Items {
					want, ok := wantDisposition[item.DeliveryID]
					if !ok || item.Disposition != want {
						t.Fatalf("continuation item %s disposition = %s, want %s", item.DeliveryID, item.Disposition, want)
					}
					after, present := item.Wake.After()
					if want == runtimedelivery.ClaimDeferred || want == runtimedelivery.ClaimBusy {
						if !present || after <= 0 {
							t.Fatalf("continuation item %s wake = %s, %v; want positive store-issued delay", item.DeliveryID, after, present)
						}
					} else if present {
						t.Fatalf("immediately eligible continuation %s carried a wake", item.DeliveryID)
					}
					observation, err := backend.restart.ObserveDeliveryContinuation(
						ctx,
						pendingSnapshot.Authority,
						item.DeliveryID,
					)
					if err != nil {
						t.Fatalf("observe continuation %s: %v", item.DeliveryID, err)
					}
					if observation.Disposition != want {
						t.Fatalf("observed continuation %s = %s, want %s", item.DeliveryID, observation.Disposition, want)
					}
				}

				if backend.postgres {
					longEvent := deliveryLifecycleEvent("long-transaction-clock-" + backend.name)
					longClaimRoute := deliveryLifecycleConformanceRoute(t, "node", "long-claim")
					longRetryRoute := deliveryLifecycleConformanceRoute(t, "node", "long-retry")
					longLeaseRoute := deliveryLifecycleConformanceRoute(t, "node", "long-lease")
					storetest.CommitSemanticEventWithInitialFacts(
						t,
						ctx,
						backend.selected,
						longEvent,
						[]events.DeliveryRoute{longClaimRoute, longRetryRoute, longLeaseRoute},
						runtimepipelineobligation.ScopeSubscribed,
						storetest.AcknowledgedPipelineDisposition(),
					)
					longClaimID, err := runtimedelivery.DeliveryID(longEvent.ID(), longClaimRoute)
					if err != nil {
						t.Fatal(err)
					}
					longClaimSnapshot, err := backend.store.Snapshot(ctx, longClaimID)
					if err != nil {
						t.Fatalf("load long-transaction authority: %v", err)
					}
					longRetryClaim, err := storetest.ClaimDelivery(ctx, backend.store, longEvent, longRetryRoute)
					if err != nil {
						t.Fatalf("claim long-transaction retry route: %v", err)
					}
					longLeaseClaim, err := storetest.ClaimDelivery(ctx, backend.store, longEvent, longLeaseRoute)
					if err != nil {
						t.Fatalf("claim long-transaction lease route: %v", err)
					}
					adapter, err := deliveryfixture.NewAdapter(deliveryfixture.DialectPostgres)
					if err != nil {
						t.Fatal(err)
					}
					tx, err := backend.db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatalf("begin long delivery transaction: %v", err)
					}
					defer func() { _ = tx.Rollback() }()
					var transactionStartedAt time.Time
					if err := tx.QueryRowContext(ctx, `SELECT CURRENT_TIMESTAMP`).Scan(&transactionStartedAt); err != nil {
						t.Fatalf("read PostgreSQL transaction timestamp: %v", err)
					}
					txctx, err := authoractivityfixture.Begin(ctx, tx, authoractivityfixture.DialectPostgres)
					if err != nil {
						t.Fatalf("begin long-transaction author activity: %v", err)
					}
					if _, err := tx.ExecContext(
						txctx,
						`UPDATE event_delivery_attempts SET lease_expires_at=$1
						 WHERE delivery_id=$2::uuid AND claim_version=$3 AND open_marker=TRUE`,
						transactionStartedAt.Add(500*time.Millisecond),
						longLeaseClaim.Claim.DeliveryID(),
						longLeaseClaim.Claim.Version(),
					); err != nil {
						t.Fatalf("age long-transaction lease: %v", err)
					}
					time.Sleep(1100 * time.Millisecond)
					longClaimResult, err := adapter.ClaimExactResult(
						txctx,
						tx,
						longClaimSnapshot.Authority,
						longEvent,
						longClaimRoute,
						runtimedelivery.DefaultLeaseTTL,
					)
					if err != nil {
						t.Fatalf("claim in long PostgreSQL transaction: %v", err)
					}
					longClaimed, acquired := longClaimResult.Acquired()
					if !acquired {
						t.Fatalf("long-transaction claim = %#v, want acquired", longClaimResult)
					}
					longRetry, err := adapter.SettleFailure(txctx, tx, longRetryClaim.Claim, runtimedelivery.Settlement{
						Disposition: runtimedelivery.FailureRetry,
						Failure:     testFailure("long_transaction_retry"),
						RetryBase:   10 * time.Second,
					})
					if err != nil {
						t.Fatalf("settle retry in long PostgreSQL transaction: %v", err)
					}
					leaseObservation, err := adapter.ObserveContinuationInTransaction(
						txctx,
						tx,
						longClaimSnapshot.Authority,
						longLeaseClaim.Claim.DeliveryID(),
					)
					if err != nil {
						t.Fatalf("observe aged lease in long PostgreSQL transaction: %v", err)
					}
					if leaseObservation.Disposition != runtimedelivery.ClaimReclaimable {
						t.Fatalf("aged lease observation = %s, want reclaimable after transaction-start time", leaseObservation.Disposition)
					}
					longPage, err := adapter.ScanContinuations(
						txctx,
						tx,
						longClaimSnapshot.Authority,
						runtimedelivery.ContinuationCursor{},
						10,
					)
					if err != nil {
						t.Fatalf("scan long PostgreSQL transaction continuations: %v", err)
					}
					longDispositions := map[string]runtimedelivery.ClaimDisposition{}
					longWakes := map[string]time.Duration{}
					for _, item := range longPage.Items {
						longDispositions[item.DeliveryID] = item.Disposition
						if after, ok := item.Wake.After(); ok {
							longWakes[item.DeliveryID] = after
						}
					}
					if longDispositions[longClaimed.Snapshot.DeliveryID] != runtimedelivery.ClaimBusy ||
						longWakes[longClaimed.Snapshot.DeliveryID] <= 0 {
						t.Fatalf("fresh long-transaction claim scan = %s/%s, want busy with opaque wake", longDispositions[longClaimed.Snapshot.DeliveryID], longWakes[longClaimed.Snapshot.DeliveryID])
					}
					if longDispositions[longRetry.DeliveryID] != runtimedelivery.ClaimDeferred ||
						longWakes[longRetry.DeliveryID] <= 0 {
						t.Fatalf("long-transaction retry scan = %s/%s, want deferred with opaque wake", longDispositions[longRetry.DeliveryID], longWakes[longRetry.DeliveryID])
					}
					if longDispositions[longLeaseClaim.Claim.DeliveryID()] != runtimedelivery.ClaimReclaimable {
						t.Fatalf("long-transaction expired lease scan = %s, want reclaimable", longDispositions[longLeaseClaim.Claim.DeliveryID()])
					}
					for label, observed := range map[string]time.Time{
						"claim": longClaimed.Snapshot.UpdatedAt,
						"retry": longRetry.UpdatedAt,
					} {
						if elapsed := observed.Sub(transactionStartedAt); elapsed < 900*time.Millisecond {
							t.Fatalf("%s database clock advanced %s across long transaction; want clock_timestamp semantics", label, elapsed)
						}
						if observed.Nanosecond()%1000 != 0 || observed.Location() != time.UTC {
							t.Fatalf("%s database clock = %s (%s), want UTC microseconds", label, observed, observed.Location())
						}
					}
					if delay := longRetry.NextEligibleAt.Sub(longRetry.UpdatedAt); delay != 10*time.Second {
						t.Fatalf("long-transaction retry delay = %s, want exact 10s from current database time", delay)
					}
					if err := authoractivityfixture.Finalize(txctx); err != nil {
						t.Fatalf("finalize long-transaction author activity: %v", err)
					}
					if err := tx.Commit(); err != nil {
						t.Fatalf("commit long PostgreSQL delivery transaction: %v", err)
					}
				}
			})

			t.Run("postcommit_handoff_restarts_before_prior_cursor", func(t *testing.T) {
				oldEvent := deliveryLifecycleEvent("handoff-before-cursor-" + backend.name)
				oldRoute := deliveryLifecycleConformanceRoute(t, "agent", "old-route")
				storetest.CommitSemanticEventWithRoutes(
					t,
					ctx,
					backend.selected,
					oldEvent,
					[]events.DeliveryRoute{oldRoute},
					runtimepipelineobligation.ScopeSubscribed,
				)
				// SQLite stores delivery timestamps at millisecond precision. Keep
				// the cursor-order proof from collapsing both inserts into one key.
				time.Sleep(2 * time.Millisecond)
				newEvent := eventtest.PersistedChildForProducer(
					eventtest.UUID("handoff-after-cursor-"+backend.name),
					events.EventType("delivery.conformance"),
					eventtest.Producer(events.EventProducerNode, "cursor-proof"),
					"",
					json.RawMessage(`{"ok":true}`),
					0,
					oldEvent.RunID(),
					oldEvent.ID(),
					oldEvent.Envelope(),
					oldEvent.CreatedAt().Add(time.Second),
				)
				newRoute := deliveryLifecycleConformanceRoute(t, "agent", "new-route")
				storetest.CommitSemanticEventWithInitialFacts(
					t,
					ctx,
					backend.selected,
					newEvent,
					[]events.DeliveryRoute{newRoute},
					runtimepipelineobligation.ScopeSubscribed,
					storetest.AcknowledgedPipelineDisposition(),
				)
				newID, err := runtimedelivery.DeliveryID(newEvent.ID(), newRoute)
				if err != nil {
					t.Fatal(err)
				}
				newSnapshot, err := backend.store.Snapshot(ctx, newID)
				if err != nil {
					t.Fatalf("load cursor proof authority: %v", err)
				}
				before, err := backend.store.ScanDeliveryContinuations(
					ctx,
					newSnapshot.Authority,
					runtimedelivery.ContinuationCursor{},
					1,
				)
				if err != nil {
					t.Fatalf("scan before old handoff commit: %v", err)
				}
				if len(before.Items) != 1 || before.Items[0].DeliveryID != newID || !before.Exhausted {
					t.Fatalf("pre-handoff page = %#v, want only newer visible delivery", before)
				}

				acknowledgeDeliveryLifecyclePipelineEvent(t, ctx, backend.selected, oldEvent.ID())
				staleCursorPage, err := backend.store.ScanDeliveryContinuations(
					ctx,
					newSnapshot.Authority,
					before.Next,
					1,
				)
				if err != nil {
					t.Fatalf("scan from stale cursor after old handoff: %v", err)
				}
				if len(staleCursorPage.Items) != 0 {
					t.Fatalf("stale cursor unexpectedly found older handoff: %#v", staleCursorPage)
				}
				oldID, err := runtimedelivery.DeliveryID(oldEvent.ID(), oldRoute)
				if err != nil {
					t.Fatal(err)
				}
				restarted, err := backend.restart.ScanDeliveryContinuations(
					ctx,
					newSnapshot.Authority,
					runtimedelivery.ContinuationCursor{},
					1,
				)
				if err != nil {
					t.Fatalf("restart continuation scope after handoff signal: %v", err)
				}
				if len(restarted.Items) != 1 || restarted.Items[0].DeliveryID != oldID || restarted.Exhausted {
					t.Fatalf("restarted post-handoff page = %#v, want older delivery then more work", restarted)
				}
			})

			t.Run("claim_renewal_fences_reclaim_and_preserves_settlement", func(t *testing.T) {
				event := deliveryLifecycleEvent("claim-renewal-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, "agent", "renewal-agent")
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
				claimed, err := storetest.ClaimDelivery(ctx, backend.store, event, route)
				if err != nil {
					t.Fatalf("claim delivery: %v", err)
				}
				agedAt := ageDeliveryClaimForConformance(t, ctx, backend, claimed.Snapshot.DeliveryID)
				renewed, err := backend.store.RenewClaim(ctx, claimed.Claim)
				if err != nil {
					t.Fatalf("renew claim: %v", err)
				}
				if renewed.ClaimVersion != claimed.Claim.Version() || renewed.Status != runtimedelivery.StatusInProgress || renewed.ClaimExpiresAt.Before(claimed.Snapshot.ClaimExpiresAt) || !renewed.UpdatedAt.After(agedAt) {
					t.Fatalf("renewed claim = %#v, original = %#v", renewed, claimed.Snapshot)
				}
				if lease := renewed.ClaimExpiresAt.Sub(renewed.UpdatedAt); lease != runtimedelivery.DefaultLeaseTTL {
					t.Fatalf("renewed lease window = %s, want %s from exact database renewal time", lease, runtimedelivery.DefaultLeaseTTL)
				}
				assertDeliveryAttemptLeaseMatchesObligation(t, ctx, backend, renewed.DeliveryID, renewed.ClaimVersion)
				beforeExpiry, err := claimDeliveryResult(ctx, backend.restart, event, route)
				if err != nil || beforeExpiry.Disposition != runtimedelivery.ClaimBusy {
					t.Fatalf("claim before renewed expiry = %#v, err=%v; want busy", beforeExpiry, err)
				}

				expireDeliveryClaimForConformance(t, ctx, backend, renewed.DeliveryID)
				if _, err := backend.store.RenewClaim(ctx, claimed.Claim); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("expired claim renewal = %v, want ErrConflict", err)
				}
				reclaimed, err := storetest.ClaimDelivery(ctx, backend.restart, event, route)
				if err != nil {
					t.Fatalf("reclaim after renewed lease expiry: %v", err)
				}
				if reclaimed.Claim.Version() != claimed.Claim.Version()+1 {
					t.Fatalf("reclaimed version = %d, want %d", reclaimed.Claim.Version(), claimed.Claim.Version()+1)
				}
				if _, err := backend.store.RenewClaim(ctx, claimed.Claim); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("superseded claim renewal = %v, want ErrConflict", err)
				}
				settled, err := backend.restart.SettleSuccess(ctx, reclaimed.Claim, []string{"renewal.proven"}, time.Millisecond)
				if err != nil || settled.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("settle renewed lifecycle = %#v, err=%v", settled, err)
				}
				assertDeliveryAttemptHistory(t, ctx, backend, settled.DeliveryID)
			})

			t.Run("parent_terminalization_fences_late_writer", func(t *testing.T) {
				event := deliveryLifecycleEvent("terminalize-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, "agent", "terminal-agent")
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
				claimed, err := storetest.ClaimDelivery(ctx, backend.store, event, route)
				if err != nil {
					t.Fatal(err)
				}
				removeFault := installDeliveryDeadLetterFault(t, ctx, backend)
				if _, err := backend.store.TerminalizeRun(ctx, event.RunID(), "run_terminal"); err == nil {
					t.Fatal("run terminalization succeeded while required diagnostic writer was faulted")
				}
				assertDeliverySettlementRolledBack(t, ctx, backend, claimed)
				removeFault()
				transitions, err := backend.store.TerminalizeRun(ctx, event.RunID(), "run_terminal")
				if err != nil {
					t.Fatalf("terminalize run: %v", err)
				}
				if len(transitions) != 1 || transitions[0].Current.Status != runtimedelivery.StatusDeadLetter {
					t.Fatalf("terminalizations = %#v", transitions)
				}
				current := transitions[0].Current
				if current.ClaimVersion != claimed.Claim.Version()+1 || current.Failure == nil || current.Failure.Detail.Code != "delivery_parent_terminalized" {
					t.Fatalf("terminalized snapshot = %#v, want new exact fence and typed parent failure", current)
				}
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("late settlement error = %v, want ErrConflict", err)
				}
				outcomes, err := backend.store.Outcomes(ctx, transitions[0].Current.DeliveryID)
				if err != nil || len(outcomes) != 1 || outcomes[0].Outcome != "terminalized" || outcomes[0].ClaimVersion != current.ClaimVersion || outcomes[0].Failure == nil {
					t.Fatalf("terminalization outcomes = %#v, err=%v", outcomes, err)
				}
				assertExactDeliveryDeadLetter(t, ctx, backend, event, current)
			})

			t.Run("concurrent_claim_and_restart_reclaim_are_fenced", func(t *testing.T) {
				event := deliveryLifecycleEvent("claim-race-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, "agent", "race-agent")
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)

				type concurrentClaimResult struct {
					result runtimedelivery.ClaimResult
					err    error
				}
				const contenders = 8
				start := make(chan struct{})
				results := make(chan concurrentClaimResult, contenders)
				for index := 0; index < contenders; index++ {
					claimStore := backend.store
					if index%2 == 1 {
						claimStore = backend.restart
					}
					go func() {
						<-start
						result, err := claimDeliveryResult(ctx, claimStore, event, route)
						results <- concurrentClaimResult{result: result, err: err}
					}()
				}
				close(start)

				var winner runtimedelivery.ClaimedObligation
				wins := 0
				for index := 0; index < contenders; index++ {
					result := <-results
					if claimed, acquired := result.result.Acquired(); result.err == nil && acquired {
						winner = claimed
						wins++
						continue
					}
					if result.err != nil {
						if !errors.Is(result.err, runtimedelivery.ErrConflict) {
							t.Fatalf("claim race loser error = %v, want ErrConflict", result.err)
						}
						continue
					}
					if result.result.Disposition != runtimedelivery.ClaimBusy {
						t.Fatalf("claim race loser disposition = %s, want busy", result.result.Disposition)
					}
				}
				if wins != 1 {
					t.Fatalf("claim race winners = %d, want exactly one", wins)
				}
				if winner.Claim.Version() != 1 || winner.Snapshot.Status != runtimedelivery.StatusInProgress {
					t.Fatalf("initial winning claim = %#v", winner)
				}

				beforeExpiry, err := claimDeliveryResult(ctx, backend.restart, event, route)
				if err != nil || beforeExpiry.Disposition != runtimedelivery.ClaimBusy {
					t.Fatalf("pre-expiry reconstructed-store claim = %#v, err=%v; want busy", beforeExpiry, err)
				}
				expireDeliveryClaimForConformance(t, ctx, backend, winner.Snapshot.DeliveryID)
				reclaimed, err := storetest.ClaimDelivery(ctx, backend.restart, event, route)
				if err != nil {
					t.Fatalf("post-expiry reconstructed-store reclaim: %v", err)
				}
				if reclaimed.Claim.Version() != winner.Claim.Version()+1 || reclaimed.Snapshot.DeliveryID != winner.Snapshot.DeliveryID {
					t.Fatalf("reclaimed delivery = %#v, first = %#v", reclaimed, winner)
				}
				if _, err := backend.store.SettleSuccess(ctx, winner.Claim, nil, 0); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("expired claimant settlement error = %v, want ErrConflict", err)
				}
				settled, err := backend.restart.SettleSuccess(ctx, reclaimed.Claim, []string{"race.proven"}, time.Millisecond)
				if err != nil || settled.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("current claimant settlement = %#v, err=%v", settled, err)
				}
				assertDeliveryAttemptHistory(t, ctx, backend, settled.DeliveryID)
			})

			t.Run("fresh_schema_rejects_disconnected_lifecycle_facts", func(t *testing.T) {
				assertDeliverySchemaRejectsDisconnectedFacts(t, ctx, backend)
			})

			t.Run("expired_claim_precedes_continuous_pending_backlog", func(t *testing.T) {
				expiredEvent := deliveryLifecycleEvent("selector-expired-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, "agent", "selector-agent-"+backend.name)
				routes := []events.DeliveryRoute{route}
				for index := 0; index < 12; index++ {
					routes = append(routes, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(fmt.Sprintf("selector-pending-%s-%02d", backend.name, index)), AgentIdentity: agentidentitytest.RootRuntime(
						t,
						fmt.Sprintf("selector-pending-%s-%02d", backend.name, index),
						"delivery-lifecycle-conformance",
					),
					})
				}
				storetest.CommitSemanticEventWithInitialFacts(
					t,
					ctx,
					backend.selected,
					expiredEvent,
					routes,
					runtimepipelineobligation.ScopeSubscribed,
					storetest.AcknowledgedPipelineDisposition(),
				)
				claimed, err := storetest.ClaimDelivery(ctx, backend.store, expiredEvent, route)
				if err != nil {
					t.Fatalf("claim selector expiry candidate: %v", err)
				}
				expireDeliveryClaimForConformance(t, ctx, backend, claimed.Snapshot.DeliveryID)
				page, err := backend.restart.ScanDeliveryContinuations(
					ctx,
					claimed.Snapshot.Authority,
					runtimedelivery.ContinuationCursor{},
					1,
				)
				if err != nil {
					t.Fatalf("scan saturated continuation scope: %v", err)
				}
				if len(page.Items) != 1 || page.Items[0].Snapshot.DeliveryID != claimed.Snapshot.DeliveryID ||
					page.Items[0].Disposition != runtimedelivery.ClaimReclaimable || page.Exhausted {
					t.Fatalf("first saturated continuation page = %#v, want reclaimable delivery %s and more work", page, claimed.Snapshot.DeliveryID)
				}
			})

			t.Run("terminal_settlement_commits_exact_diagnostic_atomically", func(t *testing.T) {
				for _, class := range []runtimedelivery.SubscriberClass{runtimedelivery.SubscriberAgent, runtimedelivery.SubscriberNode} {
					class := class
					t.Run(string(class), func(t *testing.T) {
						event := deliveryLifecycleEvent(fmt.Sprintf("atomic-diagnostic-%s-%s", backend.name, class))
						route := deliveryLifecycleConformanceRoute(t, string(class), "diagnostic-"+string(class))
						storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
						var claimed runtimedelivery.ClaimedObligation
						var err error
						if class == runtimedelivery.SubscriberAgent {
							claimed, err = storetest.ClaimDelivery(ctx, backend.store, event, route)
						} else {
							claimed, err = storetest.ClaimDelivery(ctx, backend.store, event, route)
						}
						if err != nil {
							t.Fatalf("claim %s diagnostic delivery: %v", class, err)
						}
						removeFault := installDeliveryDeadLetterFault(t, ctx, backend)
						settlement := runtimedelivery.Settlement{
							Disposition: runtimedelivery.FailureDeadLetter,
							ReasonCode:  "terminal_test_failure",
							Failure:     testFailure("terminal_test_failure"),
						}
						if _, err := backend.store.SettleFailure(ctx, claimed.Claim, settlement); err == nil {
							t.Fatal("terminal settlement succeeded while required diagnostic writer was faulted")
						}
						assertDeliverySettlementRolledBack(t, ctx, backend, claimed)
						removeFault()
						settled, err := backend.store.SettleFailure(ctx, claimed.Claim, settlement)
						if err != nil {
							t.Fatalf("settle %s diagnostic delivery: %v", class, err)
						}
						if settled.Failure == nil || settled.Failure.Class != settlement.Failure.Class || settled.Failure.Detail.Code != settlement.Failure.Detail.Code || settled.RetryCount != 0 {
							t.Fatalf("direct terminal settlement changed original failure or retry count: %#v", settled)
						}
						assertExactDeliveryDeadLetter(t, ctx, backend, event, settled)
					})
				}
			})
		})
	}
}

func acknowledgeDeliveryLifecyclePipelineEvent(
	t testing.TB,
	ctx context.Context,
	selected any,
	eventID string,
) {
	t.Helper()
	provider, ok := selected.(interface {
		PipelineObligations() runtimepipelineobligation.Store
	})
	if !ok {
		t.Fatalf("selected store %T has no pipeline obligation owner", selected)
	}
	owner := provider.PipelineObligations()
	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("claim pipeline obligation for %s: %v", eventID, err)
	}
	if _, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
		t.Fatalf("acknowledge pipeline obligation for %s: %v", eventID, err)
	}
}

func installDeliveryDeadLetterFault(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend) func() {
	t.Helper()
	if backend.postgres {
		if _, err := backend.db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION fail_delivery_dead_letter_insert() RETURNS trigger AS $$ BEGIN IF NEW.delivery_id IS NOT NULL THEN RAISE EXCEPTION 'forced delivery diagnostic failure'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql`); err != nil {
			t.Fatalf("create delivery diagnostic fault function: %v", err)
		}
		if _, err := backend.db.ExecContext(ctx, `CREATE TRIGGER fail_delivery_dead_letter_insert BEFORE INSERT ON dead_letters FOR EACH ROW EXECUTE FUNCTION fail_delivery_dead_letter_insert()`); err != nil {
			t.Fatalf("create delivery diagnostic fault trigger: %v", err)
		}
		cleanup := func() {
			if _, err := backend.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS fail_delivery_dead_letter_insert ON dead_letters`); err != nil {
				t.Fatalf("drop delivery diagnostic fault trigger: %v", err)
			}
			if _, err := backend.db.ExecContext(ctx, `DROP FUNCTION IF EXISTS fail_delivery_dead_letter_insert()`); err != nil {
				t.Fatalf("drop delivery diagnostic fault function: %v", err)
			}
		}
		t.Cleanup(cleanup)
		return cleanup
	}
	if _, err := backend.db.ExecContext(ctx, `CREATE TRIGGER fail_delivery_dead_letter_insert BEFORE INSERT ON dead_letters WHEN NEW.delivery_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'forced delivery diagnostic failure'); END`); err != nil {
		t.Fatalf("create sqlite delivery diagnostic fault trigger: %v", err)
	}
	cleanup := func() {
		if _, err := backend.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS fail_delivery_dead_letter_insert`); err != nil {
			t.Fatalf("drop sqlite delivery diagnostic fault trigger: %v", err)
		}
	}
	t.Cleanup(cleanup)
	return cleanup
}

func assertDeliverySettlementRolledBack(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, claimed runtimedelivery.ClaimedObligation) {
	t.Helper()
	snapshot, err := backend.store.Snapshot(ctx, claimed.Snapshot.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != runtimedelivery.StatusInProgress || snapshot.ClaimVersion != claimed.Claim.Version() {
		t.Fatalf("faulted settlement snapshot = %#v, want original in-progress claim", snapshot)
	}
	query := `SELECT (SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=$1::uuid), (SELECT COUNT(*) FROM dead_letters WHERE delivery_id=$1::uuid), (SELECT COUNT(*) FROM author_activity_occurrences WHERE source_identity=$1::text AND transition IN ('dead_letter', 'terminalized'))`
	if !backend.postgres {
		query = `SELECT (SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=?), (SELECT COUNT(*) FROM dead_letters WHERE delivery_id=?), (SELECT COUNT(*) FROM author_activity_occurrences WHERE source_identity=? AND transition IN ('dead_letter', 'terminalized'))`
	}
	args := []any{claimed.Snapshot.DeliveryID}
	if !backend.postgres {
		args = []any{claimed.Snapshot.DeliveryID, claimed.Snapshot.DeliveryID, claimed.Snapshot.DeliveryID}
	}
	var outcomes, diagnostics, transitions int
	if err := backend.db.QueryRowContext(ctx, query, args...).Scan(&outcomes, &diagnostics, &transitions); err != nil {
		t.Fatalf("read faulted settlement evidence: %v", err)
	}
	if outcomes != 0 || diagnostics != 0 || transitions != 0 {
		t.Fatalf("faulted settlement evidence outcomes=%d diagnostics=%d transitions=%d, want all zero", outcomes, diagnostics, transitions)
	}
}

func assertExactDeliveryDeadLetter(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, event events.Event, settled runtimedelivery.Snapshot) {
	t.Helper()
	query := `SELECT delivery_id::text, claim_version, original_event, original_payload, failure, retry_count, chain_depth, handler_node FROM dead_letters WHERE delivery_id=$1::uuid AND claim_version=$2`
	if !backend.postgres {
		query = `SELECT delivery_id, claim_version, original_event, original_payload, failure, retry_count, chain_depth, handler_node FROM dead_letters WHERE delivery_id=? AND claim_version=?`
	}
	var deliveryID, eventType, handler string
	var claimVersion int64
	var payload, failureRaw []byte
	var retryCount, chainDepth int
	if err := backend.db.QueryRowContext(ctx, query, settled.DeliveryID, settled.ClaimVersion).Scan(&deliveryID, &claimVersion, &eventType, &payload, &failureRaw, &retryCount, &chainDepth, &handler); err != nil {
		t.Fatalf("read exact terminal delivery diagnostic: %v", err)
	}
	deadLetterFailure, err := runtimefailures.UnmarshalEnvelope(failureRaw)
	if err != nil {
		t.Fatalf("decode exact terminal delivery failure: %v", err)
	}
	var gotPayload, wantPayload any
	if err := json.Unmarshal(payload, &gotPayload); err != nil {
		t.Fatalf("decode terminal diagnostic payload: %v", err)
	}
	if err := json.Unmarshal(event.Payload(), &wantPayload); err != nil {
		t.Fatal(err)
	}
	if deliveryID != settled.DeliveryID || claimVersion != settled.ClaimVersion || eventType != string(event.Type()) ||
		!reflect.DeepEqual(gotPayload, wantPayload) || settled.Failure == nil || !reflect.DeepEqual(deadLetterFailure, *settled.Failure) || retryCount != settled.RetryCount || chainDepth != event.ChainDepth() || handler != settled.SubscriberID {
		t.Fatalf("terminal diagnostic = delivery:%s version:%d type:%s payload:%v retry:%d depth:%d handler:%s; want exact settled/event facts", deliveryID, claimVersion, eventType, gotPayload, retryCount, chainDepth, handler)
	}
	attemptQuery := `SELECT failure FROM event_delivery_attempts WHERE delivery_id=$1::uuid AND claim_version=$2`
	activityQuery := `SELECT failure FROM author_activity_occurrences WHERE source_owner='event_deliveries' AND source_identity=$1 ORDER BY sequence DESC LIMIT 1`
	if !backend.postgres {
		attemptQuery = `SELECT failure FROM event_delivery_attempts WHERE delivery_id=? AND claim_version=?`
		activityQuery = `SELECT failure FROM author_activity_occurrences WHERE source_owner='event_deliveries' AND source_identity=? ORDER BY sequence DESC LIMIT 1`
	}
	for owner, queryAndArgs := range map[string]struct {
		query string
		args  []any
	}{
		"attempt":         {query: attemptQuery, args: []any{settled.DeliveryID, settled.ClaimVersion}},
		"author activity": {query: activityQuery, args: []any{settled.DeliveryID}},
	} {
		var raw []byte
		if err := backend.db.QueryRowContext(ctx, queryAndArgs.query, queryAndArgs.args...).Scan(&raw); err != nil {
			t.Fatalf("read exact terminal %s failure: %v", owner, err)
		}
		persisted, err := runtimefailures.UnmarshalEnvelope(raw)
		if err != nil || !reflect.DeepEqual(persisted, *settled.Failure) {
			t.Fatalf("terminal %s failure = %#v, err=%v; want %#v", owner, persisted, err, *settled.Failure)
		}
	}
}

func assertDeliverySchemaRejectsDisconnectedFacts(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend) {
	t.Helper()
	route := deliveryLifecycleConformanceRoute(t, "agent", "schema-agent-"+backend.name)
	event := deliveryLifecycleEvent("schema-event-" + backend.name)
	other := deliveryLifecycleEvent("schema-other-run-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, other, nil, runtimepipelineobligation.ScopeDirect)
	proof, err := backend.store.ProveHandoff(ctx, event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("persisted handoff proof is incomplete: %v", err)
	}
	assertDeliverySQLRejected(t, backend, "event/run mismatch",
		`UPDATE event_deliveries SET run_id = $1::uuid WHERE delivery_id = $2::uuid`,
		`UPDATE event_deliveries SET run_id = ? WHERE delivery_id = ?`,
		[]any{other.RunID(), proof.DeliveryID()})

	missingAttemptEvent := deliveryLifecycleEvent("schema-missing-attempt-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, missingAttemptEvent, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	missingAttempt, err := backend.store.ProveHandoff(ctx, missingAttemptEvent.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	assertDeliverySQLRejected(t, backend, "in-progress without open attempt",
		`UPDATE event_deliveries SET status='in_progress', next_eligible_at=NULL, claim_version=1, current_attempt_version=1, current_attempt_open=TRUE, started_at=$1, updated_at=$1 WHERE delivery_id=$2::uuid`,
		`UPDATE event_deliveries SET status='in_progress', next_eligible_at=NULL, claim_version=1, current_attempt_version=1, current_attempt_open=TRUE, started_at=?, updated_at=? WHERE delivery_id=?`,
		[]any{now, missingAttempt.DeliveryID()})

	sessionEvent := deliveryLifecycleEvent("schema-session-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, sessionEvent, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	claimed, err := storetest.ClaimDelivery(ctx, backend.store, sessionEvent, route)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, "nonexistent exact agent session",
		`UPDATE event_delivery_attempts SET active_session_id=$1::uuid, session_run_id=$2::uuid, session_agent_id=$3 WHERE delivery_id=$4::uuid AND claim_version=$5`,
		`UPDATE event_delivery_attempts SET active_session_id=?, session_run_id=?, session_agent_id=? WHERE delivery_id=? AND claim_version=?`,
		[]any{uuid.NewString(), sessionEvent.RunID(), route.Recipient.ID(), claimed.Snapshot.DeliveryID, claimed.Claim.Version()})

	otherSessionEvent := deliveryLifecycleEvent("schema-session-other-owner-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, otherSessionEvent, nil, runtimepipelineobligation.ScopeDirect)
	otherSessionID := uuid.NewString()
	otherAgentID := "schema-other-agent-" + backend.name
	seedDeliveryAgentSession(t, ctx, backend, otherSessionID, otherSessionEvent.RunID(), otherAgentID)
	assertDeliverySQLRejected(t, backend, "session owned by another delivery run and agent",
		`UPDATE event_delivery_attempts SET active_session_id=$1::uuid, session_delivery_id=$2::uuid, session_run_id=$3::uuid, session_subscriber_type='agent', session_agent_id=$4 WHERE delivery_id=$2::uuid AND claim_version=$5`,
		`UPDATE event_delivery_attempts SET active_session_id=?1, session_delivery_id=?2, session_run_id=?3, session_subscriber_type='agent', session_agent_id=?4 WHERE delivery_id=?2 AND claim_version=?5`,
		[]any{otherSessionID, claimed.Snapshot.DeliveryID, otherSessionEvent.RunID(), otherAgentID, claimed.Claim.Version()})
	if _, err := backend.store.BindAgentSession(ctx, claimed.Claim, otherSessionID); !errors.Is(err, runtimedelivery.ErrConflict) {
		t.Fatalf("bind session owned by another delivery error = %v, want ErrConflict", err)
	}

	assertDeliverySQLRejected(t, backend, "unreferenced second open attempt",
		`INSERT INTO event_delivery_attempts (delivery_id, claim_version, claim_token, started_at, lease_expires_at, current_delivery_id, open_marker) VALUES ($1::uuid, $2, $3::uuid, $4, $5, $1::uuid, TRUE)`,
		`INSERT INTO event_delivery_attempts (delivery_id, claim_version, claim_token, started_at, lease_expires_at, current_delivery_id, open_marker) VALUES (?1, ?2, ?3, ?4, ?5, ?1, TRUE)`,
		[]any{claimed.Snapshot.DeliveryID, claimed.Claim.Version() + 1, uuid.NewString(), now, now.Add(time.Minute)})

	terminatedRoute := deliveryLifecycleConformanceRoute(t, "agent", "terminated-session-agent-"+backend.name)
	terminatedEvent := deliveryLifecycleEvent("schema-terminated-session-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, terminatedEvent, []events.DeliveryRoute{terminatedRoute}, runtimepipelineobligation.ScopeSubscribed)
	terminatedClaim, err := storetest.ClaimDelivery(ctx, backend.store, terminatedEvent, terminatedRoute)
	if err != nil {
		t.Fatal(err)
	}
	terminatedSessionID := uuid.NewString()
	seedDeliveryAgentSession(t, ctx, backend, terminatedSessionID, terminatedEvent.RunID(), terminatedRoute.Recipient.ID())
	terminateSessionQuery := `UPDATE agent_sessions SET status='terminated', termination_reason='normal', terminated_at=$2, updated_at=$2 WHERE session_id=$1::uuid`
	if !backend.postgres {
		terminateSessionQuery = `UPDATE agent_sessions SET status='terminated', termination_reason='normal', terminated_at=?, updated_at=? WHERE session_id=?`
	}
	var terminateErr error
	if backend.postgres {
		_, terminateErr = backend.db.ExecContext(ctx, terminateSessionQuery, terminatedSessionID, now)
	} else {
		_, terminateErr = backend.db.ExecContext(ctx, terminateSessionQuery, now, now, terminatedSessionID)
	}
	if terminateErr != nil {
		t.Fatalf("terminate exact delivery session: %v", terminateErr)
	}
	if _, err := backend.store.BindAgentSession(ctx, terminatedClaim.Claim, terminatedSessionID); !errors.Is(err, runtimedelivery.ErrConflict) {
		t.Fatalf("bind terminated exact session error = %v, want ErrConflict", err)
	}

	outcomeEvent := deliveryLifecycleEvent("schema-outcome-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, outcomeEvent, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	outcomeProof, err := backend.store.ProveHandoff(ctx, outcomeEvent.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, "outcome without exact attempt",
		`INSERT INTO event_delivery_outcomes (delivery_id, claim_version, outcome, side_effects, duration_ms, settled_at) VALUES ($1::uuid, 99, 'delivered', '[]'::jsonb, 0, $2)`,
		`INSERT INTO event_delivery_outcomes (delivery_id, claim_version, outcome, side_effects, duration_ms, settled_at) VALUES (?, 99, 'delivered', '[]', 0, ?)`,
		[]any{outcomeProof.DeliveryID(), now})

	deadLetterEvent := deliveryLifecycleEvent("schema-dead-letter-failure-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, deadLetterEvent, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	deadLetterProof, err := backend.store.ProveHandoff(ctx, deadLetterEvent.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, "dead-letter without typed failure",
		`UPDATE event_deliveries SET status='dead_letter', next_eligible_at=NULL, reason_code='raw_terminal', settled_at=created_at, updated_at=created_at WHERE delivery_id=$1::uuid`,
		`UPDATE event_deliveries SET status='dead_letter', next_eligible_at=NULL, reason_code='raw_terminal', settled_at=created_at, updated_at=created_at WHERE delivery_id=?`,
		[]any{deadLetterProof.DeliveryID()})
}

func assertDeliverySQLRejected(t *testing.T, backend deliveryLifecycleConformanceBackend, name, postgresQuery, sqliteQuery string, args []any) {
	t.Helper()
	query := postgresQuery
	if !backend.postgres {
		query = sqliteQuery
		if name == "in-progress without open attempt" {
			args = []any{args[0], args[0], args[1]}
		}
	}
	if _, err := backend.db.Exec(query, args...); err == nil {
		t.Fatalf("%s mutation succeeded; fresh schema must reject it", name)
	}
}

func assertDeliveryRetryBudget(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, class runtimedelivery.SubscriberClass, subscriberID string, maxRetries int) {
	t.Helper()
	event := deliveryLifecycleEvent(fmt.Sprintf("retry-%s-%s", backend.name, class))
	route := deliveryLifecycleConformanceRoute(t, string(class), subscriberID)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	proof, err := backend.store.ProveHandoff(ctx, event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	var exhausted runtimedelivery.Snapshot
	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		var claimed runtimedelivery.ClaimedObligation
		if class == runtimedelivery.SubscriberAgent {
			claimed, err = storetest.ClaimDelivery(ctx, backend.store, event, route)
		} else {
			claimed, err = storetest.ClaimDelivery(ctx, backend.store, event, route)
		}
		if err != nil {
			t.Fatalf("claim %s attempt %d: %v", class, attempt, err)
		}
		settled, settleErr := backend.store.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry,
			Failure:     testFailure("handler_failed"),
			Duration:    time.Duration(attempt) * time.Millisecond,
			RetryBase:   time.Nanosecond,
		})
		if settleErr != nil {
			t.Fatalf("settle %s attempt %d: %v", class, attempt, settleErr)
		}
		if attempt <= maxRetries {
			if settled.Status != runtimedelivery.StatusFailed || settled.RetryCount != attempt {
				t.Fatalf("retry %d snapshot = %#v", attempt, settled)
			}
			makeDeliveryImmediatelyEligible(t, ctx, backend, proof.DeliveryID())
		} else {
			if settled.Status != runtimedelivery.StatusDeadLetter || settled.RetryCount != maxRetries || settled.ReasonCode != "retry_exhausted" {
				t.Fatalf("exhausted snapshot = %#v", settled)
			}
			exhausted = settled
			assertRetryExhaustedFailure(t, settled.Failure, maxRetries)
			assertExactDeliveryDeadLetter(t, ctx, backend, event, settled)
		}
	}
	outcomes, err := backend.store.Outcomes(ctx, proof.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != maxRetries+1 {
		t.Fatalf("outcome count = %d, want %d", len(outcomes), maxRetries+1)
	}
	for index, outcome := range outcomes {
		want := "retry_scheduled"
		if index == maxRetries {
			want = "dead_letter"
		}
		if outcome.ClaimVersion != int64(index+1) || outcome.Outcome != want {
			t.Fatalf("outcome %d = %#v, want version=%d outcome=%s", index, outcome, index+1, want)
		}
		if index < maxRetries {
			if outcome.Failure == nil || outcome.Failure.Class != runtimefailures.ClassConnectorFailure || outcome.Failure.Detail.Code != "handler_failed" {
				t.Fatalf("retry outcome %d failure = %#v, want original failure", index, outcome.Failure)
			}
		} else if exhausted.Failure == nil || outcome.Failure == nil || !reflect.DeepEqual(*outcome.Failure, *exhausted.Failure) {
			t.Fatalf("terminal outcome failure = %#v, want synthesized exhausted failure %#v", outcome.Failure, exhausted.Failure)
		}
	}
}

func assertRetryExhaustedFailure(t *testing.T, failure *runtimefailures.Envelope, maxRetries int) {
	t.Helper()
	if failure == nil || failure.Class != runtimefailures.ClassRetryExhausted || failure.Detail.Code != "delivery_retry_exhausted" || failure.Retryable || !failure.Deterministic {
		t.Fatalf("retry-exhausted failure = %#v", failure)
	}
	raw, err := json.Marshal(failure.Detail.Attributes)
	if err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		MaxRetries   int `json:"max_retries"`
		RetryHistory []struct {
			ClaimVersion int                      `json:"claim_version"`
			Failure      runtimefailures.Envelope `json:"failure"`
		} `json:"retry_history"`
	}
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatalf("decode retry-exhausted evidence: %v", err)
	}
	if evidence.MaxRetries != maxRetries || len(evidence.RetryHistory) != maxRetries+1 {
		t.Fatalf("retry-exhausted evidence = %#v, want max=%d attempts=%d", evidence, maxRetries, maxRetries+1)
	}
	for index, attempt := range evidence.RetryHistory {
		if attempt.ClaimVersion != index+1 || attempt.Failure.Class != runtimefailures.ClassConnectorFailure || attempt.Failure.Detail.Code != "handler_failed" {
			t.Fatalf("retry history attempt %d = %#v", index, attempt)
		}
	}
}

func makeDeliveryImmediatelyEligible(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	query := `UPDATE event_deliveries SET next_eligible_at = $1 WHERE delivery_id = $2::uuid AND status = 'failed'`
	if !backend.postgres {
		query = `UPDATE event_deliveries SET next_eligible_at = ? WHERE delivery_id = ? AND status = 'failed'`
	}
	result, err := backend.db.ExecContext(ctx, query, time.Now().Add(-time.Hour).UTC(), deliveryID)
	if err != nil {
		t.Fatalf("make retry eligible: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("make retry eligible affected %d rows, err=%v", rows, err)
	}
}

func ageDeliveryClaimForConformance(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) time.Time {
	t.Helper()
	agedAt := time.Now().Add(-15 * time.Minute).UTC()
	transaction, err := backend.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin aged-claim proof: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	deliveryQuery := `UPDATE event_deliveries SET created_at = $1, started_at = $1, updated_at = $1 WHERE delivery_id = $2::uuid AND status = 'in_progress'`
	attemptQuery := `UPDATE event_delivery_attempts SET started_at = $1 WHERE delivery_id = $2::uuid AND open_marker = TRUE`
	if !backend.postgres {
		deliveryQuery = `UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ? WHERE delivery_id = ? AND status = 'in_progress'`
		attemptQuery = `UPDATE event_delivery_attempts SET started_at = ? WHERE delivery_id = ? AND open_marker = TRUE`
	}
	var deliveryResult sql.Result
	if backend.postgres {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, agedAt, deliveryID)
	} else {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, agedAt, agedAt, agedAt, deliveryID)
	}
	if err != nil {
		t.Fatalf("age delivery lifecycle timestamp: %v", err)
	}
	if rows, rowsErr := deliveryResult.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("age delivery lifecycle affected %d rows, err=%v", rows, rowsErr)
	}
	if result, execErr := transaction.ExecContext(ctx, attemptQuery, agedAt, deliveryID); execErr != nil {
		t.Fatalf("age delivery attempt: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("age delivery attempt affected %d rows, err=%v", rows, rowsErr)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit aged-claim proof: %v", err)
	}
	return agedAt
}

func expireDeliveryClaimForConformance(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	startedAt := time.Now().Add(-2 * time.Hour).UTC()
	expiresAt := time.Now().Add(-time.Hour).UTC()
	transaction, err := backend.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin claim-expiry proof: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	deliveryQuery := `UPDATE event_deliveries SET created_at = $1, started_at = $1, updated_at = $2 WHERE delivery_id = $3::uuid AND status = 'in_progress'`
	attemptQuery := `UPDATE event_delivery_attempts SET started_at = $1, lease_expires_at = $2 WHERE delivery_id = $3::uuid AND open_marker = TRUE`
	if !backend.postgres {
		deliveryQuery = `UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ? WHERE delivery_id = ? AND status = 'in_progress'`
		attemptQuery = `UPDATE event_delivery_attempts SET started_at = ?, lease_expires_at = ? WHERE delivery_id = ? AND open_marker = TRUE`
	}
	var deliveryResult sql.Result
	if backend.postgres {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, startedAt, expiresAt, deliveryID)
	} else {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, startedAt, startedAt, expiresAt, deliveryID)
	}
	if err != nil {
		t.Fatalf("expire delivery claim: %v", err)
	}
	if rows, rowsErr := deliveryResult.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("expire delivery claim affected %d rows, err=%v", rows, rowsErr)
	}
	if result, execErr := transaction.ExecContext(ctx, attemptQuery, startedAt, expiresAt, deliveryID); execErr != nil {
		t.Fatalf("expire delivery attempt: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("expire delivery attempt affected %d rows, err=%v", rows, rowsErr)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit claim-expiry proof: %v", err)
	}
}

func assertDeliveryAttemptHistory(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	query := `SELECT claim_version, outcome FROM event_delivery_attempts WHERE delivery_id = $1::uuid ORDER BY claim_version`
	if !backend.postgres {
		query = `SELECT claim_version, outcome FROM event_delivery_attempts WHERE delivery_id = ? ORDER BY claim_version`
	}
	rows, err := backend.db.QueryContext(ctx, query, deliveryID)
	if err != nil {
		t.Fatalf("load delivery attempt history: %v", err)
	}
	defer rows.Close()
	type attempt struct {
		version int64
		outcome string
	}
	var attempts []attempt
	for rows.Next() {
		var current attempt
		if err := rows.Scan(&current.version, &current.outcome); err != nil {
			t.Fatalf("scan delivery attempt history: %v", err)
		}
		attempts = append(attempts, current)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read delivery attempt history: %v", err)
	}
	if len(attempts) != 2 || attempts[0] != (attempt{version: 1, outcome: "lease_expired"}) || attempts[1] != (attempt{version: 2, outcome: "delivered"}) {
		t.Fatalf("delivery attempt history = %#v", attempts)
	}
}

func assertDeliveryAttemptLeaseMatchesObligation(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string, version int64) {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id = a.delivery_id AND d.current_attempt_version = a.claim_version AND d.current_attempt_open = TRUE WHERE a.delivery_id = $1::uuid AND a.claim_version = $2 AND a.open_marker = TRUE`
	if !backend.postgres {
		query = `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id = a.delivery_id AND d.current_attempt_version = a.claim_version AND d.current_attempt_open = TRUE WHERE a.delivery_id = ? AND a.claim_version = ? AND a.open_marker = TRUE`
	}
	var matches int
	if err := backend.db.QueryRowContext(ctx, query, deliveryID, version).Scan(&matches); err != nil {
		t.Fatalf("compare renewed delivery attempt lease: %v", err)
	}
	if matches != 1 {
		t.Fatalf("matching renewed attempt and obligation leases = %d, want 1", matches)
	}
}

func deliveryLifecycleEvent(label string) events.Event {
	return deliveryLifecycleEventWithPayload(label, json.RawMessage(`{"ok":true}`))
}

func deliveryLifecycleEventWithPayload(label string, payload json.RawMessage) events.Event {
	return eventtest.RunCreatingRootIngress(
		eventtest.UUID(label),
		events.EventType("delivery.conformance"),
		"conformance-ingress",
		"",
		payload,
		0,
		eventtest.UUID(label+"-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(label+"-entity")),
		time.Now().UTC(),
	)
}

func seedDeliveryAgentSession(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, sessionID, runID, agentID string) {
	t.Helper()
	identity := agentidentitytest.RootRuntime(t, agentID, "delivery-lifecycle-conformance")
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("seed delivery agent identity: %v", err)
	}
	selected, ok := backend.selected.(storetest.AgentFixtureStore)
	if !ok {
		t.Fatalf("delivery lifecycle selected store %T lacks canonical agent fixture admission", backend.selected)
	}
	storetest.RequireAgentFixture(t, ctx, selected, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID: agentID, Identity: identity, Role: "delivery-test", Type: "stub", Model: "regular", ExecutionMode: "live",
			ResolvedLLMBackend: "anthropic", Intent: deliveryLifecycleAgentIntent(t, agentID),
		},
		Status: "active", HiredBy: "delivery-lifecycle-conformance", StartedAt: time.Now().UTC(),
	})
	sessionQuery := `
			INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance
		) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9)`
	if !backend.postgres {
		sessionQuery = `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	}
	identityArgs := []any{
		fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
	}
	sessionArgs := append([]any{sessionID, runID}, identityArgs...)
	if _, err := backend.db.ExecContext(ctx, sessionQuery, sessionArgs...); err != nil {
		t.Fatalf("seed delivery agent session: %v", err)
	}
}

func deliveryLifecycleAgentIntent(t testing.TB, agentID string) runtimeagentintent.Resolved {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+strings.TrimSpace(agentID)+".intent",
		"Settle the exact delivery lifecycle conformance route.",
	)
	if err != nil {
		t.Fatalf("resolve delivery lifecycle agent intent: %v", err)
	}
	return intent
}

func deliveryLifecycleConformanceBackends(t *testing.T) []deliveryLifecycleConformanceBackend {
	t.Helper()
	sqlite, sqliteRestart := storetest.StartSQLiteRuntimeStorePair(t)
	_, postgresDB, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	postgres := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
	postgresRestart := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
	return []deliveryLifecycleConformanceBackend{
		{name: "sqlite", store: sqlite, restart: sqliteRestart, selected: sqlite, db: storetest.DatabaseForTest(sqlite)},
		{name: "postgres", store: postgres, restart: postgresRestart, selected: postgres, db: storetest.DatabaseForTest(postgres), postgres: true},
	}
}

func requireCanonicalDeliveryLifecycleSurface(t *testing.T, ctx context.Context, pg *store.PostgresStore) {
	t.Helper()
	storetest.BootstrapPostgresRuntimeStore(t, pg)
	requireTableColumns(t, ctx, storetest.DatabaseForTest(pg), "event_deliveries",
		"delivery_id", "event_id", "route_identity", "subscriber_type", "subscriber_id",
		"status", "retry_count", "max_retries", "claim_version", "current_attempt_version",
		"current_attempt_open", "settled_at")
	requireTableColumns(t, ctx, storetest.DatabaseForTest(pg), "event_delivery_attempts",
		"delivery_id", "claim_version", "claim_token", "started_at", "lease_expires_at", "current_delivery_id",
		"active_session_id", "session_delivery_id", "session_run_id", "session_subscriber_type", "session_agent_id", "open_marker", "outcome")
	requireTableColumns(t, ctx, storetest.DatabaseForTest(pg), "event_delivery_outcomes",
		"delivery_id", "claim_version", "outcome", "side_effects", "duration_ms", "settled_at")
}

var (
	_ runtimedelivery.Store = (*store.PostgresStore)(nil)
	_ runtimedelivery.Store = (*store.SQLiteRuntimeStore)(nil)
)
