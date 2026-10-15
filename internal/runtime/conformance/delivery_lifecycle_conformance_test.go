package conformance

import (
	"bytes"
	"context"
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
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type deliveryLifecycleConformanceBackend struct {
	name      string
	store     runtimedelivery.Store
	restart   runtimedelivery.Store
	selected  any
	postgres  bool
	owner     testing.TB
	plan      agenttopology.SourceSetPlan
	authority runtimedelivery.ExecutionAuthority
}

func deliveryLifecycleConformanceRoute(t testing.TB, runID, subscriberType, subscriberID string) events.DeliveryRoute {
	t.Helper()
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(conformanceNode(t, "", subscriberID))}
	if subscriberType == string(runtimedelivery.SubscriberAgent) {
		route.Recipient = events.MustAgentDeliveryRecipient(subscriberID)
		route.AgentIdentity = agentidentitytest.RootRuntimeForRun(t, runID, subscriberID, "delivery-lifecycle-conformance")
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

func assertClaimRenewalReadback(t *testing.T, ctx context.Context, selected runtimedelivery.Store, deliveryID string, result runtimedelivery.ClaimResult) {
	t.Helper()
	persisted, err := selected.Snapshot(ctx, deliveryID)
	if err != nil {
		t.Fatalf("read renewed delivery %s: %v", deliveryID, err)
	}
	// Reclaimability is derived from each read's clock, not persisted claim state.
	persisted.ClaimReclaimable = result.Snapshot.ClaimReclaimable
	if !result.Acknowledged || !result.Renewal.Acknowledged ||
		!reflect.DeepEqual(result.Snapshot, persisted) ||
		!reflect.DeepEqual(result.Claimed.Snapshot, persisted) ||
		!reflect.DeepEqual(result.Renewal.Snapshot, persisted) {
		t.Fatalf("renewed claim receipt diverges from readback: result=%+v persisted=%+v", result, persisted)
	}
}

func TestExecutableDeliveryLifecycleParity(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {

		t.Run(backendName, func(t *testing.T) {
			ctx := runtimecorrelation.WithRuntimeInstanceID(testAuthorActivityContext(context.Background()), authorActivityTestRuntimeInstanceID)
			t.Run("exact_route_claim_settlement_and_outcome", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"agent-a"})

				event := deliveryLifecycleEvent("exact-" + backend.name)
				agent := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "agent-a")
				sibling := agent
				sibling.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:delivery-lifecycle-sibling"}}
				node := deliveryLifecycleConformanceRoute(t, event.RunID(), "node", "node-a")
				commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{agent, sibling, node}, runtimepipelineobligation.ScopeSubscribed)

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
				if !bound.Acknowledged || bound.Snapshot.ActiveSessionID != sessionID {
					t.Fatalf("bound session = %#v, want acknowledged session %q", bound, sessionID)
				}
				settled, err := backend.store.SettleSuccess(ctx, claimed.Claim, []string{"message.sent"}, 25*time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection())
				if err != nil {
					t.Fatalf("settle agent success: %v", err)
				}
				if settled.Status != runtimedelivery.StatusDelivered || !settled.Terminal() {
					t.Fatalf("settled status = %q", settled.Status)
				}
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); !errors.Is(err, runtimedelivery.ErrConflict) {
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
				commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{agent, sibling, node}, runtimepipelineobligation.ScopeSubscribed)
				afterDuplicate, err := backend.store.Snapshot(ctx, agentProof.DeliveryID())
				if err != nil {
					t.Fatalf("snapshot after exact duplicate: %v", err)
				}
				if afterDuplicate.Status != runtimedelivery.StatusDelivered || afterDuplicate.ClaimVersion != claimed.Claim.Version() {
					t.Fatalf("exact duplicate changed lifecycle: %#v", afterDuplicate)
				}
			})

			t.Run("singular_final_selection_allows_new_attempt_and_fences_terminal_rewrite", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, nil)

				facts := []handlerselection.HandlerRuleSelectionFact{
					deliveryLifecycleSelectedRuleFact(t, handlerselection.ContextRules, `nodes["conformance"].handlers["proof"].rules[0]`, "rules-label"),
					deliveryLifecycleSelectedRuleFact(t, handlerselection.ContextOnComplete, `nodes["conformance"].handlers["proof"].on_complete[0]`, "complete-label"),
					deliveryLifecycleSelectedRuleFact(t, handlerselection.ContextJoinComplete, `nodes["conformance"].handlers["proof"].join.on_complete[0]`, "join-complete"),
					deliveryLifecycleSelectedRuleFact(t, handlerselection.ContextJoinTimeout, `nodes["conformance"].handlers["proof"].join.timeout[0]`, "join-timeout"),
					mustHandlerSelectionNoMatch(t, handlerselection.ContextRules),
					mustHandlerSelectionNoMatch(t, handlerselection.ContextOnComplete),
					handlerselection.NotApplicable(),
				}
				for index, fact := range facts {
					event := deliveryLifecycleEvent(fmt.Sprintf("rule-fact-%s-%d", backend.name, index))
					route := deliveryLifecycleConformanceRoute(t, event.RunID(), "node", fmt.Sprintf("rule-node-%d", index))
					commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
					claimed, err := storetest.ClaimDelivery(ctx, backend.store, event, route)
					if err != nil {
						t.Fatalf("claim fact %d: %v", index, err)
					}
					if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0, fact); err != nil {
						t.Fatalf("settle fact %d: %v", index, err)
					}
					if got := loadDeliveryHandlerRuleSelectionFact(t, ctx, backend, claimed.Snapshot.DeliveryID); !got.Equal(fact) {
						t.Fatalf("persisted fact %d = %#v, want %#v", index, got, fact)
					}
					restarted, err := backend.restart.Snapshot(ctx, claimed.Snapshot.DeliveryID)
					if err != nil || restarted.Status != runtimedelivery.StatusDelivered {
						t.Fatalf("restart snapshot %d = %#v, %v", index, restarted, err)
					}
				}

				event := deliveryLifecycleEvent("rule-conflict-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, event.RunID(), "node", "rule-conflict")
				commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
				firstClaim, err := storetest.ClaimDelivery(ctx, backend.store, event, route)
				if err != nil {
					t.Fatal(err)
				}
				firstFact := deliveryLifecycleEvaluationFailedFact(t, handlerselection.ContextRules, `nodes["conformance"].handlers["proof"].rules[1]`, "failed-evaluation")
				failed, err := backend.store.SettleFailure(ctx, firstClaim.Claim, runtimedelivery.Settlement{
					Disposition:   runtimedelivery.FailureRetry,
					Failure:       testFailure("selection_retry"),
					RetryBase:     time.Nanosecond,
					RuleSelection: handlerselection.Resolved(firstFact),
				})
				if err != nil || failed.Status != runtimedelivery.StatusFailed {
					t.Fatalf("first selection settlement = %#v, %v", failed, err)
				}
				assertDeliveryHandlerRuleSelectionMissing(t, ctx, backend, firstClaim.Snapshot.DeliveryID)
				makeDeliveryImmediatelyEligible(t, ctx, backend, firstClaim.Snapshot.DeliveryID)
				secondClaim, err := storetest.ClaimDelivery(ctx, backend.restart, event, route)
				if err != nil {
					t.Fatal(err)
				}
				contradiction := deliveryLifecycleSelectedRuleFact(t, handlerselection.ContextRules, `nodes["conformance"].handlers["proof"].rules[2]`, "changed")
				if _, err := backend.restart.SettleSuccess(ctx, secondClaim.Claim, nil, 0, contradiction); err != nil {
					t.Fatalf("valid new attempt could not commit its actual selection: %v", err)
				}
				if got := loadDeliveryHandlerRuleSelectionFact(t, ctx, backend, firstClaim.Snapshot.DeliveryID); !got.Equal(contradiction) {
					t.Fatalf("final attempt selection was not persisted: %#v", got)
				}
				outcomes, err := backend.restart.Outcomes(ctx, firstClaim.Snapshot.DeliveryID)
				if err != nil || len(outcomes) != 2 || outcomes[0].Outcome != "retry_scheduled" || outcomes[1].Outcome != "delivered" {
					t.Fatalf("retry/final outcomes = %#v, %v", outcomes, err)
				}
				if _, err := backend.restart.SettleFailure(ctx, secondClaim.Claim, runtimedelivery.Settlement{
					Disposition:   runtimedelivery.FailureDeadLetter,
					ReasonCode:    "selection_evaluation_failed",
					Failure:       testFailure("selection_evaluation_failed"),
					RuleSelection: handlerselection.Resolved(firstFact),
				}); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("terminal rewrite must remain fenced: %v", err)
				}
				if got := loadDeliveryHandlerRuleSelectionFact(t, ctx, backend, firstClaim.Snapshot.DeliveryID); !got.Equal(contradiction) {
					t.Fatalf("terminal rewrite changed final fact = %#v", got)
				}
			})

			t.Run("delivery_continuation_restart_preserves_exact_payload_bytes", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"continuation-payload", "continuation-payload-sibling"})

				payload := json.RawMessage("{\n  \"numeric\": 1.0, \"ordered\": {\"b\": 2, \"a\": 1}\n}")
				event := deliveryLifecycleEventWithPayload("continuation-payload-"+backend.name, payload)
				route := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "continuation-payload")
				sibling := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "continuation-payload-sibling")
				commitDeliveryLifecycleEvent(
					t,
					ctx,
					backend,
					event,
					[]events.DeliveryRoute{route, sibling},
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
				siblingID, err := runtimedelivery.DeliveryID(event.ID(), sibling)
				if err != nil {
					t.Fatal(err)
				}
				found := make(map[string]bool)
				for _, item := range page.Items {
					if item.DeliveryID != deliveryID && item.DeliveryID != siblingID {
						continue
					}
					if item.Disposition != runtimedelivery.ClaimAcquired || !bytes.Equal(item.Event.Payload(), payload) {
						t.Fatalf("continuation %s = %#v, want acquired with exact payload bytes", item.DeliveryID, item)
					}
					found[item.DeliveryID] = true
				}
				if !found[deliveryID] || !found[siblingID] {
					t.Fatalf("continuation page = %#v, want both deliveries for event %s", page, event.ID())
				}
			})

			t.Run("normal_authority_activation_reclaims_predecessor_attempt", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"activation-reclaim-agent"})

				event := deliveryLifecycleEvent("activation-reclaim-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "activation-reclaim-agent")
				commitDeliveryLifecycleEvent(
					t,
					ctx,
					backend,
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
					predecessor.SourceArtifact(),
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
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("predecessor settlement error = %v, want stale claim conflict", err)
				}
				if _, err := backend.restart.SettleSuccess(ctx, reclaimed.Claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatalf("settle successor delivery: %v", err)
				}
			})

			t.Run("class_retry_budgets_are_structural", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"retry-agent"})

				assertDeliveryRetryBudget(t, ctx, backend, runtimedelivery.SubscriberAgent, "retry-agent", runtimedelivery.AgentMaxRetries)
				assertDeliveryRetryBudget(t, ctx, backend, runtimedelivery.SubscriberNode, "retry-node", runtimedelivery.NodeMaxRetries)
			})

			t.Run("closed_claim_and_cursor_disposition_matrix", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"claim-matrix-pending", "claim-matrix-deferred", "claim-matrix-invalid"})

				pendingEvent := deliveryLifecycleEvent("claim-matrix-pending-" + backend.name)
				pendingRoute := deliveryLifecycleConformanceRoute(t, pendingEvent.RunID(), "agent", "claim-matrix-pending")
				commitDeliveryLifecycleEvent(t, ctx, backend, pendingEvent, []events.DeliveryRoute{pendingRoute}, runtimepipelineobligation.ScopeSubscribed)
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
					pending.Authority.SourceArtifact(),
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
				assertClaimRenewalReadback(t, ctx, backend.store, pendingID, acquired)
				busy, err := backend.restart.ClaimDelivery(ctx, pending.Authority, pendingEvent, pendingRoute)
				if err != nil || busy.Disposition != runtimedelivery.ClaimBusy {
					t.Fatalf("busy claim = %#v, err=%v", busy, err)
				}
				expireDeliveryClaimForConformance(t, ctx, backend, acquired.Claimed.Claim)
				reclaimed, err := backend.restart.ClaimDelivery(ctx, pending.Authority, pendingEvent, pendingRoute)
				if err != nil || reclaimed.Disposition != runtimedelivery.ClaimAcquired ||
					reclaimed.Previous != runtimedelivery.ClaimReclaimable {
					t.Fatalf("reclaim claim = %#v, err=%v", reclaimed, err)
				}
				assertClaimRenewalReadback(t, ctx, backend.restart, pendingID, reclaimed)
				reclaimedObligation, ok := reclaimed.Acquired()
				if !ok {
					t.Fatalf("reclaim result has no exact claim: %#v", reclaimed)
				}
				delivered, err := backend.restart.SettleSuccess(ctx, reclaimedObligation.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection())
				if err != nil || !delivered.Terminal() {
					t.Fatalf("settle reclaimed delivery = %#v, err=%v", delivered, err)
				}
				terminal, err := backend.store.ClaimDelivery(ctx, pending.Authority, pendingEvent, pendingRoute)
				if err != nil || terminal.Disposition != runtimedelivery.ClaimTerminal {
					t.Fatalf("terminal claim = %#v, err=%v", terminal, err)
				}

				deferredEvent := deliveryLifecycleEvent("claim-matrix-deferred-" + backend.name)
				deferredRoute := deliveryLifecycleConformanceRoute(t, deferredEvent.RunID(), "agent", "claim-matrix-deferred")
				commitDeliveryLifecycleEvent(t, ctx, backend, deferredEvent, []events.DeliveryRoute{deferredRoute}, runtimepipelineobligation.ScopeSubscribed)
				deferredClaim, err := storetest.ClaimDelivery(ctx, backend.store, deferredEvent, deferredRoute)
				if err != nil {
					t.Fatal(err)
				}
				deferredSnapshot, err := backend.store.SettleFailure(ctx, deferredClaim.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureRetry,
					Failure:     testFailure("handler_failed"),
					RetryBase:   time.Hour, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
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
				invalidRoute := deliveryLifecycleConformanceRoute(t, invalidEvent.RunID(), "agent", "claim-matrix-invalid")
				commitDeliveryLifecycleEvent(t, ctx, backend, invalidEvent, []events.DeliveryRoute{invalidRoute}, runtimepipelineobligation.ScopeSubscribed)
				invalidID, err := runtimedelivery.DeliveryID(invalidEvent.ID(), invalidRoute)
				if err != nil {
					t.Fatal(err)
				}
				if err := storetest.CorruptDeliveryConformanceRoute(ctx, backend.selected, invalidID); err != nil {
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
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"inventory-pending", "inventory-failed", "inventory-busy"})

				event := deliveryLifecycleEvent("recovery-inventory-" + backend.name)
				pendingRoute := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "inventory-pending")
				failedRoute := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "inventory-failed")
				busyRoute := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "inventory-busy")
				commitDeliveryLifecycleEvent(
					t,
					ctx,
					backend,
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
				source := pendingSnapshot.Authority.SourceArtifact()
				before, err := backend.store.InspectDeliveryRecovery(ctx, source)
				if err != nil {
					t.Fatalf("inspect initial delivery recovery inventory: %v", err)
				}
				beforeRun := deliveryRecoveryInventoryRun(t, before, event.RunID())

				failedClaim, err := storetest.ClaimDelivery(ctx, backend.store, event, failedRoute)
				if err != nil {
					t.Fatalf("claim failed inventory route: %v", err)
				}
				failedSnapshot, err := backend.store.SettleFailure(ctx, failedClaim.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureRetry,
					Failure:     testFailure("inventory_retry"),
					RetryBase:   10 * time.Second, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
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
				want := runtimedelivery.RecoveryRunInventory{
					RunID:      beforeRun.RunID,
					Pending:    beforeRun.Pending - 2,
					Failed:     beforeRun.Failed + 1,
					InProgress: beforeRun.InProgress + 1,
				}
				if got := deliveryRecoveryInventoryRun(t, after, event.RunID()); got != want {
					t.Fatalf("delivery recovery inventory for run %s = %#v, want %#v", event.RunID(), got, want)
				}

				foreignSource, err := runtimecorrelation.NewSourceArtifactFact(
					"bundle-v2:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
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
					longClaimRoute := deliveryLifecycleConformanceRoute(t, longEvent.RunID(), "node", "long-claim")
					longRetryRoute := deliveryLifecycleConformanceRoute(t, longEvent.RunID(), "node", "long-retry")
					longLeaseRoute := deliveryLifecycleConformanceRoute(t, longEvent.RunID(), "node", "long-lease")
					commitDeliveryLifecycleEvent(
						t,
						ctx,
						backend,
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
					if err := storetest.ProveDeliveryConformanceClock(ctx, backend.selected, storetest.DeliveryConformanceClockInput{
						Event: longEvent, Snapshot: longClaimSnapshot, ClaimRoute: longClaimRoute, RetryClaim: longRetryClaim, LeaseClaim: longLeaseClaim, Failure: testFailure("long_transaction_retry"),
					}); err != nil {
						t.Fatalf("commit long PostgreSQL delivery mutation: %v", err)
					}
				}
			})

			t.Run("postcommit_handoff_restarts_before_prior_cursor", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"old-route", "new-route"})

				oldEvent := deliveryLifecycleEvent("handoff-before-cursor-" + backend.name)
				oldRoute := deliveryLifecycleConformanceRoute(t, oldEvent.RunID(), "agent", "old-route")
				commitDeliveryLifecycleEvent(
					t,
					ctx,
					backend,
					oldEvent,
					[]events.DeliveryRoute{oldRoute},
					runtimepipelineobligation.ScopeSubscribed,
				)
				// SQLite stores delivery timestamps at millisecond precision. Keep
				// the cursor-order proof from collapsing both inserts into one key.
				time.Sleep(2 * time.Millisecond)
				newEvent := eventtest.ChildForProducerWithRoutingSource(
					eventtest.UUID("handoff-after-cursor-"+backend.name),
					events.EventType("delivery.conformance"),
					eventtest.Producer(events.EventProducerNode, "cursor-proof"),
					"",
					json.RawMessage(`{"ok":true}`),
					0,
					events.LineageFromEvent(oldEvent),
					oldEvent.Envelope(), oldEvent.RoutingSource(),
					oldEvent.CreatedAt().Add(time.Second),
				)
				newRoute := deliveryLifecycleConformanceRoute(t, newEvent.RunID(), "agent", "new-route")
				commitDeliveryLifecycleEvent(
					t,
					ctx,
					backend,
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
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"renewal-agent"})

				event := deliveryLifecycleEvent("claim-renewal-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "renewal-agent")
				commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
				claimed, err := storetest.ClaimDelivery(ctx, backend.store, event, route)
				if err != nil {
					t.Fatalf("claim delivery: %v", err)
				}
				agedAt := ageDeliveryClaimForConformance(t, ctx, backend, claimed.Claim)
				renewal, err := backend.store.RenewClaim(ctx, claimed.Claim)
				if err != nil {
					t.Fatalf("renew claim: %v", err)
				}
				if !renewal.Acknowledged {
					t.Fatal("renew claim returned no acknowledged commit")
				}
				renewed := renewal.Snapshot
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

				expireDeliveryClaimForConformance(t, ctx, backend, claimed.Claim)
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
				settled, err := backend.restart.SettleSuccess(ctx, reclaimed.Claim, []string{"renewal.proven"}, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection())
				if err != nil || settled.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("settle renewed lifecycle = %#v, err=%v", settled, err)
				}
				assertDeliveryAttemptHistory(t, ctx, backend, settled.DeliveryID)
			})

			t.Run("parent_terminalization_fences_late_writer", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"terminal-agent"})

				event := deliveryLifecycleEvent("terminalize-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "terminal-agent")
				commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
				claimed, err := storetest.ClaimDelivery(ctx, backend.store, event, route)
				if err != nil {
					t.Fatal(err)
				}
				removeFault := installDeliveryDeadLetterFault(t, ctx, backend)
				if _, err := backend.store.TerminalizeRun(ctx, event.RunID(), "run_terminal"); err == nil {
					t.Fatal("run terminalization succeeded while required diagnostic writer was faulted")
				}
				assertDeliverySettlementRolledBack(t, ctx, backend, claimed)
				assertDeliveryHandlerRuleSelectionMissing(t, ctx, backend, claimed.Snapshot.DeliveryID)
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
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("late settlement error = %v, want ErrConflict", err)
				}
				outcomes, err := backend.store.Outcomes(ctx, transitions[0].Current.DeliveryID)
				if err != nil || len(outcomes) != 1 || outcomes[0].Outcome != "terminalized" || outcomes[0].ClaimVersion != current.ClaimVersion || outcomes[0].Failure == nil {
					t.Fatalf("terminalization outcomes = %#v, err=%v", outcomes, err)
				}
				assertExactDeliveryDeadLetter(t, ctx, backend, event, current)
				if got := loadDeliveryHandlerRuleSelectionFact(t, ctx, backend, current.DeliveryID); !got.Equal(handlerselection.NotApplicable()) {
					t.Fatalf("terminalization fact = %#v, want not-applicable", got)
				}
			})

			t.Run("parent_terminalization_owns_pending_and_failed_resolution", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, nil)

				pendingEvent := deliveryLifecycleEvent("terminalize-pending-" + backend.name)
				pendingRoute := deliveryLifecycleConformanceRoute(t, pendingEvent.RunID(), "node", "terminal-pending")
				commitDeliveryLifecycleEvent(t, ctx, backend, pendingEvent, []events.DeliveryRoute{pendingRoute}, runtimepipelineobligation.ScopeSubscribed)
				pendingID, err := runtimedelivery.DeliveryID(pendingEvent.ID(), pendingRoute)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := backend.store.TerminalizeRun(ctx, pendingEvent.RunID(), "run_terminal"); err != nil {
					t.Fatalf("terminalize pending delivery: %v", err)
				}
				if got := loadDeliveryHandlerRuleSelectionFact(t, ctx, backend, pendingID); !got.Equal(handlerselection.NotApplicable()) {
					t.Fatalf("pending terminalization fact = %#v", got)
				}

				failedEvent := deliveryLifecycleEvent("terminalize-failed-" + backend.name)
				failedRoute := deliveryLifecycleConformanceRoute(t, failedEvent.RunID(), "node", "terminal-failed")
				commitDeliveryLifecycleEvent(t, ctx, backend, failedEvent, []events.DeliveryRoute{failedRoute}, runtimepipelineobligation.ScopeSubscribed)
				claimed, err := storetest.ClaimDelivery(ctx, backend.store, failedEvent, failedRoute)
				if err != nil {
					t.Fatal(err)
				}
				fact := deliveryLifecycleSelectedRuleFact(t, handlerselection.ContextRules, `nodes["conformance"].handlers["proof"].rules[3]`, "preserved")
				failed, err := backend.store.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureRetry, Failure: testFailure("retry_before_terminalization"),
					RetryBase: time.Hour, RuleSelection: handlerselection.Resolved(fact),
				})
				if err != nil || failed.Status != runtimedelivery.StatusFailed {
					t.Fatalf("settle retry before terminalization = %#v, %v", failed, err)
				}
				assertDeliveryHandlerRuleSelectionMissing(t, ctx, backend, claimed.Snapshot.DeliveryID)
				if _, err := backend.restart.TerminalizeRun(ctx, failedEvent.RunID(), "run_terminal"); err != nil {
					t.Fatalf("terminalize failed delivery: %v", err)
				}
				if got := loadDeliveryHandlerRuleSelectionFact(t, ctx, backend, claimed.Snapshot.DeliveryID); !got.Equal(handlerselection.NotApplicable()) {
					t.Fatalf("terminalization must record its own resolution, not the discarded attempt: %#v", got)
				}
			})

			t.Run("concurrent_claim_and_restart_reclaim_are_fenced", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"race-agent"})

				event := deliveryLifecycleEvent("claim-race-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "race-agent")
				commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)

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
				expireDeliveryClaimForConformance(t, ctx, backend, winner.Claim)
				reclaimed, err := storetest.ClaimDelivery(ctx, backend.restart, event, route)
				if err != nil {
					t.Fatalf("post-expiry reconstructed-store reclaim: %v", err)
				}
				if reclaimed.Claim.Version() != winner.Claim.Version()+1 || reclaimed.Snapshot.DeliveryID != winner.Snapshot.DeliveryID {
					t.Fatalf("reclaimed delivery = %#v, first = %#v", reclaimed, winner)
				}
				if _, err := backend.store.SettleSuccess(ctx, winner.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("expired claimant settlement error = %v, want ErrConflict", err)
				}
				settled, err := backend.restart.SettleSuccess(ctx, reclaimed.Claim, []string{"race.proven"}, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection())
				if err != nil || settled.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("current claimant settlement = %#v, err=%v", settled, err)
				}
				assertDeliveryAttemptHistory(t, ctx, backend, settled.DeliveryID)
			})

			t.Run("fresh_schema_rejects_disconnected_lifecycle_facts", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"schema-agent-" + backendName, "schema-other-agent-" + backendName, "terminated-session-agent-" + backendName})

				assertDeliverySchemaRejectsDisconnectedFacts(t, ctx, backend)
			})

			t.Run("expired_claim_precedes_continuous_pending_backlog", func(t *testing.T) {
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, deliveryLifecycleSelectorAgents(backendName))

				expiredEvent := deliveryLifecycleEvent("selector-expired-" + backend.name)
				route := deliveryLifecycleConformanceRoute(t, expiredEvent.RunID(), "agent", "selector-agent-"+backend.name)
				routes := []events.DeliveryRoute{route}
				for index := 0; index < 12; index++ {
					routes = append(routes, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(fmt.Sprintf("selector-pending-%s-%02d", backend.name, index)), AgentIdentity: agentidentitytest.RootRuntimeForRun(
						t,
						expiredEvent.RunID(),
						fmt.Sprintf("selector-pending-%s-%02d", backend.name, index),
						"delivery-lifecycle-conformance",
					),
					})
				}
				commitDeliveryLifecycleEvent(
					t,
					ctx,
					backend,
					expiredEvent,
					routes,
					runtimepipelineobligation.ScopeSubscribed,
					storetest.AcknowledgedPipelineDisposition(),
				)
				claimed, err := storetest.ClaimDelivery(ctx, backend.store, expiredEvent, route)
				if err != nil {
					t.Fatalf("claim selector expiry candidate: %v", err)
				}
				expireDeliveryClaimForConformance(t, ctx, backend, claimed.Claim)
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
				backend := deliveryLifecycleConformanceBackends(t, backendName)
				backend.admitPlan(t, ctx, []string{"diagnostic-agent"})

				for _, class := range []runtimedelivery.SubscriberClass{runtimedelivery.SubscriberAgent, runtimedelivery.SubscriberNode} {
					class := class
					t.Run(string(class), func(t *testing.T) {
						event := deliveryLifecycleEvent(fmt.Sprintf("atomic-diagnostic-%s-%s", backend.name, class))
						route := deliveryLifecycleConformanceRoute(t, event.RunID(), string(class), "diagnostic-"+string(class))
						commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
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
							Failure:     testFailure("terminal_test_failure"), RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
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
	cleanup := func() {
		if err := storetest.RemoveDeliveryDiagnosticFault(context.WithoutCancel(ctx), backend.selected); err != nil {
			t.Errorf("drop delivery diagnostic fault trigger: %v", err)
		}
	}
	t.Cleanup(cleanup)
	if err := storetest.InstallDeliveryDiagnosticFault(ctx, backend.selected); err != nil {
		t.Fatalf("create delivery diagnostic fault trigger: %v", err)
	}
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
	stored, err := storetest.ReadDeliveryRollbackStorage(ctx, backend.selected, claimed.Snapshot.DeliveryID)
	if err != nil {
		t.Fatalf("read faulted settlement evidence: %v", err)
	}
	outcomes, diagnostics, transitions := stored.Outcomes, stored.Diagnostics, stored.Transitions
	if outcomes != 0 || diagnostics != 0 || transitions != 0 {
		t.Fatalf("faulted settlement evidence outcomes=%d diagnostics=%d transitions=%d, want all zero", outcomes, diagnostics, transitions)
	}
}

func assertExactDeliveryDeadLetter(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, event events.Event, settled runtimedelivery.Snapshot) {
	t.Helper()
	stored, err := storetest.ReadExactDeliveryDeadLetterStorage(ctx, backend.selected, settled.DeliveryID, settled.ClaimVersion)
	if err != nil {
		t.Fatalf("read exact terminal delivery diagnostic: %v", err)
	}
	deliveryID, eventType, handler := stored.DeliveryID, stored.EventType, stored.Handler
	claimVersion, payload, failureRaw := stored.ClaimVersion, stored.Payload, stored.Failure
	retryCount, chainDepth := stored.RetryCount, stored.ChainDepth
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
	for owner, read := range map[string]func() ([]byte, error){
		"attempt": func() ([]byte, error) {
			return storetest.ReadDeliveryAttemptFailureStorage(ctx, backend.selected, settled.DeliveryID, settled.ClaimVersion)
		},
		"author activity": func() ([]byte, error) {
			return storetest.ReadLatestDeliveryActivityFailureStorage(ctx, backend.selected, settled.DeliveryID)
		},
	} {
		raw, err := read()
		if err != nil {
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
	event := deliveryLifecycleEvent("schema-event-" + backend.name)
	route := deliveryLifecycleConformanceRoute(t, event.RunID(), "agent", "schema-agent-"+backend.name)
	other := deliveryLifecycleEvent("schema-other-run-" + backend.name)
	commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	commitDeliveryLifecycleEvent(t, ctx, backend, other, nil, runtimepipelineobligation.ScopeDirect)
	proof, err := backend.store.ProveHandoff(ctx, event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("persisted handoff proof is incomplete: %v", err)
	}
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintEventRunMismatch, storetest.DeliveryConstraintInput{OtherRunID: other.RunID(), DeliveryID: proof.DeliveryID()})

	missingAttemptEvent := deliveryLifecycleEvent("schema-missing-attempt-" + backend.name)
	missingAttemptRoute := deliveryLifecycleConformanceRoute(t, missingAttemptEvent.RunID(), "agent", "schema-agent-"+backend.name)
	commitDeliveryLifecycleEvent(t, ctx, backend, missingAttemptEvent, []events.DeliveryRoute{missingAttemptRoute}, runtimepipelineobligation.ScopeSubscribed)
	missingAttempt, err := backend.store.ProveHandoff(ctx, missingAttemptEvent.ID(), missingAttemptRoute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintMissingOpenAttempt, storetest.DeliveryConstraintInput{At: now, DeliveryID: missingAttempt.DeliveryID()})

	sessionEvent := deliveryLifecycleEvent("schema-session-" + backend.name)
	sessionRoute := deliveryLifecycleConformanceRoute(t, sessionEvent.RunID(), "agent", "schema-agent-"+backend.name)
	commitDeliveryLifecycleEvent(t, ctx, backend, sessionEvent, []events.DeliveryRoute{sessionRoute}, runtimepipelineobligation.ScopeSubscribed)
	claimed, err := storetest.ClaimDelivery(ctx, backend.store, sessionEvent, sessionRoute)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintMissingSession, storetest.DeliveryConstraintInput{SessionID: uuid.NewString(), RunID: sessionEvent.RunID(), AgentID: sessionRoute.Recipient.ID(), DeliveryID: claimed.Snapshot.DeliveryID, Version: claimed.Claim.Version()})

	otherSessionEvent := deliveryLifecycleEvent("schema-session-other-owner-" + backend.name)
	commitDeliveryLifecycleEvent(t, ctx, backend, otherSessionEvent, nil, runtimepipelineobligation.ScopeDirect)
	otherSessionID := uuid.NewString()
	otherAgentID := "schema-other-agent-" + backend.name
	seedDeliveryAgentSession(t, ctx, backend, otherSessionID, otherSessionEvent.RunID(), otherAgentID)
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintForeignSession, storetest.DeliveryConstraintInput{SessionID: otherSessionID, DeliveryID: claimed.Snapshot.DeliveryID, RunID: otherSessionEvent.RunID(), AgentID: otherAgentID, Version: claimed.Claim.Version()})
	if _, err := backend.store.BindAgentSession(ctx, claimed.Claim, otherSessionID); !errors.Is(err, runtimedelivery.ErrConflict) {
		t.Fatalf("bind session owned by another delivery error = %v, want ErrConflict", err)
	}

	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintSecondOpenAttempt, storetest.DeliveryConstraintInput{DeliveryID: claimed.Snapshot.DeliveryID, Version: claimed.Claim.Version() + 1, Token: uuid.NewString(), At: now, ExpiresAt: now.Add(time.Minute)})

	terminatedEvent := deliveryLifecycleEvent("schema-terminated-session-" + backend.name)
	terminatedRoute := deliveryLifecycleConformanceRoute(t, terminatedEvent.RunID(), "agent", "terminated-session-agent-"+backend.name)
	commitDeliveryLifecycleEvent(t, ctx, backend, terminatedEvent, []events.DeliveryRoute{terminatedRoute}, runtimepipelineobligation.ScopeSubscribed)
	terminatedClaim, err := storetest.ClaimDelivery(ctx, backend.store, terminatedEvent, terminatedRoute)
	if err != nil {
		t.Fatal(err)
	}
	terminatedSessionID := uuid.NewString()
	seedDeliveryAgentSession(t, ctx, backend, terminatedSessionID, terminatedEvent.RunID(), terminatedRoute.Recipient.ID())
	terminateErr := storetest.TerminateDeliveryConformanceSession(ctx, backend.selected, terminatedSessionID, now)
	if terminateErr != nil {
		t.Fatalf("terminate exact delivery session: %v", terminateErr)
	}
	if _, err := backend.store.BindAgentSession(ctx, terminatedClaim.Claim, terminatedSessionID); !errors.Is(err, runtimedelivery.ErrConflict) {
		t.Fatalf("bind terminated exact session error = %v, want ErrConflict", err)
	}

	outcomeEvent := deliveryLifecycleEvent("schema-outcome-" + backend.name)
	outcomeRoute := deliveryLifecycleConformanceRoute(t, outcomeEvent.RunID(), "agent", "schema-agent-"+backend.name)
	commitDeliveryLifecycleEvent(t, ctx, backend, outcomeEvent, []events.DeliveryRoute{outcomeRoute}, runtimepipelineobligation.ScopeSubscribed)
	outcomeProof, err := backend.store.ProveHandoff(ctx, outcomeEvent.ID(), outcomeRoute)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintMissingSettlementEvidence, storetest.DeliveryConstraintInput{DeliveryID: outcomeProof.DeliveryID(), At: now})
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintMissingOutcome, storetest.DeliveryConstraintInput{DeliveryID: outcomeProof.DeliveryID(), At: now})
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintMissingCompletionTime, storetest.DeliveryConstraintInput{DeliveryID: outcomeProof.DeliveryID(), At: now})
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintExpiredDuration, storetest.DeliveryConstraintInput{DeliveryID: outcomeProof.DeliveryID(), At: now})
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintOpenDeadLetter, storetest.DeliveryConstraintInput{RecordID: uuid.NewString(), EventID: sessionEvent.ID(), DeliveryID: claimed.Snapshot.DeliveryID, Version: claimed.Claim.Version()})
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintPartialSettlementReference, storetest.DeliveryConstraintInput{RecordID: uuid.NewString(), EventID: sessionEvent.ID(), DeliveryID: claimed.Snapshot.DeliveryID})
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintContradictoryClosure, storetest.DeliveryConstraintInput{DeliveryID: claimed.Snapshot.DeliveryID, Version: claimed.Claim.Version()})

	deadLetterEvent := deliveryLifecycleEvent("schema-dead-letter-failure-" + backend.name)
	deadLetterRoute := deliveryLifecycleConformanceRoute(t, deadLetterEvent.RunID(), "agent", "schema-agent-"+backend.name)
	commitDeliveryLifecycleEvent(t, ctx, backend, deadLetterEvent, []events.DeliveryRoute{deadLetterRoute}, runtimepipelineobligation.ScopeSubscribed)
	deadLetterProof, err := backend.store.ProveHandoff(ctx, deadLetterEvent.ID(), deadLetterRoute)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintMissingTypedFailure, storetest.DeliveryConstraintInput{DeliveryID: deadLetterProof.DeliveryID()})
}

func assertDeliverySQLRejected(t *testing.T, backend deliveryLifecycleConformanceBackend, name storetest.DeliveryConstraintCase, input storetest.DeliveryConstraintInput) {
	t.Helper()
	input.Case = name
	rejected, err := storetest.ProbeDeliveryConstraint(testAuthorActivityContext(context.Background()), backend.selected, input)
	if err != nil {
		t.Fatalf("%s constraint probe failed outside expected schema rejection: %v", name, err)
	}
	if !rejected {
		t.Fatalf("%s mutation succeeded; fresh schema must reject it", name)
	}
}

func assertDeliveryRetryBudget(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, class runtimedelivery.SubscriberClass, subscriberID string, maxRetries int) {
	t.Helper()
	event := deliveryLifecycleEvent(fmt.Sprintf("retry-%s-%s", backend.name, class))
	route := deliveryLifecycleConformanceRoute(t, event.RunID(), string(class), subscriberID)
	commitDeliveryLifecycleEvent(t, ctx, backend, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
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
			RetryBase:   time.Nanosecond, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
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
	if err := storetest.MakeDeliveryRecoveryRetryEligible(ctx, backend.selected, deliveryID); err != nil {
		t.Fatalf("make retry eligible: %v", err)
	}
}

func ageDeliveryClaimForConformance(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, claim runtimedelivery.Claim) time.Time {
	t.Helper()
	agedAt := time.Now().Add(-15 * time.Minute).UTC()
	if err := storetest.AgeManagerClaimStartedAt(ctx, backend.selected, claim, agedAt); err != nil {
		t.Fatalf("commit aged-claim proof: %v", err)
	}
	return agedAt
}

func expireDeliveryClaimForConformance(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, claim runtimedelivery.Claim) {
	t.Helper()
	if err := storetest.ExpireExactDeliveryClaimFault(ctx, backend.selected, claim); err != nil {
		t.Fatalf("commit claim-expiry proof: %v", err)
	}
}

func assertDeliveryAttemptHistory(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	rows, err := storetest.ReadDeliveryConformanceAttemptsStorage(ctx, backend.selected, deliveryID)
	if err != nil {
		t.Fatalf("load delivery attempt history: %v", err)
	}
	type attempt struct {
		version int64
		closure string
		outcome string
	}
	var attempts []attempt
	for _, row := range rows {
		current := attempt{version: row.Version, closure: row.Closure, outcome: row.Outcome}
		attempts = append(attempts, current)
	}
	if len(attempts) != 2 || attempts[0] != (attempt{version: 1, closure: "lease_expired", outcome: "lease_expired"}) || attempts[1] != (attempt{version: 2, closure: "settled", outcome: "delivered"}) {
		t.Fatalf("delivery attempt history = %#v", attempts)
	}
	outcomes, err := backend.store.Outcomes(ctx, deliveryID)
	if err != nil || len(outcomes) != 1 || outcomes[0].ClaimVersion != 2 || outcomes[0].Outcome != "delivered" {
		t.Fatalf("expired attempt appeared as settlement: outcomes=%#v err=%v", outcomes, err)
	}
	assertDeliverySQLRejected(t, backend, storetest.DeliveryConstraintExpiredDeadLetter, storetest.DeliveryConstraintInput{RecordID: uuid.NewString(), DeliveryID: deliveryID})
}

func assertDeliveryAttemptLeaseMatchesObligation(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string, version int64) {
	t.Helper()
	matches, err := storetest.CountMatchingOpenDeliveryAttemptStorage(ctx, backend.selected, deliveryID, version)
	if err != nil {
		t.Fatalf("compare renewed delivery attempt lease: %v", err)
	}
	if matches != 1 {
		t.Fatalf("matching renewed attempt and obligation leases = %d, want 1", matches)
	}
}

func deliveryRecoveryInventoryRun(t testing.TB, inventory runtimedelivery.RecoveryInventory, runID string) runtimedelivery.RecoveryRunInventory {
	t.Helper()
	for _, run := range inventory.Runs {
		if run.RunID == runID {
			return run
		}
	}
	t.Fatalf("delivery recovery inventory has no row for run %s: %#v", runID, inventory)
	return runtimedelivery.RecoveryRunInventory{}
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
	identity := agentidentitytest.RootRuntimeForRun(t, runID, agentID, "delivery-lifecycle-conformance")
	if _, err := identity.StorageFields(); err != nil {
		t.Fatalf("seed delivery agent identity: %v", err)
	}
	backend.admitAgent(t, ctx, runID, agentID)
	if err := storetest.SeedReplayDeliverySession(ctx, backend.selected, sessionID, identity); err != nil {
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

func deliveryLifecycleConformanceBackends(t *testing.T, name string) deliveryLifecycleConformanceBackend {
	t.Helper()
	if name == "sqlite" {
		selected, reopened := storetest.StartSQLiteRuntimeStorePair(t)
		return deliveryLifecycleConformanceBackend{name: name, store: selected, restart: reopened, selected: selected, owner: t}
	}
	if name != "postgres" {
		t.Fatalf("unknown delivery lifecycle backend %q", name)
	}
	selected, reopen := storetest.StartPostgresRuntimeStoreWithReopen(t)
	return deliveryLifecycleConformanceBackend{name: name, store: selected, restart: reopen(), selected: selected, postgres: true, owner: t}
}

func deliveryLifecycleSelectedRuleFact(t testing.TB, context handlerselection.Context, semanticPath, label string) handlerselection.HandlerRuleSelectionFact {
	t.Helper()
	ref, err := runtimeidentity.AdmitDeclarationIdentity("conformance", "handler_rule", semanticPath)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := handlerselection.Selected(context, ref, label)
	if err != nil {
		t.Fatal(err)
	}
	return fact
}

func deliveryLifecycleEvaluationFailedFact(t testing.TB, context handlerselection.Context, semanticPath, label string) handlerselection.HandlerRuleSelectionFact {
	t.Helper()
	ref, err := runtimeidentity.AdmitDeclarationIdentity("conformance", "handler_rule", semanticPath)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := handlerselection.EvaluationFailed(context, ref, label)
	if err != nil {
		t.Fatal(err)
	}
	return fact
}

func mustHandlerSelectionNoMatch(t testing.TB, context handlerselection.Context) handlerselection.HandlerRuleSelectionFact {
	t.Helper()
	fact, err := handlerselection.NoMatch(context)
	if err != nil {
		t.Fatal(err)
	}
	return fact
}

func loadDeliveryHandlerRuleSelectionFact(t testing.TB, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) handlerselection.HandlerRuleSelectionFact {
	t.Helper()
	stored, err := storetest.ReadDeliveryConformanceSelectionStorage(ctx, backend.selected, deliveryID)
	if err != nil {
		t.Fatalf("load handler rule selection %s: %v", deliveryID, err)
	}
	fact, err := handlerselection.Hydrate(stored.Context, stored.Disposition, stored.Flow, stored.Family, stored.Path, stored.Label)
	if err != nil {
		t.Fatalf("hydrate handler rule selection %s: %v", deliveryID, err)
	}
	return fact
}

func assertDeliveryHandlerRuleSelectionMissing(t testing.TB, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	count, err := storetest.CountDeliveryHandlerSelectionsStorage(ctx, backend.selected, deliveryID)
	if err != nil {
		t.Fatalf("count handler rule selection %s: %v", deliveryID, err)
	}
	if count != 0 {
		t.Fatalf("handler rule selection %s count = %d, want 0", deliveryID, count)
	}
}

func requireCanonicalDeliveryLifecycleSurface(t *testing.T, ctx context.Context, pg *store.PostgresStore) {
	t.Helper()
	if err := storetest.CheckDeliveryLifecycleStorageColumns(ctx, pg); err != nil {
		t.Fatal(err)
	}
}

var (
	_ runtimedelivery.Store = (*store.PostgresStore)(nil)
	_ runtimedelivery.Store = (*store.SQLiteRuntimeStore)(nil)
)

func deliveryLifecycleSelectorAgents(backend string) []string {
	agents := []string{"selector-agent-" + backend}
	for index := 0; index < 12; index++ {
		agents = append(agents, fmt.Sprintf("selector-pending-%s-%02d", backend, index))
	}
	return agents
}

func deliveryLifecycleAgentConfig(t testing.TB, runID, agentID string) runtimeactors.AgentConfig {
	t.Helper()
	config := runtimeactors.AgentConfig{
		ID: agentID, Identity: agentidentitytest.RootRuntimeForRun(t, runID, agentID, "delivery-lifecycle-conformance"),
		Role: "delivery-test", Type: "stub", Model: "regular", ExecutionMode: "live",
		ResolvedLLMBackend: "anthropic", Intent: deliveryLifecycleAgentIntent(t, agentID), Config: json.RawMessage(`{}`),
	}
	profile, err := selection.ResolveActiveBackend(config.ResolvedLLMBackend)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := llm.ResolveAgentExecution(executionposture.Live, profile, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	return resolved.Actor
}

func (backend *deliveryLifecycleConformanceBackend) admitPlan(t *testing.T, ctx context.Context, agentIDs []string) {
	t.Helper()
	if len(agentIDs) == 0 {
		return
	}
	selected := backend.selected.(agentfixture.Store)
	if err := sourceartifactfixture.Ensure(ctx, selected); err != nil {
		t.Fatal(err)
	}
	source := agenttopology.SourceCoordinate{BundleHash: authorActivityTestSourceArtifactFact.BundleHash()}
	agents := make([]agenttopology.DesiredAgent, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		config := deliveryLifecycleAgentConfig(t, eventtest.UUID("delivery-lifecycle-plan"), agentID)
		identity, err := config.Identity.Plan()
		if err != nil {
			t.Fatal(err)
		}
		revision, err := runtimemanager.AgentConfigPlanRevision(config, identity)
		if err != nil {
			t.Fatal(err)
		}
		agents = append(agents, agenttopology.DesiredAgent{Identity: identity, Source: source, ConfigRevision: revision})
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{source}, agents)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := agentfixture.AdmitGeneration(backend.owner, ctx, selected, plan, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.AdmitExecution(ctx)
	if err != nil || evidence.State != startupownership.GrantAdmitted {
		t.Fatalf("admit delivery lifecycle fixture grant: %+v err=%v", evidence, err)
	}
	backend.authority, err = runtimedelivery.NewNormalExecutionAuthority(authorActivityTestSourceArtifactFact, evidence.GrantID, evidence.RuntimeGeneration)
	if err != nil {
		t.Fatal(err)
	}
	backend.plan = plan
}

func (backend deliveryLifecycleConformanceBackend) admitAgent(t testing.TB, ctx context.Context, runID, agentID string) {
	t.Helper()
	selected := backend.selected.(agentfixture.Store)
	config := deliveryLifecycleAgentConfig(t, runID, agentID)
	identity, err := config.Identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := runtimemanager.AgentConfigPlanRevision(config, identity)
	if err != nil {
		t.Fatal(err)
	}
	want := agenttopology.DesiredAgent{Identity: identity, Source: agenttopology.SourceCoordinate{BundleHash: authorActivityTestSourceArtifactFact.BundleHash()}, ConfigRevision: revision}
	found := false
	for _, agent := range backend.plan.Agents {
		found = found || reflect.DeepEqual(agent, want)
	}
	if !found {
		t.Fatalf("agent %s is absent from the complete delivery lifecycle fixture plan", agentID)
	}
	state, exists, err := selected.LoadAgentLifecycleState(ctx, config.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		if state.ConfigRevision != revision || state.Phase != runtimemanager.AgentLifecycleRunning {
			t.Fatalf("delivery lifecycle fixture agent is not the admitted configuration: %+v", state)
		}
		return
	}
	storetest.RequireStaticAgentFixture(backend.owner, ctx, selected, runtimemanager.PersistedAgent{
		Config: config, Status: "active", HiredBy: "delivery-lifecycle-conformance", StartedAt: time.Now().UTC(),
	})
}

func commitDeliveryLifecycleEvent(t testing.TB, ctx context.Context, backend deliveryLifecycleConformanceBackend, event events.Event, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, dispositions ...*runtimepipelineobligation.Disposition) {
	t.Helper()
	var disposition *runtimepipelineobligation.Disposition
	if len(dispositions) > 1 {
		t.Fatal("delivery lifecycle fixture requires at most one pipeline disposition")
	}
	if len(dispositions) > 0 {
		disposition = dispositions[0]
	}
	outcome := storetest.CommitSemanticEventWithInitialFacts(t, ctx, backend.selected, event, routes, scope, disposition)
	if outcome == runtimebus.EventAppendExactDuplicate || len(backend.plan.Agents) == 0 || len(routes) == 0 {
		return
	}
	for _, route := range routes {
		if route.Recipient.IsAgent() {
			backend.admitAgent(t, ctx, event.RunID(), route.Recipient.ID())
		}
	}
	if err := backend.store.ActivateDeliveryAuthority(ctx, backend.authority); err != nil {
		t.Fatal(err)
	}
}
