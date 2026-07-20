package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatereply"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestReplyResolutionConformance_BootAndLoweringExposePairedLoop(t *testing.T) {
	source := templatereply.LoadSource(t, templatereply.Options{})
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template reply hard invalidities = %#v, want none", got)
	}
	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("reply lowering issues=%#v, want none", issues)
	}
	roles := map[string]runtimepinrouting.ConnectRoutePlanReplyResolution{}
	for _, plan := range plans {
		if plan.ReplyResolution == nil {
			continue
		}
		if plan.ReplyResolution.Role == runtimepinrouting.ConnectReplyRoleResponse && plan.ResolutionKind != runtimepinrouting.ConnectResolutionReply {
			t.Fatalf("reply response plan resolution = %q, want reply", plan.ResolutionKind)
		}
		roles[plan.ReplyResolution.Role] = *plan.ReplyResolution
	}
	if len(roles) != 2 {
		t.Fatalf("reply lowering plans=%#v, want exactly two paired reply roles", plans)
	}
	for _, role := range []string{runtimepinrouting.ConnectReplyRoleRequest, runtimepinrouting.ConnectReplyRoleResponse} {
		got, ok := roles[role]
		if !ok {
			t.Fatalf("reply role %q missing from %#v", role, roles)
		}
		if got.RequestOutputPin != templatereply.RequesterRequestPin || got.ReplyInputPin != templatereply.RequesterReplyPin || got.ProviderFlowID != templatereply.ProviderFlowID || got.CorrelationKey != "" {
			t.Fatalf("reply role %q pairing = %#v", role, got)
		}
	}
}

func TestReplyResolutionConformance_DefaultCorrelationUsesStableRequestEventID(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	source := templatereply.LoadSource(t, templatereply.Options{DefaultEventIDCorrelation: true})
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("default-correlation hard invalidities = %#v", got)
	}
	store := newReplyConformanceStore()
	eb, err := newScopedTestEventBus(t, store, bus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRouteContext(ctx, bus.FlowInstanceRouteMaterializationRequest{
		Identity:            runtimeflowidentity.StoredRoute(templatereply.RequesterFlowID, "account-a", templatereply.RequesterFlowID+"/account-a"),
		ActivationVariables: map[string]string{"account_id": "account-a"},
	}); err != nil {
		t.Fatalf("materialize requester route: %v", err)
	}
	request := replyConformanceEvent(source.ResolveFlowEventReference(templatereply.RequesterFlowID, templatereply.RequestEvent), uuid.NewString(), templatereply.RequesterFlowID, templatereply.RequesterFlowID+"/account-a", map[string]any{
		"provider_request_id": "ignored-for-default",
		"account_id":          "account-a",
	})
	if err := eb.Publish(ctx, request); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	routes := store.routesFor(request.ID())
	if len(routes) != 1 {
		t.Fatalf("request routes = %#v", routes)
	}
	record, err := store.LoadReplyContext(ctx, routes[0].Context.ReplyContextID())
	if err != nil || record.RequestCorrelationID != request.ID() || record.CorrelationKey != "" {
		t.Fatalf("default correlation record = %#v err=%v", record, err)
	}
}

func TestReplyResolutionConformance_VerifierFailsClosedForInvalidPairedTopology(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts templatereply.Options
		want string
	}{
		{name: "missing replies_to", opts: templatereply.Options{MissingRepliesTo: true}, want: "requires replies_to"},
		{name: "correlation key not carried", opts: templatereply.Options{MissingCorrelationCarry: true}, want: "must name a carry"},
		{name: "ambiguous request counterpart", opts: templatereply.Options{AmbiguousRequestEdge: true}, want: "exactly one connected counterpart"},
		{name: "different provider counterpart", opts: templatereply.Options{MismatchedProvider: true}, want: "same provider flow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := templatereply.LoadSource(t, tc.opts)
			report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
			found := false
			for _, finding := range report.HardInvalidities() {
				if strings.Contains(finding.Message, tc.want) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("hard invalidities = %#v, want message containing %q", report.HardInvalidities(), tc.want)
			}
		})
	}
}

func TestReplyResolutionConformance_RoutesConcurrentSameOriginAndCrossOriginByPersistedContext(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	source := templatereply.LoadSource(t, templatereply.Options{ExplicitCorrelation: true})
	store := newReplyConformanceStore()
	eb, err := newScopedTestEventBus(t, store, bus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	for _, accountID := range []string{"account-a", "account-b"} {
		if err := eb.AddFlowInstanceRouteContext(ctx, bus.FlowInstanceRouteMaterializationRequest{
			Identity:            runtimeflowidentity.StoredRoute(templatereply.RequesterFlowID, accountID, templatereply.RequesterFlowID+"/"+accountID),
			ActivationVariables: map[string]string{"account_id": accountID},
		}); err != nil {
			t.Fatalf("materialize requester route %s: %v", accountID, err)
		}
	}

	type requestCase struct {
		name       string
		accountID  string
		requestID  string
		requestKey string
	}
	cases := []requestCase{
		{name: "same-origin-first", accountID: "account-a", requestID: uuid.NewString(), requestKey: "account-a-request-1"},
		{name: "same-origin-second", accountID: "account-a", requestID: uuid.NewString(), requestKey: "account-a-request-2"},
		{name: "cross-origin", accountID: "account-b", requestID: uuid.NewString(), requestKey: "account-a-request-1"},
	}
	contexts := map[string]events.DeliveryContext{}
	for _, tc := range cases {
		request := replyConformanceEvent(source.ResolveFlowEventReference(templatereply.RequesterFlowID, templatereply.RequestEvent), tc.requestID, templatereply.RequesterFlowID, templatereply.RequesterFlowID+"/"+tc.accountID, map[string]any{
			"provider_request_id": tc.requestKey,
			"account_id":          tc.accountID,
		})
		before := store.createCalls
		preflight, err := eb.CheckPublishRecipientPlan(ctx, request)
		if err != nil {
			t.Fatalf("preflight %s: %v", tc.name, err)
		}
		if store.createCalls != before {
			t.Fatalf("preflight %s persisted reply context", tc.name)
		}
		if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 || preflight.DeliveryRoutes[0].Context.Empty() {
			t.Fatalf("preflight %s = %#v", tc.name, preflight)
		}
		if err := eb.Publish(ctx, request); err != nil {
			t.Fatalf("publish request %s: %v", tc.name, err)
		}
		routes := store.routesFor(request.ID())
		if len(routes) != 1 || routes[0].Context.Empty() {
			t.Fatalf("request %s persisted routes = %#v", tc.name, routes)
		}
		contexts[tc.name] = routes[0].Context
	}
	if contexts["same-origin-first"].ReplyContextID() == contexts["same-origin-second"].ReplyContextID() || contexts["same-origin-first"].ReplyContextID() == contexts["cross-origin"].ReplyContextID() {
		t.Fatalf("concurrent request contexts crossed: %#v", contexts)
	}

	for _, tc := range []requestCase{cases[2], cases[1], cases[0]} {
		replyID := uuid.NewString()
		reply := replyConformanceEvent(source.ResolveFlowEventReference(templatereply.ProviderFlowID, templatereply.ReplyEvent), replyID, templatereply.ProviderFlowID, templatereply.ProviderFlowID, map[string]any{
			"provider_request_id": tc.requestKey,
			"account_id":          tc.accountID,
			"result":              tc.name,
		})
		replyCtx := events.WithDeliveryContext(ctx, contexts[tc.name])
		beforeClaims := store.claimCalls
		preflight, err := eb.CheckPublishRecipientPlan(replyCtx, reply)
		if err != nil {
			t.Fatalf("reply preflight %s: %v", tc.name, err)
		}
		if store.claimCalls != beforeClaims {
			t.Fatalf("reply preflight %s claimed terminal context", tc.name)
		}
		if preflight.TargetFailure != "" {
			t.Fatalf("reply preflight %s failure = %q", tc.name, preflight.TargetFailure)
		}
		if err := eb.Publish(replyCtx, reply); err != nil {
			t.Fatalf("publish reply %s: %v", tc.name, err)
		}
		routes := store.routesFor(reply.ID())
		if len(routes) != 1 || routes[0].Target.FlowInstance != templatereply.RequesterFlowID+"/"+tc.accountID {
			t.Fatalf("reply %s routes = %#v", tc.name, routes)
		}
		if !routes[0].Context.Empty() {
			t.Fatalf("terminal reply %s leaked consumed context: %#v", tc.name, routes[0].Context)
		}
	}
	firstContext := contexts["same-origin-first"]
	lateReply := replyConformanceEvent(source.ResolveFlowEventReference(templatereply.ProviderFlowID, templatereply.ReplyEvent), uuid.NewString(), templatereply.ProviderFlowID, templatereply.ProviderFlowID, map[string]any{
		"provider_request_id": cases[0].requestKey,
		"account_id":          "account-a",
		"result":              "too-late",
	})
	lateCtx := events.WithDeliveryContext(ctx, firstContext)
	beforeClaims := store.claimCalls
	latePlan, err := eb.CheckPublishRecipientPlan(lateCtx, lateReply)
	if err != nil {
		t.Fatalf("late reply preflight: %v", err)
	}
	if store.claimCalls != beforeClaims || latePlan.TargetFailure != string(runtimepinrouting.FailureReplyAlreadyTerminal) {
		t.Fatalf("late reply preflight = %#v claims=%d/%d", latePlan, store.claimCalls, beforeClaims)
	}
	if err := eb.Publish(lateCtx, lateReply); err != nil {
		t.Fatalf("publish late reply: %v", err)
	}
	if routes := store.routesFor(lateReply.ID()); len(routes) != 0 {
		t.Fatalf("late reply persisted routes = %#v, want none", routes)
	}

	staleReply := replyConformanceEvent(source.ResolveFlowEventReference(templatereply.ProviderFlowID, templatereply.ReplyEvent), uuid.NewString(), templatereply.ProviderFlowID, templatereply.ProviderFlowID, map[string]any{
		"provider_request_id": "missing",
		"account_id":          "account-a",
		"result":              "stale",
	})
	staleCtx := events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:missing"}})
	stalePlan, err := eb.CheckPublishRecipientPlan(staleCtx, staleReply)
	if err != nil {
		t.Fatalf("stale reply preflight: %v", err)
	}
	if stalePlan.TargetFailure != string(runtimepinrouting.FailureStaleArrival) {
		t.Fatalf("stale reply failure = %q, want %q", stalePlan.TargetFailure, runtimepinrouting.FailureStaleArrival)
	}
}

func TestReplyResolutionConformance_DurableRestartRoutesOverlappingRequestsOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) durableReplyConformanceStore
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) durableReplyConformanceStore {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) durableReplyConformanceStore {
				return storetest.StartSQLiteRuntimeStore(t)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			source := templatereply.LoadSource(t, templatereply.Options{ExplicitCorrelation: true})
			backend := tc.setup(t)
			runID := uuid.NewString()
			seedDurableReplyConformanceRun(t, ctx, backend, runID)
			ctx = runtimecorrelation.WithRunID(ctx, runID)

			requestBus := newDurableReplyConformanceBus(t, ctx, backend, source)
			type requestCase struct {
				name       string
				accountID  string
				eventID    string
				requestKey string
			}
			requests := []requestCase{
				{name: "same-origin-first", accountID: "account-a", eventID: uuid.NewString(), requestKey: "account-a-request-1"},
				{name: "same-origin-second", accountID: "account-a", eventID: uuid.NewString(), requestKey: "account-a-request-2"},
				{name: "cross-origin", accountID: "account-b", eventID: uuid.NewString(), requestKey: "account-a-request-1"},
			}
			contexts := make(map[string]events.DeliveryContext, len(requests))
			for _, req := range requests {
				evt := replyConformanceEventForRun(
					source.ResolveFlowEventReference(templatereply.RequesterFlowID, templatereply.RequestEvent),
					req.eventID,
					runID,
					templatereply.RequesterFlowID,
					templatereply.RequesterFlowID+"/"+req.accountID,
					map[string]any{
						"provider_request_id": req.requestKey,
						"account_id":          req.accountID,
					},
				)
				if err := requestBus.Publish(ctx, evt); err != nil {
					t.Fatalf("publish request %s: %v", req.name, err)
				}
				routes, err := backend.ListEventDeliveryRoutes(ctx, req.eventID)
				if err != nil || len(routes) != 1 || routes[0].Context.Empty() {
					t.Fatalf("persisted request routes %s = %#v err=%v", req.name, routes, err)
				}
				contexts[req.name] = routes[0].Context
			}
			if contexts[requests[0].name].ReplyContextID() == contexts[requests[1].name].ReplyContextID() {
				t.Fatalf("overlapping same-origin requests share reply context: %#v", contexts)
			}
			waitReplyConformanceBus(t, requestBus)

			// Reconstructing the bus forces reply routing to consume persisted route
			// context and durable reply state rather than process-local request state.
			restartedBus := newDurableReplyConformanceBus(t, ctx, backend, source)
			for _, req := range []requestCase{requests[2], requests[1], requests[0]} {
				replyID := uuid.NewString()
				reply := replyConformanceEventForRun(
					source.ResolveFlowEventReference(templatereply.ProviderFlowID, templatereply.ReplyEvent),
					replyID,
					runID,
					templatereply.ProviderFlowID,
					templatereply.ProviderFlowID,
					map[string]any{
						"provider_request_id": req.requestKey,
						"account_id":          req.accountID,
						"result":              req.name,
					},
				)
				publishCtx := events.WithDeliveryContext(ctx, contexts[req.name])
				if err := restartedBus.Publish(publishCtx, reply); err != nil {
					t.Fatalf("publish reply %s after restart: %v", req.name, err)
				}
				routes, err := backend.ListEventDeliveryRoutes(ctx, replyID)
				if err != nil || len(routes) != 1 {
					t.Fatalf("persisted reply routes %s = %#v err=%v", req.name, routes, err)
				}
				wantInstance := templatereply.RequesterFlowID + "/" + req.accountID
				if routes[0].Target.FlowInstance != wantInstance || !routes[0].Context.Empty() {
					t.Fatalf("persisted reply route %s = %#v, want instance %q with consumed context", req.name, routes[0], wantInstance)
				}
				record, err := backend.LoadReplyContext(ctx, contexts[req.name].ReplyContextID())
				if err != nil || record.State != runtimereplycontext.StateTerminal || record.AcceptedReplyEventID != replyID {
					t.Fatalf("terminal reply context %s = %#v err=%v", req.name, record, err)
				}
			}
		})
	}
}

func TestReplyResolutionConformance_DurableExplicitCorrelationFailsClosedOnBothBackends(t *testing.T) {
	for _, backendCase := range []struct {
		name  string
		setup func(*testing.T) durableReplyConformanceStore
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) durableReplyConformanceStore {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) durableReplyConformanceStore {
				return storetest.StartSQLiteRuntimeStore(t)
			},
		},
	} {
		t.Run(backendCase.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			source := templatereply.LoadSource(t, templatereply.Options{OptionalReplyCorrelation: true})
			backend := backendCase.setup(t)
			runID := uuid.NewString()
			seedDurableReplyConformanceRun(t, ctx, backend, runID)
			ctx = runtimecorrelation.WithRunID(ctx, runID)
			eb := newDurableReplyConformanceBus(t, ctx, backend, source)

			for _, invalid := range []struct {
				name    string
				value   string
				include bool
			}{
				{name: "absent_value"},
				{name: "inconsistent_value", value: "different-request", include: true},
			} {
				t.Run(invalid.name, func(t *testing.T) {
					requestID := uuid.NewString()
					requestKey := "request-" + uuid.NewString()
					request := replyConformanceEventForRun(
						source.ResolveFlowEventReference(templatereply.RequesterFlowID, templatereply.RequestEvent),
						requestID,
						runID,
						templatereply.RequesterFlowID,
						templatereply.RequesterFlowID+"/account-a",
						map[string]any{"provider_request_id": requestKey, "account_id": "account-a"},
					)
					if err := eb.Publish(ctx, request); err != nil {
						t.Fatalf("publish request: %v", err)
					}
					requestRoutes, err := backend.ListEventDeliveryRoutes(ctx, requestID)
					if err != nil || len(requestRoutes) != 1 || requestRoutes[0].Context.Empty() {
						t.Fatalf("request routes = %#v err=%v, want one carried reply context", requestRoutes, err)
					}
					deliveryContext := requestRoutes[0].Context
					replyID := uuid.NewString()
					replyPayload := map[string]any{"account_id": "account-a", "result": "invalid"}
					if invalid.include {
						replyPayload["provider_request_id"] = invalid.value
					}
					reply := replyConformanceEventForRun(
						source.ResolveFlowEventReference(templatereply.ProviderFlowID, templatereply.ReplyEvent),
						replyID,
						runID,
						templatereply.ProviderFlowID,
						templatereply.ProviderFlowID,
						replyPayload,
					)
					publishCtx := events.WithDeliveryContext(ctx, deliveryContext)
					preflight, err := eb.CheckPublishRecipientPlan(publishCtx, reply)
					if err != nil {
						t.Fatalf("reply preflight: %v", err)
					}
					if preflight.TargetFailure != string(runtimepinrouting.FailureStaleArrival) || len(preflight.DeliveryRoutes) != 0 {
						t.Fatalf("reply preflight = %#v, want stale arrival with zero routes", preflight)
					}
					assertReplyContextState(t, ctx, backend, deliveryContext.ReplyContextID(), runtimereplycontext.StateOpen, "")

					if err := eb.Publish(publishCtx, reply); err != nil {
						t.Fatalf("publish invalid reply: %v", err)
					}
					if routes, err := backend.ListEventDeliveryRoutes(ctx, replyID); err != nil || len(routes) != 0 {
						t.Fatalf("invalid reply routes = %#v err=%v, want none", routes, err)
					}
					if got := loadReplyConformanceTargetFailure(t, ctx, backend, replyID); got != string(runtimepinrouting.FailureStaleArrival) {
						t.Fatalf("invalid reply target failure = %q, want %q", got, runtimepinrouting.FailureStaleArrival)
					}
					assertReplyContextState(t, ctx, backend, deliveryContext.ReplyContextID(), runtimereplycontext.StateOpen, "")
				})
			}
		})
	}
}

func TestReplyResolutionConformance_TypedHumanTaskPreservesReplyAuthorityAcrossRestart(t *testing.T) {
	for _, backendCase := range []struct {
		name  string
		setup func(*testing.T) durableReplyConformanceStore
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) durableReplyConformanceStore {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) durableReplyConformanceStore {
				return storetest.StartSQLiteRuntimeStore(t)
			},
		},
	} {
		t.Run(backendCase.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			requestKey := "human-task-request"
			source := templatereply.LoadSource(t, templatereply.Options{
				ProviderContinuation: templatereply.ContinuationHuman,
			})
			if findings := runtimebootverify.Run(ctx, source, runtimebootverify.Options{}).HardInvalidities(); len(findings) != 0 {
				t.Fatalf("human-task continuation fixture hard invalidities = %#v", findings)
			}
			backend := backendCase.setup(t)
			cards, ok := backend.(replyHumanTaskConformanceStore)
			if !ok {
				t.Fatalf("%T lacks typed human-task conformance surface", backend)
			}
			runID := uuid.NewString()
			seedDurableReplyConformanceRun(t, ctx, backend, runID)
			ctx = runtimecorrelation.WithRunID(ctx, runID)

			requestBus := newDurableReplyConformanceBus(t, ctx, backend, source)
			requestID := uuid.NewString()
			request := replyConformanceEventForRun(
				source.ResolveFlowEventReference(templatereply.RequesterFlowID, templatereply.RequestEvent),
				requestID,
				runID,
				templatereply.RequesterFlowID,
				templatereply.RequesterFlowID+"/account-a",
				map[string]any{"provider_request_id": requestKey, "account_id": "account-a"},
			)
			if err := requestBus.Publish(ctx, request); err != nil {
				t.Fatalf("publish human-task request: %v", err)
			}
			requestRoutes, err := backend.ListEventDeliveryRoutes(ctx, requestID)
			if err != nil || len(requestRoutes) != 1 || requestRoutes[0].Context.Empty() {
				t.Fatalf("human-task request routes = %#v err=%v", requestRoutes, err)
			}
			deliveryContext := requestRoutes[0].Context
			card := createReplyConformanceHumanTask(t, ctx, cards, runID, requestID, deliveryContext, "main")
			waitReplyConformanceBus(t, requestBus)

			// Rebuild the bus and coordinator before any operator outcome. No
			// process-local request state may be needed to resume the requester.
			resumedBus, outcomes := newDurableReplyHumanTaskRuntime(t, ctx, backend, source)
			deferredAt := time.Now().UTC().Truncate(time.Microsecond).Add(789 * time.Nanosecond)
			deferred, err := cards.DeferDecisionCard(ctx, decisioncard.DeferRequest{
				CardID: card.CardID, ActorTokenID: "operator", Until: deferredAt.Add(10 * time.Minute), Now: deferredAt,
			})
			if err != nil {
				t.Fatalf("defer typed human-task card: %v", err)
			}
			deferEvent := replyConformanceCardLifecycleEvent(t, deferred.Card, uuid.NewString(), "mailbox.card_deferred", deferredAt)
			if err := resumedBus.Publish(ctx, deferEvent); err != nil {
				t.Fatalf("publish typed human-task defer: %v", err)
			}
			deferredOutcome := receiveReplyConformanceHumanTaskOutcome(t, outcomes, "human_task.deferred")
			if got := deferredOutcome.DeliveryContext().ReplyContextID(); got != deliveryContext.ReplyContextID() {
				t.Fatalf("deferred human-task reply context = %q, want %q", got, deliveryContext.ReplyContextID())
			}
			assertReplyContextState(t, ctx, backend, deliveryContext.ReplyContextID(), runtimereplycontext.StateOpen, "")

			decisionAt := deferredAt.Add(time.Second)
			decisionEventID := uuid.NewString()
			decided, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator",
				ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: decisionAt,
			})
			if err != nil {
				t.Fatalf("decide typed human-task card: %v", err)
			}
			decisionEvent := replyConformanceCardLifecycleEvent(t, decided.Card, decisionEventID, "mailbox.card_decided", decisionAt)
			if err := resumedBus.Publish(ctx, decisionEvent); err != nil {
				t.Fatalf("publish typed human-task decision: %v", err)
			}
			approvedOutcome := receiveReplyConformanceHumanTaskOutcome(t, outcomes, "human_task.approved")
			if got := approvedOutcome.DeliveryContext().ReplyContextID(); got != deliveryContext.ReplyContextID() {
				t.Fatalf("approved human-task reply context = %q, want %q", got, deliveryContext.ReplyContextID())
			}
			continuation, err := cards.LoadHumanTaskContinuation(ctx, card.CardID)
			if err != nil || continuation.State != decisioncard.HumanTaskContinuationOutcomeDispatched {
				t.Fatalf("dispatched human-task continuation = %#v err=%v", continuation, err)
			}

			replyID := uuid.NewString()
			reply := replyConformanceEventForRun(
				source.ResolveFlowEventReference(templatereply.ProviderFlowID, templatereply.ReplyEvent),
				replyID,
				runID,
				templatereply.ProviderFlowID,
				templatereply.ProviderFlowID,
				map[string]any{"provider_request_id": requestKey, "account_id": "account-a", "result": "approved"},
			)
			if err := resumedBus.Publish(events.WithDeliveryContext(ctx, approvedOutcome.DeliveryContext()), reply); err != nil {
				t.Fatalf("publish terminal reply after typed human task: %v", err)
			}
			routes, err := backend.ListEventDeliveryRoutes(ctx, replyID)
			if err != nil || len(routes) != 1 || routes[0].Target.FlowInstance != templatereply.RequesterFlowID+"/account-a" {
				t.Fatalf("terminal typed-human-task reply routes = %#v err=%v", routes, err)
			}
			assertReplyContextState(t, ctx, backend, deliveryContext.ReplyContextID(), runtimereplycontext.StateTerminal, replyID)

			proveTypedHumanTaskStaleOrigin(t, ctx, backend, cards, source, runID)
		})
	}
}

func proveTypedHumanTaskStaleOrigin(t *testing.T, ctx context.Context, backend durableReplyConformanceStore, cards replyHumanTaskConformanceStore, source semanticview.Source, runID string) {
	t.Helper()
	requestKey := "human-task-stale-origin"
	requestBus := newDurableReplyConformanceBus(t, ctx, backend, source)
	requestID := uuid.NewString()
	request := replyConformanceEventForRun(
		source.ResolveFlowEventReference(templatereply.RequesterFlowID, templatereply.RequestEvent),
		requestID,
		runID,
		templatereply.RequesterFlowID,
		templatereply.RequesterFlowID+"/account-a",
		map[string]any{"provider_request_id": requestKey, "account_id": "account-a"},
	)
	if err := requestBus.Publish(ctx, request); err != nil {
		t.Fatalf("publish stale-origin human-task request: %v", err)
	}
	routes, err := backend.ListEventDeliveryRoutes(ctx, requestID)
	if err != nil || len(routes) != 1 || routes[0].Context.Empty() {
		t.Fatalf("stale-origin human-task request routes = %#v err=%v", routes, err)
	}
	deliveryContext := routes[0].Context
	card := createReplyConformanceHumanTask(t, ctx, cards, runID, requestID, deliveryContext, "stale")
	waitReplyConformanceBus(t, requestBus)

	resumedBus, outcomes := newDurableReplyHumanTaskRuntime(t, ctx, backend, source)
	decisionAt := time.Now().UTC()
	decisionEventID := uuid.NewString()
	decided, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
		CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator",
		ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: decisionAt,
	})
	if err != nil {
		t.Fatalf("decide stale-origin human-task card: %v", err)
	}
	if err := resumedBus.Publish(ctx, replyConformanceCardLifecycleEvent(t, decided.Card, decisionEventID, "mailbox.card_decided", decisionAt)); err != nil {
		t.Fatalf("publish stale-origin human-task decision: %v", err)
	}
	approvedOutcome := receiveReplyConformanceHumanTaskOutcome(t, outcomes, "human_task.approved")
	if got := approvedOutcome.DeliveryContext().ReplyContextID(); got != deliveryContext.ReplyContextID() {
		t.Fatalf("stale-origin approved reply context = %q, want %q", got, deliveryContext.ReplyContextID())
	}
	if err := resumedBus.RouteTable().RemoveFlowInstanceRoute(runtimeflowidentity.StoredRoute(
		templatereply.RequesterFlowID, "account-a", templatereply.RequesterFlowID+"/account-a",
	)); err != nil {
		t.Fatalf("remove stale requester route: %v", err)
	}

	replyID := uuid.NewString()
	reply := replyConformanceEventForRun(
		source.ResolveFlowEventReference(templatereply.ProviderFlowID, templatereply.ReplyEvent),
		replyID,
		runID,
		templatereply.ProviderFlowID,
		templatereply.ProviderFlowID,
		map[string]any{"provider_request_id": requestKey, "account_id": "account-a", "result": "approved"},
	)
	if err := resumedBus.Publish(events.WithDeliveryContext(ctx, approvedOutcome.DeliveryContext()), reply); err != nil {
		t.Fatalf("publish stale-origin terminal reply: %v", err)
	}
	if routes, err := backend.ListEventDeliveryRoutes(ctx, replyID); err != nil || len(routes) != 0 {
		t.Fatalf("stale-origin terminal reply routes = %#v err=%v, want none", routes, err)
	}
	if got := loadReplyConformanceTargetFailure(t, ctx, backend, replyID); got != string(runtimepinrouting.FailureStaleArrival) {
		t.Fatalf("stale-origin target failure = %q, want %q", got, runtimepinrouting.FailureStaleArrival)
	}
	assertReplyContextState(t, ctx, backend, deliveryContext.ReplyContextID(), runtimereplycontext.StateOpen, "")
}

type replyHumanTaskConformanceStore interface {
	durableReplyConformanceStore
	decisioncard.Store
	decisioncard.HumanTaskStore
}

func createReplyConformanceHumanTask(t *testing.T, ctx context.Context, cards replyHumanTaskConformanceStore, runID, sourceEventID string, deliveryContext events.DeliveryContext, suffix string) decisioncard.Card {
	t.Helper()
	now := time.Now().UTC()
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "provider-agent",
		OperationID:      "provider-turn/" + suffix,
		Category:         "approval",
		Scope:            decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: templatereply.ProviderFlowID},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("human_task", "Approve provider reply", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", Label: "Approve"},
		"reject":  {Verdict: "reject", Label: "Reject"},
	})
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, Anchor: anchor, Snapshot: snapshot,
		ExecutionMode: "live",
		BundleHash:    "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	continuation := decisioncard.HumanTaskContinuation{
		CardID: card.CardID, RunID: runID, ReplyContextID: deliveryContext.ReplyContextID(), SourceEventID: sourceEventID,
		DeadlineAt: now.Add(time.Hour), BudgetBundleHash: card.BundleHash,
		BudgetWindowStart: now.Truncate(7 * 24 * time.Hour), BudgetWindowEnd: now.Truncate(7 * 24 * time.Hour).Add(7 * 24 * time.Hour),
		State: decisioncard.HumanTaskContinuationPending, CreatedAt: now, UpdatedAt: now,
	}
	if err := cards.CreateHumanTaskCard(ctx, card, continuation); err != nil {
		t.Fatalf("create typed human-task card: %v", err)
	}
	return card
}

func newDurableReplyHumanTaskRuntime(t *testing.T, ctx context.Context, backend durableReplyConformanceStore, source semanticview.Source) (*bus.EventBus, <-chan *bus.LocalDelivery) {
	t.Helper()
	cards, ok := backend.(replyHumanTaskConformanceStore)
	if !ok {
		t.Fatalf("%T lacks typed human-task runtime surface", backend)
	}
	eb := newDurableReplyConformanceBus(t, ctx, backend, source)
	db := replyConformanceDB(t, backend)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if sqliteStore, ok := backend.(*store.SQLiteRuntimeStore); ok {
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, sqliteStore)
	}
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	module := conformanceLoadedWorkflowModule{
		source: source, workflow: workflow, nodes: nodes,
		guards: runtimepipeline.NewContractGuardRegistry(source), actions: runtimepipeline.NewContractActionRegistry(source),
	}
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(eb, db, runtimepipeline.PipelineCoordinatorOptions{
		Module: module, WorkflowStore: workflowStore, DecisionCards: cards,
	})
	eb.SetInterceptors(coordinator)
	eb.RegisterRuntimeActiveAgentDescriptor(bus.ActiveAgentDescriptor{AgentID: "provider-agent"})
	outcomes := eb.Subscribe("provider-agent", events.EventType("human_task.deferred"), events.EventType("human_task.approved"))
	return eb, outcomes
}

func replyConformanceCardLifecycleEvent(t *testing.T, card decisioncard.Card, eventID, eventType string, at time.Time) events.Event {
	t.Helper()
	payload, err := canonicaljson.Bytes(map[string]any{
		"card_id": card.CardID, "anchor_kind": card.Anchor.Kind(), "anchor": card.Anchor.SemanticValue().Interface(),
		"decision_id": card.Snapshot.Decision, "verdict": card.Verdict, "card_content_hash": card.CardContentHash,
		"decision_schema_hash": card.DecisionSchemaHash, "bundle_hash": card.BundleHash, "fields": card.Fields.Interface(),
	})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := card.Anchor.Scope()
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.RuntimeControl(
		eventID, events.EventType(eventType), "platform", "", payload, 0, card.RunID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, scope.EntityID), scope.FlowInstance), at.UTC(),
	)
}

func receiveReplyConformanceHumanTaskOutcome(t *testing.T, outcomes <-chan *bus.LocalDelivery, eventType string) events.Event {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case delivery := <-outcomes:
			evt := delivery.Event()
			_ = delivery.Complete()
			if string(evt.Type()) == eventType {
				return evt
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", eventType)
		}
	}
}

type durableReplyConformanceStore interface {
	bus.EventStore
	bus.FlowInstanceRoutePersistence
	runtimereplycontext.Store
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
}

func newDurableReplyConformanceBus(t *testing.T, ctx context.Context, backend durableReplyConformanceStore, source semanticview.Source) *bus.EventBus {
	t.Helper()
	if source == nil {
		t.Fatal("reply conformance semantic source is required")
	}
	eb, err := newScopedTestEventBus(t, backend, bus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	persisted, err := backend.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	persistedByPath := make(map[string]struct{}, len(persisted))
	for _, route := range persisted {
		persistedByPath[route.InstancePath] = struct{}{}
	}
	for _, accountID := range []string{"account-a", "account-b"} {
		req := bus.FlowInstanceRouteMaterializationRequest{
			Identity:            runtimeflowidentity.StoredRoute(templatereply.RequesterFlowID, accountID, templatereply.RequesterFlowID+"/"+accountID),
			ActivationVariables: map[string]string{"account_id": accountID},
		}
		var err error
		if _, exists := persistedByPath[req.Identity.InstancePath]; exists {
			err = eb.RestorePersistedFlowInstanceRoute(req)
		} else {
			err = eb.AddFlowInstanceRouteContext(ctx, req)
		}
		if err != nil {
			t.Fatalf("materialize requester route %s: %v", accountID, err)
		}
	}
	return eb
}

func replyConformanceDB(t *testing.T, backend durableReplyConformanceStore) *sql.DB {
	t.Helper()
	switch typed := backend.(type) {
	case *store.PostgresStore:
		return typed.DB
	case *store.SQLiteRuntimeStore:
		return typed.DB
	default:
		t.Fatalf("unsupported reply conformance backend %T", backend)
		return nil
	}
}

func loadReplyConformanceTargetFailure(t *testing.T, ctx context.Context, backend durableReplyConformanceStore, eventID string) string {
	t.Helper()
	db := replyConformanceDB(t, backend)
	query := `SELECT failure::text FROM dead_letters WHERE original_event_id = $1::uuid ORDER BY created_at DESC LIMIT 1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT failure FROM dead_letters WHERE original_event_id = ? ORDER BY created_at DESC LIMIT 1`
	}
	var raw string
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&raw); err != nil {
		t.Fatalf("load reply conformance target failure: %v", err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope([]byte(raw))
	if err != nil {
		t.Fatalf("decode reply conformance target failure: %v", err)
	}
	return string(failure.Class)
}

func countReplyConformanceEvents(t *testing.T, ctx context.Context, backend durableReplyConformanceStore, eventType string) int {
	t.Helper()
	db := replyConformanceDB(t, backend)
	query := `SELECT COUNT(*) FROM events WHERE event_name = $1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(*) FROM events WHERE event_name = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, eventType).Scan(&count); err != nil {
		t.Fatalf("count reply conformance events: %v", err)
	}
	return count
}

func loadLatestReplyConformanceEventID(t *testing.T, ctx context.Context, backend durableReplyConformanceStore, eventType string) string {
	t.Helper()
	db := replyConformanceDB(t, backend)
	query := `SELECT event_id::text FROM events WHERE event_name = $1 ORDER BY created_at DESC, event_id DESC LIMIT 1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT event_id FROM events WHERE event_name = ? ORDER BY created_at DESC, event_id DESC LIMIT 1`
	}
	var id string
	if err := db.QueryRowContext(ctx, query, eventType).Scan(&id); err != nil {
		t.Fatalf("load terminal reply event: %v\npersisted runtime rows:\n%s", err, replyConformanceDebugRows(ctx, db, backend))
	}
	return id
}

func replyConformanceDebugRows(ctx context.Context, db *sql.DB, backend durableReplyConformanceStore) string {
	eventID := "event_id::text"
	sourceID := "COALESCE(source_event_id::text, '')"
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		eventID = "event_id"
		sourceID = "COALESCE(source_event_id, '')"
	}
	rows, err := db.QueryContext(ctx, `SELECT `+eventID+`, event_name, `+sourceID+` FROM events ORDER BY created_at, event_id`)
	if err != nil {
		return "events query: " + err.Error()
	}
	defer rows.Close()
	var out strings.Builder
	for rows.Next() {
		var id, name, source string
		if err := rows.Scan(&id, &name, &source); err != nil {
			return "events scan: " + err.Error()
		}
		fmt.Fprintf(&out, "event %s %s source=%s\n", id, name, source)
	}
	deliveryEventID := "event_id::text"
	deliveryFailure := "COALESCE(failure, 'null'::jsonb)::text"
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		deliveryEventID = "event_id"
		deliveryFailure = "COALESCE(failure, 'null')"
	}
	deliveries, err := db.QueryContext(ctx, `SELECT `+deliveryEventID+`, subscriber_type, subscriber_id, status, COALESCE(reason_code, ''), `+deliveryFailure+` FROM event_deliveries ORDER BY created_at, delivery_id`)
	if err != nil {
		fmt.Fprintf(&out, "deliveries query: %v\n", err)
		return out.String()
	}
	defer deliveries.Close()
	for deliveries.Next() {
		var id, subscriberType, subscriberID, status, reason, failureRaw string
		if err := deliveries.Scan(&id, &subscriberType, &subscriberID, &status, &reason, &failureRaw); err != nil {
			fmt.Fprintf(&out, "deliveries scan: %v\n", err)
			break
		}
		failureSummary := ""
		if strings.TrimSpace(failureRaw) != "" && strings.TrimSpace(failureRaw) != "null" {
			if failure, err := runtimefailures.UnmarshalEnvelope([]byte(failureRaw)); err == nil {
				failureSummary = string(failure.Class) + "/" + failure.Detail.Code
			} else {
				failureSummary = "invalid canonical failure: " + err.Error()
			}
		}
		fmt.Fprintf(&out, "delivery %s %s/%s status=%s reason=%s failure=%s\n", id, subscriberType, subscriberID, status, reason, failureSummary)
	}
	receiptEventID := "event_id::text"
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		receiptEventID = "event_id"
	}
	receipts, err := db.QueryContext(ctx, `SELECT `+receiptEventID+`, subscriber_type, subscriber_id, outcome, COALESCE(reason_code, ''), COALESCE(side_effects, '{}') FROM event_receipts ORDER BY event_id, subscriber_type, subscriber_id`)
	if err != nil {
		fmt.Fprintf(&out, "receipts query: %v\n", err)
		return out.String()
	}
	defer receipts.Close()
	for receipts.Next() {
		var id, subscriberType, subscriberID, outcome, reason string
		var sideEffects any
		if err := receipts.Scan(&id, &subscriberType, &subscriberID, &outcome, &reason, &sideEffects); err != nil {
			fmt.Fprintf(&out, "receipts scan: %v\n", err)
			break
		}
		if raw, ok := sideEffects.([]byte); ok {
			sideEffects = string(raw)
		}
		fmt.Fprintf(&out, "receipt %s %s/%s outcome=%s reason=%s effects=%v\n", id, subscriberType, subscriberID, outcome, reason, sideEffects)
	}
	return out.String()
}

func assertReplyContextState(t *testing.T, ctx context.Context, backend durableReplyConformanceStore, contextID string, state runtimereplycontext.State, acceptedEventID string) {
	t.Helper()
	record, err := backend.LoadReplyContext(ctx, contextID)
	if err != nil || record.State != state || record.AcceptedReplyEventID != acceptedEventID {
		t.Fatalf("reply context state = %#v err=%v, want state=%q accepted=%q", record, err, state, acceptedEventID)
	}
}

func waitReplyConformanceBus(t *testing.T, eb *bus.EventBus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 5*time.Second)
	defer cancel()
	if err := eb.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("wait for reply conformance bus: %v", err)
	}
}

func seedDurableReplyConformanceRun(t *testing.T, ctx context.Context, backend durableReplyConformanceStore, runID string) {
	t.Helper()
	switch typed := backend.(type) {
	case *store.PostgresStore:
		if _, err := typed.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
			t.Fatalf("seed postgres reply conformance run: %v", err)
		}
	case *store.SQLiteRuntimeStore:
		if _, err := typed.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
			t.Fatalf("seed sqlite reply conformance run: %v", err)
		}
	default:
		t.Fatalf("unsupported reply conformance backend %T", backend)
	}
}

func replyConformanceEvent(eventType, id, flowID, flowInstance string, payload map[string]any) events.Event {
	return replyConformanceEventForRun(eventType, id, uuid.NewString(), flowID, flowInstance, payload)
}

func replyConformanceEventForRun(eventType, id, runID, flowID, flowInstance string, payload map[string]any) events.Event {
	flowID = strings.Trim(strings.TrimSpace(flowID), "/")
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance != "" && flowInstance != flowID && strings.HasPrefix(eventType, flowID+"/") {
		eventType = flowInstance + "/" + strings.TrimPrefix(eventType, flowID+"/")
	}
	raw, _ := json.Marshal(payload)
	return eventtest.RunCreatingRootIngress(
		id,
		events.EventType(eventType),
		flowID,
		"",
		raw,
		0,
		runID,
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID:       flowID,
			FlowInstance: flowInstance,
			EntityID:     runtimeflowidentity.EntityID(flowInstance),
		}),
		time.Now().UTC(),
	)
}

type replyConformanceStore struct {
	bus.InMemoryEventStore
	mu          sync.Mutex
	events      map[string]events.Event
	routes      map[string][]events.DeliveryRoute
	scopes      map[string]runtimereplayclaim.CommittedReplayScope
	contexts    map[string]runtimereplycontext.Record
	createCalls int
	claimCalls  int
}

func newReplyConformanceStore() *replyConformanceStore {
	return &replyConformanceStore{
		events:   map[string]events.Event{},
		routes:   map[string][]events.DeliveryRoute{},
		scopes:   map[string]runtimereplayclaim.CommittedReplayScope{},
		contexts: map[string]runtimereplycontext.Record{},
	}
}

func (s *replyConformanceStore) SupportsPersistedReplay() bool { return true }

func (s *replyConformanceStore) CommitPublish(ctx context.Context, plan bus.CommitPublishPlan) (bus.PreparedPublish, error) {
	return runtimebustest.CommitPublish(ctx, plan, nil, func(_ context.Context, req bus.CommitPublishRequest) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.events[req.Event.ID()] = req.Event.Event()
		s.routes[req.Event.ID()] = events.NormalizeDeliveryRoutes(req.DeliveryRoutes)
		s.scopes[req.Event.ID()] = req.ReplayScope
		return nil
	})
}

func (s *replyConformanceStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	return s.routesFor(eventID), nil
}

func (s *replyConformanceStore) UpsertCommittedReplayScope(_ context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scopes[eventID] = scope
	return nil
}

func (s *replyConformanceStore) LoadCommittedReplayScope(_ context.Context, eventID string) (runtimereplayclaim.CommittedReplayScope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	scope := s.scopes[eventID]
	if scope == "" {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	return scope, nil
}

func (s *replyConformanceStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	routes := s.routesFor(eventID)
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		out = append(out, route.SubscriberID)
	}
	return out, nil
}

func (s *replyConformanceStore) CreateReplyContext(_ context.Context, record runtimereplycontext.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createCalls++
	record = record.Normalized()
	if existing, ok := s.contexts[record.ID]; ok {
		if existing.RequestEventID != record.RequestEventID || existing.Origin != record.Origin {
			return errors.New("reply context identity collision")
		}
		return nil
	}
	s.contexts[record.ID] = record
	return nil
}

func (s *replyConformanceStore) LoadReplyContext(_ context.Context, id string) (runtimereplycontext.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.contexts[id]
	if !ok {
		return runtimereplycontext.Record{}, runtimereplycontext.ErrNotFound
	}
	return record, nil
}

func (s *replyConformanceStore) ClaimReplyContext(_ context.Context, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	record, ok := s.contexts[id]
	if !ok {
		return runtimereplycontext.Record{}, "", runtimereplycontext.ErrNotFound
	}
	if record.State == runtimereplycontext.StateTerminal {
		if record.AcceptedReplyEventID == replyEventID {
			return record, runtimereplycontext.ClaimIdempotent, nil
		}
		return record, runtimereplycontext.ClaimTerminal, nil
	}
	now := time.Now().UTC()
	record.State = runtimereplycontext.StateTerminal
	record.AcceptedReplyEventID = replyEventID
	record.TerminalAt = &now
	record.UpdatedAt = now
	s.contexts[id] = record
	return record, runtimereplycontext.ClaimAccepted, nil
}

func (s *replyConformanceStore) routesFor(eventID string) []events.DeliveryRoute {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...)
}
