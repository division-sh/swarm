package bus

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestIssue642RequestContinuationBindsBeforeCompiledClaim(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyRootReplyBoundary(t, true, true))
	var plan pinrouting.ConnectRoutePlan
	for _, candidate := range pinrouting.CompileConnectGraph(source).Plans() {
		if candidate.ReplyRole() == pinrouting.ConnectReplyRoleRequest {
			plan = candidate
		}
	}
	ctx, event, routes := selectedRecipientContextFixture(t)
	origin := events.RouteIdentity{FlowID: ".", FlowInstance: event.RunID(), EntityID: event.RunID()}
	draft, err := plan.ReplyRequestRecord(event, origin,
		pinrouting.AdmitConnectRouteMatchValues(map[string]string{"payload.request_id": "key"}), event.CreatedAt())
	if err != nil {
		t.Fatal(err)
	}
	retained := draft.Normalized()
	retained.ID, retained.RequestEventID = uuid.NewString(), uuid.NewString()
	retained.SourceRunID, retained.SourceRequestEventID = uuid.NewString(), uuid.NewString()
	retained.EnclosingReplyContextID = uuid.NewString()
	bound, err := WithSelectedForkRequestReplies(ctx, event, []replycontext.Record{retained})
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := selectedForkRequestReply(bound, event, draft)
	if err != nil || !found || !reflect.DeepEqual(got, retained) {
		t.Fatalf("exact request binding: found=%t row=%+v error=%v", found, got, err)
	}
	node, _ := routes.DeliveryIntents[0].Recipient.Node()
	compiled := []pinrouting.ConnectDeliveryRoute{{Recipient: routes.DeliveryIntents[0].Recipient,
		Handler: pinrouting.MustConnectReceiverHandler(node), Target: origin}}
	resolved := bindReplyRequestRoutes(compiled, got.ID)
	life := events.InstanceLife{RunID: event.RunID(), FlowInstance: origin.FlowInstance, Life: 7}
	resolved, err = stampConnectExecutionClaims(events.WithDeliveryContext(ctx, events.DeliveryContext{Life: &life}), event, plan, resolved)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := pinrouting.ConnectExecutionClaim(plan, resolved[0])
	if err != nil || !resolved[0].ConnectClaim.Equal(expected) {
		t.Fatalf("child claim does not bind actual context: %v", err)
	}
	if !reflect.DeepEqual(resolved[0].Context.Life, &life) {
		t.Fatal("child claim omitted its continuation life")
	}
	withoutLife := resolved[0]
	withoutLife.Context.Life = nil
	unstamped, err := pinrouting.ConnectExecutionClaim(plan, withoutLife)
	if err != nil || unstamped.Equal(expected) {
		t.Fatal("claim did not bind its exact life")
	}
	other := resolved[0]
	other.Context.Reply = &events.ReplyContextRef{ID: draft.ID}
	fresh, err := pinrouting.ConnectExecutionClaim(plan, other)
	if err != nil || fresh.Equal(expected) {
		t.Fatal("fresh context retained the same compiled claim")
	}
	materialized := pinrouting.MaterializeConnectRoutePlan(plan)
	sourceRoute := events.DeliveryRoute{Recipient: compiled[0].Recipient,
		Target:  events.MustExistingEntityTarget(events.RouteIdentity{FlowID: materialized.Target.FlowID, FlowInstance: materialized.Target.FlowInstance, EntityID: uuid.NewString()}),
		Context: resolved[0].Context, ConnectClaim: resolved[0].ConnectClaim}
	raw := resolved[0]
	raw.Target = materialized.Target
	original, err := pinrouting.ConnectExecutionClaim(plan, raw)
	if err != nil {
		t.Fatal(err)
	}
	sourceRoute.ConnectClaim = original
	continuation := pinrouting.ReplyRequestContinuation{Record: retained, Plan: plan}
	raw.Target = sourceRoute.Target.Route()
	acquired, err := pinrouting.ConnectExecutionClaim(plan, raw)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := continuation.ClaimForDelivery(sourceRoute)
	if err != nil || !observed.Equal(acquired) || observed.Equal(original) {
		t.Fatalf("request continuation lost its acquired receiver identity: %v", err)
	}
	t.Run("projected_life_before_hash", func(t *testing.T) {
		selected, err := WithSelectedForkRecipientContexts(bound, event, []events.DeliveryRoute{sourceRoute})
		if err != nil {
			t.Fatal(err)
		}
		candidate := raw
		candidate.Context.Life = nil
		actual, err := stampConnectExecutionClaims(selected, event, plan, []pinrouting.ConnectDeliveryRoute{candidate})
		if err != nil || len(actual) != 1 || !actual[0].ConnectClaim.Equal(acquired) || !reflect.DeepEqual(actual[0].Context, sourceRoute.Context) {
			t.Fatalf("projected life was attached after the full claim: %+v err=%v", actual, err)
		}
		candidate.Target.EntityID = uuid.NewString()
		if _, err := stampConnectExecutionClaims(selected, event, plan, []pinrouting.ConnectDeliveryRoute{candidate}); err == nil {
			t.Fatal("changed target borrowed projected execution life")
		}
		intent := RoutePlanDeliveryIntent{Recipient: actual[0].Recipient, Context: actual[0].Context, ConnectClaim: actual[0].ConnectClaim}
		intent.Context.Life = nil
		projected := (RoutePlan{ReplyContextConsumed: true, DeliveryIntents: []RoutePlanDeliveryIntent{intent}}).WithDefaultDeliveryContext(events.DeliveryContext{Life: &life})
		if projected.DeliveryIntents[0].Context.Life != nil {
			t.Fatal("default context mutated an already-hashed connect route")
		}
	})
	for _, fault := range []string{"terminal", "foreign", "duplicate", "ambiguous", "pair", "ordinary_scope", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			input := []replycontext.Record{retained.Normalized()}
			callCtx := ctx
			switch fault {
			case "terminal":
				input[0].State = replycontext.StateTerminal
			case "foreign":
				input[0].RunID = uuid.NewString()
			case "duplicate":
				input = append(input, input[0])
			case "ambiguous":
				other := input[0]
				other.ID = uuid.NewString()
				input = append(input, other)
			case "pair":
				input[0].ProviderFlowID = "foreign-provider"
			case "ordinary_scope":
				callCtx = context.Background()
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				callCtx = canceled
			}
			candidate, err := WithSelectedForkRequestReplies(callCtx, event, input)
			if err == nil {
				_, _, err = selectedForkRequestReply(candidate, event, draft)
			}
			if err == nil {
				t.Fatal("inexact request scope admitted")
			}
		})
	}
	if _, found, err := selectedForkRequestReply(context.Background(), event, draft); err != nil || found {
		t.Fatalf("fresh publication borrowed retained state: found=%t error=%v", found, err)
	}
}
