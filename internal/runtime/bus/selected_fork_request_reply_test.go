package bus

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
	resolved, err = stampConnectExecutionClaims(plan, resolved)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := pinrouting.ConnectExecutionClaim(plan, resolved[0])
	if err != nil || !resolved[0].ConnectClaim.Equal(expected) {
		t.Fatalf("child claim does not bind actual context: %v", err)
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
	observed, err := continuation.ClaimForDelivery(sourceRoute)
	if err != nil || !observed.Equal(original) {
		t.Fatalf("acquired entity replaced compiled target evidence: %v", err)
	}
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
