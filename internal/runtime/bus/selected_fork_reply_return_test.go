package bus

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestIssue642RetainedReturnRequiresExactTerminalScope(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyMixedReplyResponse(t))
	var request, response, ordinary pinrouting.ConnectRoutePlan
	for _, plan := range pinrouting.CompileConnectGraph(source).Plans() {
		if plan.ReplyRole() == pinrouting.ConnectReplyRoleRequest {
			request = plan
		} else if plan.ReplyRole() == pinrouting.ConnectReplyRoleResponse {
			response = plan
		}
		if plan.ReceiverLocalEvent() == "provider.observed" {
			ordinary = plan
		}
	}
	ctx, event, _ := selectedRecipientContextFixture(t)
	record, err := request.ReplyRequestRecord(event, events.RouteIdentity{FlowID: ".", FlowInstance: event.RunID(), EntityID: event.RunID()},
		pinrouting.AdmitConnectRouteMatchValues(map[string]string{"payload.request_id": "key"}), event.CreatedAt())
	if err != nil {
		t.Fatal(err)
	}
	at := record.CreatedAt.Add(time.Millisecond)
	record.State, record.AcceptedReplyEventID = replycontext.StateTerminal, uuid.NewString()
	record.UpdatedAt, record.TerminalAt = at, &at
	if record.AcceptedReplyEventID == event.ID() {
		t.Fatal("test must retain the distinct logical and physical coordinates")
	}
	before := record.Normalized()
	bound, err := WithSelectedForkReplyReturn(ctx, event, record)
	if err != nil {
		t.Fatal(err)
	}
	if events.DeliveryContextFromContext(bound).ReplyContextID() != record.ID {
		t.Fatal("return lookup did not name its own context")
	}
	if found, err := selectedForkReplyReturn(bound, event, record); err != nil || !found {
		t.Fatalf("exact retained return: found=%t error=%v", found, err)
	}
	if err := (pinrouting.ReplyReturnContinuation{Record: record, Plan: response}).Validate(); err != nil {
		t.Fatal(err)
	}
	node := identitytest.FlowNode(t, ".", "requester")
	compiled := pinrouting.ConnectDeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: record.Origin, Handler: pinrouting.MustConnectReceiverHandler(node)}
	compiled.ConnectClaim, err = pinrouting.ConnectExecutionClaim(response, compiled)
	if err != nil {
		t.Fatal(err)
	}
	intents, err := replyResponseDeliveryIntents(record.RunID, response, []pinrouting.ConnectDeliveryRoute{compiled}, response.ReceiverLocalEvent())
	if err != nil || len(intents) != 1 || intents[0].ConnectPlan.Empty() || intents[0].ConnectClaim.Empty() {
		t.Fatalf("reply response omitted complete plan/claim evidence: %+v error=%v", intents, err)
	}
	observer := identitytest.FlowNode(t, ".", "response-observer")
	ordinaryRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(observer), Target: events.MustExistingEntityTarget(record.Origin)}
	ordinaryBlueprint := record.Origin
	ordinaryBlueprint.EntityID = ""
	ordinaryRoute.ConnectClaim, err = pinrouting.ConnectExecutionClaim(ordinary, pinrouting.ConnectDeliveryRoute{Recipient: ordinaryRoute.Recipient,
		Target: ordinaryBlueprint, Handler: pinrouting.MustConnectReceiverHandler(observer)})
	if err != nil {
		t.Fatal(err)
	}
	responseRoute := events.DeliveryRoute{Recipient: compiled.Recipient, Target: events.MustExistingEntityTarget(record.Origin), ConnectClaim: compiled.ConnectClaim}
	retainedRoutes := []events.DeliveryRoute{ordinaryRoute, responseRoute}
	contexts, err := selectedForkReturnContexts(selectedForkRecipientPlan{returnReply: &record, returnRoutes: retainedRoutes},
		[]pinrouting.ReplyReturnContinuation{{Record: record, Plan: response, OrdinaryPlans: []pinrouting.ConnectRoutePlan{ordinary}}})
	if err != nil || len(contexts) != 2 {
		t.Fatalf("same-origin compiled response subset restoration: contexts=%+v error=%v", contexts, err)
	}
	for _, route := range retainedRoutes {
		key, err := selectedForkContextRouteIdentity(route)
		actual, found := contexts[key]
		if err != nil || !found || !reflect.DeepEqual(actual, route.Context.Normalized()) {
			t.Fatalf("restoration changed or omitted an ordinary/response claim: %+v found=%t error=%v", route, found, err)
		}
	}
	if !reflect.DeepEqual(retainedRoutes, []events.DeliveryRoute{ordinaryRoute, responseRoute}) {
		t.Fatal("context restoration rewrote retained input")
	}
	resolver := connectRoutePlanResolver{replyStore: retainedReturnReplyStore{record: record}}
	matched, loaded, err := resolver.resolveReplyPlans(bound, []pinrouting.ConnectRoutePlan{ordinary, response})
	if err != nil || len(matched) != 2 || !reflect.DeepEqual(loaded, record) {
		t.Fatalf("paired reply selection discarded an ordinary connection: plans=%d error=%v", len(matched), err)
	}
	for _, fault := range []string{"missing_reference", "missing_row", "unpaired_row"} {
		t.Run("plan_selection_"+fault, func(t *testing.T) {
			callCtx, callResolver := bound, resolver
			switch fault {
			case "missing_reference":
				callCtx = context.Background()
			case "missing_row":
				callResolver.replyStore = retainedReturnReplyStore{}
			case "unpaired_row":
				changed := record.Normalized()
				changed.ProviderOutputPin = "other"
				callResolver.replyStore = retainedReturnReplyStore{record: changed}
			}
			plans, _, err := callResolver.resolveReplyPlans(callCtx, []pinrouting.ConnectRoutePlan{ordinary, response})
			if err != nil || len(plans) != 1 || plans[0].ReplyRole() != pinrouting.ConnectReplyRoleResponse {
				t.Fatalf("unadmitted response escaped its ordinary refusal branch: plans=%d error=%v", len(plans), err)
			}
		})
	}
	for _, fault := range []string{"open", "run", "identity", "accepted", "origin", "clock", "original", "enclosing", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			actual := record.Normalized()
			callCtx := bound
			switch fault {
			case "open":
				actual.State, actual.AcceptedReplyEventID, actual.TerminalAt = replycontext.StateOpen, "", nil
			case "run":
				actual.RunID = uuid.NewString()
			case "identity":
				actual.ID = uuid.NewString()
			case "accepted":
				actual.AcceptedReplyEventID = uuid.NewString()
			case "origin":
				actual.Origin.EntityID = uuid.NewString()
			case "clock":
				actual.UpdatedAt = actual.UpdatedAt.Add(time.Millisecond)
			case "original":
				actual.SourceRequestEventID = uuid.NewString()
			case "enclosing":
				actual.EnclosingReplyContextID = uuid.NewString()
			case "canceled":
				canceled, cancel := context.WithCancel(bound)
				cancel()
				callCtx = canceled
			}
			if _, err := selectedForkReplyReturn(callCtx, event, actual); err == nil {
				t.Fatal("changed terminal obligation or canceled scope admitted")
			}
		})
	}
	if found, err := selectedForkReplyReturn(context.Background(), event, record); err != nil || found {
		t.Fatal("ordinary publication borrowed retained-return admission")
	}
	if _, err := WithSelectedForkReplyReturn(bound, event, record); err == nil {
		t.Fatal("duplicate binding admitted")
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("return binding rewrote retained input")
	}
}

type retainedReturnReplyStore struct {
	replycontext.Store
	record replycontext.Record
}

func (s retainedReturnReplyStore) LoadReplyContext(_ context.Context, id string) (replycontext.Record, error) {
	if id != s.record.ID {
		return replycontext.Record{}, replycontext.ErrNotFound
	}
	return s.record.Normalized(), nil
}
