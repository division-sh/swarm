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
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyRootReplyBoundary(t, true, true))
	var request, response pinrouting.ConnectRoutePlan
	for _, plan := range pinrouting.CompileConnectGraph(source).Plans() {
		if plan.ReplyRole() == pinrouting.ConnectReplyRoleRequest {
			request = plan
		} else if plan.ReplyRole() == pinrouting.ConnectReplyRoleResponse {
			response = plan
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
