package runforkpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestIssue642RetainedReturnPreservesCompleteSourceClaim(t *testing.T) {
	for _, fault := range []string{"exact", "missing", "settled", "claim", "plan", "target", "handler", "projection", "context"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, _, _, _, _ := requestContinuationPartitionFixture(t)
			source := workflowOwnershipSource(t, canonicalrouting.CopyRootReplyBoundary(t, true, true))
			var plan pinrouting.ConnectRoutePlan
			for _, compiled := range pinrouting.CompileConnectGraph(source).Plans() {
				if compiled.ReplyRole() == pinrouting.ConnectReplyRoleResponse {
					plan = compiled
				}
			}
			record := snapshot.ReplyContexts[0].Record.Normalized()
			at := record.CreatedAt.Add(time.Millisecond)
			record.State, record.AcceptedReplyEventID, record.TerminalAt, record.UpdatedAt = replycontext.StateTerminal, uuid.NewString(), &at, at
			node := identitytest.FlowNode(t, ".", "requester")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(record.Origin)}
			var err error
			route.ConnectClaim, err = pinrouting.ConnectExecutionClaim(plan, pinrouting.ConnectDeliveryRoute{Recipient: route.Recipient,
				Target: record.Origin, Handler: pinrouting.MustConnectReceiverHandler(node)})
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Deliveries[0].Snapshot.EventID = record.AcceptedReplyEventID
			snapshot.Deliveries[0].Snapshot.Route = route
			switch fault {
			case "missing":
				snapshot.Deliveries = nil
			case "settled":
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDelivered
			case "claim":
				snapshot.Deliveries[0].Snapshot.Route.ConnectClaim = events.ConnectExecutionClaim{}
			case "plan":
				plan = pinrouting.ConnectRoutePlan{}
			case "target":
				snapshot.Deliveries[0].Snapshot.Route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID, EntityID: uuid.NewString()})
			case "handler":
				snapshot.Deliveries[0].Snapshot.Route.Recipient = events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, ".", "different"))
			case "projection":
				projection, err := events.NewDeliveryPayloadProjection(map[string]string{"value": "other"})
				if err != nil {
					t.Fatal(err)
				}
				snapshot.Deliveries[0].Snapshot.Route.PayloadProjection = projection
			case "context":
				snapshot.Deliveries[0].Snapshot.Route.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: uuid.NewString()}}
			}
			before := append([]runForkRevisionDelivery(nil), snapshot.Deliveries...)
			for i := range before {
				before[i].Snapshot.Route.Context = before[i].Snapshot.Route.Context.Normalized()
			}
			err = requireCapturedReplyReturnClaims(snapshot, record.AcceptedReplyEventID, pinrouting.ReplyReturnContinuation{Record: record, Plan: plan})
			if (err == nil) != (fault == "exact") {
				t.Fatalf("source response claim %s: %v", fault, err)
			}
			if !reflect.DeepEqual(before, snapshot.Deliveries) {
				t.Fatal("source claim validation rewrote captured evidence")
			}
		})
	}
}

func TestIssue642RetainedReturnPreservesOrdinaryRecipients(t *testing.T) {
	for _, sameOrigin := range []bool{true, false} {
		t.Run(map[bool]string{true: "same_origin", false: "different_origin"}[sameOrigin], func(t *testing.T) {
			snapshot, _, _, _, _ := requestContinuationPartitionFixture(t)
			source := workflowOwnershipSource(t, canonicalrouting.CopyMixedReplyResponse(t))
			var response, ordinary pinrouting.ConnectRoutePlan
			for _, plan := range pinrouting.CompileConnectGraph(source).Plans() {
				if plan.ReplyRole() == pinrouting.ConnectReplyRoleResponse {
					response = plan
				}
				if plan.ReceiverLocalEvent() == "provider.observed" {
					ordinary = plan
				}
			}
			record := snapshot.ReplyContexts[0].Record.Normalized()
			at := record.CreatedAt.Add(time.Millisecond)
			record.State, record.AcceptedReplyEventID, record.TerminalAt, record.UpdatedAt = replycontext.StateTerminal, uuid.NewString(), &at, at
			makeRoute := func(plan pinrouting.ConnectRoutePlan, name string, target events.RouteIdentity) events.DeliveryRoute {
				node := identitytest.FlowNode(t, ".", name)
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}
				if plan.ReplyRole() == 0 && plan.ReceiverEndpoint().IsRoot() {
					target.EntityID = ""
				}
				var err error
				route.ConnectClaim, err = pinrouting.ConnectExecutionClaim(plan, pinrouting.ConnectDeliveryRoute{Recipient: route.Recipient,
					Target: target, Handler: pinrouting.MustConnectReceiverHandler(node)})
				if err != nil {
					t.Fatal(err)
				}
				return route
			}
			ordinaryTarget := record.Origin
			if !sameOrigin {
				ordinaryTarget.EntityID = uuid.NewString()
			}
			routes := []events.DeliveryRoute{makeRoute(response, "requester", record.Origin), makeRoute(ordinary, "response-observer", ordinaryTarget)}
			first := snapshot.Deliveries[0]
			first.Snapshot.EventID, first.Snapshot.Route = record.AcceptedReplyEventID, routes[0]
			second := first
			second.Snapshot.Route = routes[1]
			snapshot.Deliveries = []runForkRevisionDelivery{first, second}
			before := append([]runForkRevisionDelivery(nil), snapshot.Deliveries...)
			continuation := pinrouting.ReplyReturnContinuation{Record: record, Plan: response, OrdinaryPlans: []pinrouting.ConnectRoutePlan{ordinary}}
			if err := requireCapturedReplyReturnClaims(snapshot, record.AcceptedReplyEventID, continuation); err != nil {
				t.Errorf("valid source response subset: %v", err)
			}
			if !reflect.DeepEqual(before, snapshot.Deliveries) {
				t.Fatal("source subset validation rewrote captured evidence")
			}
			for _, fault := range []string{"claim", "context", "projection", "missing_plan", "duplicate_plan", "reply_plan"} {
				t.Run("ordinary_"+fault, func(t *testing.T) {
					bad := *snapshot
					bad.Deliveries = append([]runForkRevisionDelivery(nil), snapshot.Deliveries...)
					candidate := continuation.Normalized()
					switch fault {
					case "claim":
						bad.Deliveries[1].Snapshot.Route.ConnectClaim = routes[0].ConnectClaim
					case "context":
						bad.Deliveries[1].Snapshot.Route.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: uuid.NewString()}}
					case "projection":
						projection, err := events.NewDeliveryPayloadProjection(map[string]string{"result": "request_id"})
						if err != nil {
							t.Fatal(err)
						}
						bad.Deliveries[1].Snapshot.Route.PayloadProjection = projection
					case "missing_plan":
						candidate.OrdinaryPlans = nil
					case "duplicate_plan":
						candidate.OrdinaryPlans = append(candidate.OrdinaryPlans, ordinary)
					case "reply_plan":
						candidate.OrdinaryPlans[0] = response
					}
					if err := requireCapturedReplyReturnClaims(&bad, record.AcceptedReplyEventID, candidate); err == nil {
						t.Fatal("hostile complete ordinary claim or plan admitted")
					}
					if !reflect.DeepEqual(before, snapshot.Deliveries) || !continuation.OrdinaryPlans[0].ReceiverPinIdentity().Equal(ordinary.ReceiverPinIdentity()) {
						t.Fatal("hostile source check or normalized copy changed original evidence")
					}
				})
			}
			original := routes[1]
			if err := bindReplyReturnRecipientClaims(routes, continuation); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, routes[1]) {
				t.Fatal("ordinary observer was rebound to the response claim")
			}
		})
	}
}
