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

func requestContinuationPartitionFixture(t *testing.T) (*runForkRevisionSnapshot, string, string, pinrouting.ReplyRequestContinuation, events.DeliveryRoute) {
	t.Helper()
	source := workflowOwnershipSource(t, canonicalrouting.CopyRootReplyBoundary(t, true, true))
	var plan pinrouting.ConnectRoutePlan
	for _, candidate := range pinrouting.CompileConnectGraph(source).Plans() {
		if candidate.ReplyRole() == pinrouting.ConnectReplyRoleRequest {
			plan = candidate
		}
	}
	snapshot, event, _ := replayCutFixture(t)
	record, err := plan.ReplyRequestRecord(event, events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID, EntityID: snapshot.RunID},
		pinrouting.AdmitConnectRouteMatchValues(map[string]string{"payload.request_id": "key"}), event.CreatedAt())
	if err != nil {
		t.Fatal(err)
	}
	node := identitytest.FlowNode(t, "provider", "provider-node")
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node),
		Target:  events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "provider", FlowInstance: "provider"}),
		Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}}
	compiled := pinrouting.ConnectDeliveryRoute{Recipient: route.Recipient, Handler: pinrouting.MustConnectReceiverHandler(node),
		Target: route.Target.Route(), Context: route.Context}
	route.ConnectClaim, err = pinrouting.ConnectExecutionClaim(plan, compiled)
	if err != nil {
		t.Fatal(err)
	}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Deliveries = []runForkRevisionDelivery{{runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3, Revision: 5},
		Snapshot: deliverylifecycle.Snapshot{DeliveryID: id, EventID: event.ID(), RunID: snapshot.RunID, Route: route,
			Status: deliverylifecycle.StatusPending, FinalSelection: deliverylifecycle.AbsentSelection()}}}
	snapshot.ReplyContexts = []runForkRevisionReplyContext{{runForkHistoricalReplyFact: runForkHistoricalReplyFact{Record: record,
		runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3, Revision: 3}}}}
	child := uuid.NewString()
	projected, err := projectSelectedFanOutRecipient(snapshot, event, snapshot.Deliveries[0].Snapshot, child)
	if err != nil {
		t.Fatal(err)
	}
	record.ID, record.RunID = projected.Context.ReplyContextID(), child
	record.RequestEventID = deterministicRunForkReplayEventID(child, record.RequestEventID)
	record.Origin = events.RouteIdentity{FlowID: ".", FlowInstance: child, EntityID: child}
	continuation := pinrouting.ReplyRequestContinuation{Record: record, Plan: plan}
	if err := bindRunForkRequestRecipientClaim(route, &projected, []pinrouting.ReplyRequestContinuation{continuation}); err != nil {
		t.Fatal(err)
	}
	return snapshot, event.ID(), child, continuation, projected
}

func TestIssue642RequestContinuationPreservesCompleteCompiledClaim(t *testing.T) {
	for _, fault := range []string{"exact", "missing_route", "extra_route", "duplicate_route", "claim", "plan", "target", "projection", "context", "handler", "source_claim", "settled_source"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, event, child, reply, route := requestContinuationPartitionFixture(t)
			routes := []events.DeliveryRoute{route}
			switch fault {
			case "missing_route":
				routes = nil
			case "extra_route", "duplicate_route":
				routes = append(routes, route)
			case "claim":
				routes[0].ConnectClaim = events.ConnectExecutionClaim{}
			case "plan":
				reply.Plan = pinrouting.ConnectRoutePlan{}
			case "target":
				routes[0].Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "provider", FlowInstance: "foreign"})
			case "projection":
				projection, err := events.NewDeliveryPayloadProjection(map[string]string{"x": "y"})
				if err != nil {
					t.Fatal(err)
				}
				routes[0].PayloadProjection = projection
			case "context":
				routes[0].Context.Reply.ID = uuid.NewString()
			case "handler":
				routes[0].Recipient = events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "provider", "other"))
			case "source_claim":
				snapshot.Deliveries[0].Snapshot.Route.ConnectClaim = events.ConnectExecutionClaim{}
			case "settled_source":
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDelivered
			}
			before := projectionJSON(t, snapshot)
			input := append([]events.DeliveryRoute(nil), routes...)
			err := requireRunForkRequestRecipientPartition(snapshot, event, child, []pinrouting.ReplyRequestContinuation{reply}, routes)
			if (err == nil) != (fault == "exact") {
				t.Fatalf("complete request partition %s: %v", fault, err)
			}
			if before != projectionJSON(t, snapshot) || !reflect.DeepEqual(input, routes) {
				t.Fatal("validation repaired its input")
			}
		})
	}
}

func TestIssue642RequestContinuationRejectsSubstitutedCapturedRows(t *testing.T) {
	for _, fault := range []string{"exact", "missing", "duplicate", "foreign", "original", "enclosing", "clock", "terminal"} {
		t.Run(fault, func(t *testing.T) {
			_, _, _, continuation, route := requestContinuationPartitionFixture(t)
			expected := []replycontext.Record{continuation.Record.Normalized()}
			replies := []pinrouting.ReplyRequestContinuation{continuation}
			switch fault {
			case "missing":
				replies = nil
			case "duplicate":
				replies = append(replies, continuation)
			case "foreign":
				replies[0].Record.RunID = uuid.NewString()
			case "original":
				replies[0].Record.SourceRequestEventID = uuid.NewString()
			case "enclosing":
				replies[0].Record.EnclosingReplyContextID = uuid.NewString()
			case "clock":
				replies[0].Record.UpdatedAt = replies[0].Record.UpdatedAt.Add(time.Second)
			case "terminal":
				replies[0].Record.State = replycontext.StateTerminal
			}
			err := requireRunForkRequestReplyEvidence(expected, replies, []events.DeliveryRoute{route})
			if (err == nil) != (fault == "exact") {
				t.Fatalf("captured request binding %s: %v", fault, err)
			}
		})
	}
}
