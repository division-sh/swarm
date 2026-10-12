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
			err = requireCapturedReplyReturnClaims(snapshot, record.AcceptedReplyEventID, record, plan)
			if (err == nil) != (fault == "exact") {
				t.Fatalf("source response claim %s: %v", fault, err)
			}
			if !reflect.DeepEqual(before, snapshot.Deliveries) {
				t.Fatal("source claim validation rewrote captured evidence")
			}
		})
	}
}
