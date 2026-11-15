package bus

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

// A paired response consumes readiness rather than admitting another requester;
// independently authored connections keep their own compiled membership.
func TestAwaitReplyAdmissionKeepsPairedReturnNonexecutable(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyMixedReplyResponse(t))
	var request, response, ordinary pinrouting.ConnectRoutePlan
	for _, plan := range pinrouting.CompileConnectGraph(source).Plans() {
		switch plan.ReplyRole() {
		case pinrouting.ConnectReplyRoleRequest:
			request = plan
		case pinrouting.ConnectReplyRoleResponse:
			response = plan
		}
		if plan.ReceiverLocalEvent() == "provider.observed" {
			ordinary = plan
		}
	}
	_, event, _ := selectedRecipientContextFixture(t)
	record, err := request.ReplyRequestRecord(event, events.RouteIdentity{FlowID: ".", FlowInstance: event.RunID(), EntityID: event.RunID()}, pinrouting.AdmitConnectRouteMatchValues(map[string]string{"payload.request_id": "key"}), event.CreatedAt())
	if err != nil {
		t.Fatal(err)
	}
	record.Await = &replycontext.AwaitBinding{Version: "reply-await.v1", ReplyContextID: record.ID, RequestEventID: record.RequestEventID, RunID: record.RunID, OriginLife: record.OriginLife,
		DeliveryID: "delivery", ClaimVersion: 1, ExecutionOwner: "original", OperationID: "operation", AttemptID: "attempt", BundleHash: "source", PlanFingerprint: "plan", FrameID: "frame", ResponseFingerprint: "response", CallID: "call", CompletedResults: "[]"}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	validate := func(command CommitPublishRequest) error {
		projected, _, err := ResolvePreparedPublishEventTargetProjection(event, command.DeliveryRoutes)
		if err != nil {
			return err
		}
		command.Event, err = events.AdmitForPersistence(projected, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
		if err != nil {
			return err
		}
		if len(command.DeliveryRoutes) == 0 {
			command.RouteSettlement, err = events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
		} else {
			command.RouteSettlement, err = events.NewDeliverySettlement(events.EventWriteNormalPublication, events.ConnectEvaluationLedger{})
		}
		if err != nil {
			return err
		}
		return command.ValidatePreparedEvent()
	}
	command := CommitPublishRequest{ReplyClaims: []replycontext.ClaimCommand{{Expected: record, ReplyEventID: event.ID()}}, ConnectPlans: []pinrouting.ConnectRoutePlan{request, response, ordinary}}
	if err := validate(command); err != nil {
		t.Fatalf("ready-only reply refused: %v", err)
	}
	node := identitytest.FlowNode(t, ".", "requester")
	compiled := pinrouting.ConnectDeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: record.Origin, Handler: pinrouting.MustConnectReceiverHandler(node)}
	compiled.ConnectClaim, err = pinrouting.ConnectExecutionClaim(response, compiled)
	if err != nil {
		t.Fatal(err)
	}
	paired := events.DeliveryRoute{Recipient: compiled.Recipient, Target: events.MustExistingEntityTarget(record.Origin), ConnectClaim: compiled.ConnectClaim}
	command.DeliveryRoutes = []events.DeliveryRoute{paired}
	if err := validate(command); err == nil || !strings.Contains(err.Error(), "paired requester delivery") {
		t.Fatalf("paired invocation admitted: %v", err)
	}
	withoutPlans := command
	withoutPlans.ConnectPlans = nil
	if err := validate(withoutPlans); err == nil {
		t.Fatal("erasing compiled plans admitted an await delivery")
	}
	erasedClaim := command
	erasedClaim.DeliveryRoutes = []events.DeliveryRoute{paired}
	erasedClaim.DeliveryRoutes[0].ConnectClaim = events.ConnectExecutionClaim{}
	if err := validate(erasedClaim); err == nil || !strings.Contains(err.Error(), "compiled connection membership") {
		t.Fatalf("paired plan with erased route claim admitted an await delivery: %v", err)
	}
	observer := identitytest.FlowNode(t, ".", "response-observer")
	blueprint := record.Origin
	blueprint.EntityID = ""
	independent := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(observer), Target: events.MustExistingEntityTarget(record.Origin)}
	independent.ConnectClaim, err = pinrouting.ConnectExecutionClaim(ordinary, pinrouting.ConnectDeliveryRoute{Recipient: independent.Recipient, Target: blueprint, Handler: pinrouting.MustConnectReceiverHandler(observer)})
	if err != nil {
		t.Fatal(err)
	}
	command.DeliveryRoutes = []events.DeliveryRoute{independent}
	if independent.Target.Route() != paired.Target.Route() {
		t.Fatal("independent fixture must address the same requester instance")
	}
	if err := validate(command); err != nil {
		t.Fatalf("independent compiled branch was globally suppressed: %v", err)
	}
	changedClaim := command
	changedClaim.DeliveryRoutes = []events.DeliveryRoute{independent}
	changedClaim.DeliveryRoutes[0].Context.Reply = &events.ReplyContextRef{ID: record.ID}
	if err := validate(changedClaim); err == nil || !strings.Contains(err.Error(), "compiled connection membership") {
		t.Fatalf("receiver membership alone accepted a changed full delivery claim: %v", err)
	}
	command.ReplyClaims[0].Expected.Await = nil
	command.DeliveryRoutes = []events.DeliveryRoute{paired}
	if err := validate(command); err != nil {
		t.Fatalf("ordinary reply semantics changed: %v", err)
	}
}
