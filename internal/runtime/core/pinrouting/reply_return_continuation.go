package pinrouting

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

// A retained return consumes an already accepted context. It is never a new
// reply claim; the native cut owner must admit its exact physical occurrence.
type ReplyReturnContinuation struct {
	Record replycontext.Record
	Plan   ConnectRoutePlan
}

func (c ReplyReturnContinuation) Validate() error {
	if err := c.Record.Validate(); err != nil {
		return err
	}
	if c.Record.State != replycontext.StateTerminal || c.Plan.ReplyRole() != ConnectReplyRoleResponse || !c.Plan.MatchesReplyRecord(c.Record) {
		return fmt.Errorf("retained return requires its terminal paired obligation and response plan")
	}
	return nil
}

func (c ReplyReturnContinuation) ClaimForDelivery(route events.DeliveryRoute) (events.ConnectExecutionClaim, error) {
	if err := c.Validate(); err != nil {
		return events.ConnectExecutionClaim{}, err
	}
	if route.Target.Route() != c.Record.Origin {
		return events.ConnectExecutionClaim{}, fmt.Errorf("retained return changed its context-owned destination")
	}
	compiled := ConnectDeliveryRoute{Recipient: route.Recipient, Target: c.Record.Origin,
		Context: route.Context, PayloadProjection: route.PayloadProjection}
	if route.Recipient.IsAgent() {
		plan, err := route.AgentIdentity.Plan()
		if err != nil {
			return events.ConnectExecutionClaim{}, err
		}
		compiled.AgentPlan = plan
	} else {
		node, _, found := route.ConnectClaim.NodeHandlerOwner()
		if !found {
			return events.ConnectExecutionClaim{}, fmt.Errorf("retained return lacks its captured handler claim")
		}
		compiled.Handler = MustConnectReceiverHandler(node)
	}
	return ConnectExecutionClaim(c.Plan, compiled)
}
