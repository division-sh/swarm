package pinrouting

import (
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

// A retained return consumes an already accepted context. It is never a new
// reply claim; the native cut owner must admit its exact physical occurrence.
type ReplyReturnContinuation struct {
	Record        replycontext.Record
	Plan          ConnectRoutePlan
	OrdinaryPlans []ConnectRoutePlan
}

func (c ReplyReturnContinuation) Normalized() ReplyReturnContinuation {
	c.Record = c.Record.Normalized()
	c.OrdinaryPlans = slices.Clone(c.OrdinaryPlans)
	return c
}

func (c ReplyReturnContinuation) Validate() error {
	if err := c.Record.Validate(); err != nil {
		return err
	}
	if c.Record.State != replycontext.StateTerminal || c.Plan.ReplyRole() != ConnectReplyRoleResponse || !c.Plan.MatchesReplyRecord(c.Record) {
		return fmt.Errorf("retained return requires its terminal paired obligation and response plan")
	}
	seen := make(map[ConnectReceiverPinIdentity]bool, len(c.OrdinaryPlans))
	for _, plan := range c.OrdinaryPlans {
		pin := plan.ReceiverPinIdentity()
		if plan.ReplyRole() != 0 || plan.source != c.Plan.source || pin.Empty() || pin.Equal(c.Plan.ReceiverPinIdentity()) || seen[pin] {
			return fmt.Errorf("retained return has a foreign, reply or duplicate ordinary plan")
		}
		seen[pin] = true
	}
	return nil
}

func (c ReplyReturnContinuation) ClaimForDelivery(route events.DeliveryRoute) (events.ConnectExecutionClaim, error) {
	if err := c.Validate(); err != nil {
		return events.ConnectExecutionClaim{}, err
	}
	if !c.MatchesResponseRecipient(route) || route.Target.Route() != c.Record.Origin {
		return events.ConnectExecutionClaim{}, fmt.Errorf("retained return changed its context-owned destination")
	}
	return retainedRecipientClaim(c.Plan, route, route.Target.Route())
}

// Ordinary recipients keep their own compiled edge and context. Only execution
// coordinates change; their original full claims are checked by native admission.
func (c ReplyReturnContinuation) OrdinaryClaimForDelivery(route events.DeliveryRoute) (events.ConnectExecutionClaim, bool, error) {
	if err := c.Validate(); err != nil {
		return events.ConnectExecutionClaim{}, false, err
	}
	for _, plan := range c.OrdinaryPlans {
		if !retainedRecipientMatches(plan, route) {
			continue
		}
		claim, err := plan.ClaimForRetainedDelivery(route)
		return claim, true, err
	}
	return events.ConnectExecutionClaim{}, false, nil
}

// This recomputes evidence only. The native continuation owner must first
// match the original complete claim before using the plan for a child route.
func (p ConnectRoutePlan) ClaimForRetainedDelivery(route events.DeliveryRoute) (events.ConnectExecutionClaim, error) {
	if !retainedRecipientMatches(p, route) {
		return events.ConnectExecutionClaim{}, fmt.Errorf("retained recipient differs from its compiled receiver pin")
	}
	target := route.Target.Route()
	if p.ReplyRole() == ConnectReplyRoleRequest {
		var err error
		target, err = (ReplyRequestContinuation{Plan: p}).claimTarget(target)
		if err != nil {
			return events.ConnectExecutionClaim{}, err
		}
	} else if p.ReplyRole() == 0 && p.ReceiverEndpoint().IsRoot() {
		// Root composition adds state ownership after hashing the blueprint.
		target.EntityID = ""
	}
	return retainedRecipientClaim(p, route, target)
}

func retainedRecipientClaim(plan ConnectRoutePlan, route events.DeliveryRoute, target events.RouteIdentity) (events.ConnectExecutionClaim, error) {
	compiled := ConnectDeliveryRoute{Recipient: route.Recipient, Target: target,
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
	return ConnectExecutionClaim(plan, compiled)
}

// Membership is the captured compiled response pin, not the requester address:
// another connection can legitimately deliver to that same instance.
func (c ReplyReturnContinuation) MatchesResponseRecipient(route events.DeliveryRoute) bool {
	return c.Plan.ReplyRole() == ConnectReplyRoleResponse && c.Plan.MatchesReplyRecord(c.Record) && retainedRecipientMatches(c.Plan, route)
}

func retainedRecipientMatches(plan ConnectRoutePlan, route events.DeliveryRoute) bool {
	receiver, present := route.ConnectClaim.ReceiverIdentity()
	event, hasEvent := route.ConnectClaim.ReceiverEvent()
	return present && hasEvent && receiver == plan.ReceiverPinIdentity().EvidenceIdentity() && event == plan.ReceiverLocalEvent()
}
