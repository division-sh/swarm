package pinrouting

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

// The plan is input to the existing claim owner, not a replacement claim or
// execution grant. Native admission checks it against the cut's complete claim.
type ReplyRequestContinuation struct {
	Record replycontext.Record
	Plan   ConnectRoutePlan
}

func (c ReplyRequestContinuation) Validate() error {
	if err := c.Record.Validate(); err != nil {
		return err
	}
	if c.Record.State != replycontext.StateOpen || c.Plan.ReplyRole() != ConnectReplyRoleRequest || !c.Plan.MatchesReplyRecord(c.Record) {
		return fmt.Errorf("request continuation requires its open paired obligation and compiled plan")
	}
	return nil
}

func (c ReplyRequestContinuation) ClaimForDelivery(route events.DeliveryRoute) (events.ConnectExecutionClaim, error) {
	if err := c.Validate(); err != nil {
		return events.ConnectExecutionClaim{}, err
	}
	target, err := c.claimTarget(route.Target.Route())
	if err != nil {
		return events.ConnectExecutionClaim{}, err
	}
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
			return events.ConnectExecutionClaim{}, fmt.Errorf("request continuation lacks its captured handler claim")
		}
		compiled.Handler = MustConnectReceiverHandler(node)
	}
	return ConnectExecutionClaim(c.Plan, compiled)
}

// Fixed targets are compiled blueprints. Durable receiver acquisition may add
// an entity to that address later; native admission separately checks the full
// acquired route. Runtime-resolved targets already carry descriptor ownership.
func (c ReplyRequestContinuation) claimTarget(acquired events.RouteIdentity) (events.RouteIdentity, error) {
	if c.Plan.RequiresRuntimeResolution() {
		return acquired, nil
	}
	if c.Plan.ReceiverEndpoint().IsRoot() {
		if acquired.FlowID != "." || acquired.FlowInstance == "" {
			return events.RouteIdentity{}, fmt.Errorf("request claim has a foreign acquired root")
		}
		return events.RouteIdentity{}, nil
	}
	materialized := MaterializeConnectRoutePlan(c.Plan)
	targets := append([]events.RouteIdentity(nil), materialized.TargetSet...)
	if !materialized.Target.Empty() {
		targets = append(targets, materialized.Target)
	}
	for _, target := range targets {
		if materializedRouteAcceptsTarget(target, acquired) {
			return target, nil
		}
	}
	return events.RouteIdentity{}, fmt.Errorf("request claim contradicts its compiled fixed target")
}
