package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type committedAgentHandoff uint8

const (
	noCommittedAgentHandoff committedAgentHandoff = iota
	transferredAgentHandoff
)

func pureCommittedAgentTransition(ctx context.Context, err error) bool {
	return ctx.Err() == nil && errors.Is(err, ErrCommittedAgentRouteTransition) &&
		failures.OnlyBranches(err, func(branch error) bool { return branch == ErrCommittedAgentRouteTransition })
}

func (eb *EventBus) finalizePublicationAgentReadiness(ctx context.Context, event events.Event, routes []events.DeliveryRoute) ([]events.DeliveryRoute, error) {
	var transitions []events.DeliveryRoute
	var result error
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if !route.Recipient.IsAgent() {
			continue
		}
		err := eb.finalizeEngineAgentReadiness(ctx, event, []events.DeliveryRoute{route})
		if pureCommittedAgentTransition(ctx, err) {
			transitions = append(transitions, route)
		}
		result = errors.Join(result, err)
	}
	return transitions, result
}

func (eb *EventBus) transferCommittedAgentReadiness(ctx context.Context, prepared PreparedPublish, committed CommittedPublication, transitions []events.DeliveryRoute) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !committed.Acknowledged || committed.AppendOutcome != EventAppendInserted || prepared.targetFailure || prepared.dispatchQueued || len(transitions) == 0 {
		return errors.New("agent readiness transition requires a newly acknowledged executable publication")
	}
	owner := eb.DeliveryContinuationOwner()
	authority, err := eb.DeliveryAuthority()
	if err != nil || authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime || owner == nil || !owner.OwnsPersistedRecovery() {
		return errors.Join(err, errors.New("agent readiness transition requires its normal-runtime coordinator"))
	}
	if err := eb.validateAgentTransitionCoverage(prepared, committed.DeliveryHandoffs, authority); err != nil {
		return err
	}
	return eb.AcceptCommittedDeliveryHandoffs(committed.DeliveryHandoffs)
}

// Transfer is valid only when every recipient, including independent internal
// recipients, has its own exact durable route. Event-wide decisions stay owned
// by ordinary publication dispatch and cannot be bypassed by this branch.
func (eb *EventBus) validateAgentTransitionCoverage(prepared PreparedPublish, proofs []deliverylifecycle.DurableHandoffProof, authority deliverylifecycle.ExecutionAuthority) error {
	routes := prepared.plan.DeliveryRoutes()
	if len(routes) == 0 || len(routes) != len(proofs) || !durableRoutesCoverLiveRecipients(prepared.plan.LiveRecipients, routes) {
		return errors.New("agent readiness transition lacks complete exact recipient handoffs")
	}
	expected := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		id, err := deliverylifecycle.DeliveryID(prepared.Event.ID(), route)
		if err != nil {
			return err
		}
		expected[id] = struct{}{}
	}
	for _, proof := range proofs {
		if err := proof.Validate(); err != nil || proof.EventID() != prepared.Event.ID() || !proof.Authority().Equal(authority) {
			return errors.Join(err, errors.New("agent readiness handoff differs from event or generation"))
		}
		if _, found := expected[proof.DeliveryID()]; !found {
			return errors.New("agent readiness handoff names an unrelated or duplicate route")
		}
		delete(expected, proof.DeliveryID())
	}
	if len(expected) != 0 {
		return errors.New("agent readiness handoff omits an exact route")
	}
	eventInterceptors, _ := splitDeliveryRouteInterceptors(eb.interceptorsSnapshot())
	if len(eventInterceptors) != 0 {
		return errors.New("agent readiness handoff cannot bypass event-wide coordination")
	}
	return nil
}

func durableRoutesCoverLiveRecipients(recipients []RoutePlanLiveRecipient, routes []events.DeliveryRoute) bool {
	for _, recipient := range recipients {
		covered := false
		for _, route := range routes {
			if recipient.Recipient.IsAgent() {
				covered = route.Recipient == recipient.Recipient && route.AgentIdentity.Normalize() == recipient.AgentIdentity.Normalize()
			} else if recipient.Recipient.IsNode() {
				covered = route.Recipient == recipient.Recipient
			} else {
				covered = recipient.isInternal() && route.Recipient.IsNode() && route.Recipient.ID() == recipient.subscriberID()
			}
			if covered {
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func (eb *EventBus) completeCommittedAgentHandoff(ctx context.Context, event events.Event, claim *pipelinePublicationClaim) (err error) {
	eb.notifyTestPostCommitDispatchStarted(ctx, event)
	defer eb.notifyTestPostCommitDispatchCompleted(ctx, event)
	if claim == nil || claim.bus != eb {
		return fmt.Errorf("agent readiness handoff requires its exact publication claim")
	}
	// The existing receipt transaction makes the already-transferred routes
	// executable. Their coordinator, not this publication, waits for topology.
	outcome, err := claim.Settle(ctx, pipelineobligation.Acknowledged("pipeline_persisted"))
	if outcome.DeliveryHandoffCommitted() {
		eb.clearPendingInternalDeliveryRoutes(event.ID())
	}
	eb.logPublished(ctx, event, 0)
	return err
}
