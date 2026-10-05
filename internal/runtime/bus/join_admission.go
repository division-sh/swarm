package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
)

func (eb *EventBus) prepareJoinAdmission(ctx context.Context, evt events.Event, prospective pipeline.PreparedWorkflowPublicationState, plan *RoutePlan) error {
	if eb.semanticSource == nil || isJoinLifecycleControlEvent(evt.Type()) {
		return nil
	}
	if err := eb.prepareDeliveryJoinAdmission(ctx, evt, prospective, plan); err != nil {
		return err
	}
	var returnOwners *selectedRunTargetOwnerProjection
	for index := range plan.ReplyCreations {
		if err := eb.prepareReplyJoinAdmission(ctx, prospective, plan, &plan.ReplyCreations[index], &returnOwners); err != nil {
			return err
		}
	}
	return nil
}

func (eb *EventBus) prepareDeliveryJoinAdmission(ctx context.Context, evt events.Event, prospective pipeline.PreparedWorkflowPublicationState, plan *RoutePlan) error {
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		node, isNode := intent.Recipient.Node()
		if !intent.Persist || !isNode {
			continue
		}
		route := intent.deliveryRoute()
		event, err := pipeline.WorkflowJoinAdmissionEvent(eb.semanticSource, route, string(evt.Type()))
		if err != nil {
			return err
		}
		if len(pipeline.WorkflowJoinAdmissionPlans(eb.semanticSource, node, event)) == 0 && len(route.Context.Joins) == 0 {
			continue
		}
		receipts, fence, err := eb.prepareJoinRouteAdmission(ctx, evt.RunID(), event, prospective, plan, route)
		if err != nil {
			return err
		}
		intent.Context.Joins = receipts
		if fence != nil {
			plan.JoinAdmissionFences = append(plan.JoinAdmissionFences, *fence)
		}
	}
	return nil
}

func (eb *EventBus) prepareReplyJoinAdmission(ctx context.Context, prospective pipeline.PreparedWorkflowPublicationState, plan *RoutePlan, record *runtimereplycontext.Record, returnOwners **selectedRunTargetOwnerProjection) error {
	for _, returnPlan := range eb.connectRoutePlanner.graph.Plans() {
		if returnPlan.ReplyRole() != runtimepinrouting.ConnectReplyRoleResponse || !returnPlan.MatchesReplyRecord(*record) {
			continue
		}
		subscribers, err := eb.connectRoutePlanner.resolveSelectedReceiverCarriers(ctx, record.RunID, returnPlan, record.Origin)
		if err != nil {
			return err
		}
		for _, subscriber := range subscribers {
			node, isNode := subscriber.Recipient.Node()
			if !isNode || len(pipeline.WorkflowJoinAdmissionPlans(eb.semanticSource, node, string(returnPlan.ReceiverLocalEvent()))) == 0 {
				continue
			}
			if *returnOwners == nil {
				owners, err := eb.loadJoinReturnOwners(ctx, prospective, plan)
				if err != nil {
					return err
				}
				*returnOwners = owners
			}
			target, err := (*returnOwners).resolveSelectedRoute(record.Origin)
			if err != nil {
				return err
			}
			route := events.DeliveryRoute{Recipient: subscriber.Recipient, Target: target}
			receipts, fence, err := eb.prepareJoinRouteAdmission(ctx, record.RunID, string(returnPlan.ReceiverLocalEvent()), prospective, plan, route)
			if err != nil {
				return err
			}
			record.ReturnJoins = append(record.ReturnJoins, receipts...)
			if fence != nil {
				plan.JoinAdmissionFences = append(plan.JoinAdmissionFences, *fence)
			}
		}
	}
	return record.Validate()
}

func (eb *EventBus) loadJoinReturnOwners(ctx context.Context, prospective pipeline.PreparedWorkflowPublicationState, plan *RoutePlan) (*selectedRunTargetOwnerProjection, error) {
	policy := eb.deliveryPlanner.recipientPolicy
	policy.prospective = prospective
	owners, err := policy.loadSelectedRunTargetOwnerProjection(ctx)
	if err != nil {
		return nil, err
	}
	owners, err = owners.withActivationPlans(plan.ActivationPlans)
	return &owners, err
}

func (eb *EventBus) prepareJoinRouteAdmission(ctx context.Context, runID, event string, prospective pipeline.PreparedWorkflowPublicationState, plan *RoutePlan, route events.DeliveryRoute) ([]events.JoinAdmissionReceipt, *pipeline.WorkflowJoinAdmissionFence, error) {
	node, isNode := route.Recipient.Node()
	if !isNode || len(pipeline.WorkflowJoinAdmissionPlans(eb.semanticSource, node, event)) == 0 && len(route.Context.Joins) == 0 {
		return nil, nil, nil
	}
	var instance *pipeline.WorkflowInstance
	if len(route.Context.Joins) == 0 {
		var err error
		instance, err = prospective.JoinAdmissionInstance(route.Target.Route())
		if err != nil {
			return nil, nil, err
		}
		for _, activation := range plan.ActivationPlans {
			if instance == nil && activation.Identity.EntityID == route.Target.Route().EntityID && activation.Identity.InstancePath == route.Target.Route().FlowInstance {
				item := activation.Instance
				item.Revision = 1
				instance = &item
			}
		}
		if instance == nil {
			instance, err = eb.loadJoinAdmissionInstance(ctx, runID, route.Target.Route())
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return pipeline.PrepareWorkflowJoinAdmission(eb.semanticSource, runID, event, route, instance)
}

func (eb *EventBus) loadJoinAdmissionInstance(ctx context.Context, runID string, target events.RouteIdentity) (*pipeline.WorkflowInstance, error) {
	reader, canRead := eb.store.(pipeline.WorkflowTargetPersistenceReader)
	if !canRead {
		return nil, fmt.Errorf("selected store lacks complete join receiver persistence reader")
	}
	owner, err := pipeline.WorkflowJoinAdmissionOwner(eb.semanticSource, runID, target)
	if err != nil {
		return nil, err
	}
	record, err := reader.LoadWorkflowTargetPersistence(ctx, owner, identity.EntityID(target.EntityID))
	if err != nil || record.Presence == pipeline.WorkflowTargetPersistenceAbsent {
		return nil, err
	}
	item, err := record.DecodeComplete(owner.Route, identity.EntityID(target.EntityID))
	return &item, err
}

func isJoinLifecycleControlEvent(event events.EventType) bool {
	return event == "platform.join_complete" || event == "platform.join_timeout"
}
