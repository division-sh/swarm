package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type workflowJoinOccurrenceResolution struct {
	Handle  timeridentity.TimerHandle
	Ref     timeridentity.JoinRef
	Plan    runtimecontracts.WorkflowJoinPlan
	Handler runtimecontracts.SystemNodeEventHandler
}

// ResolveWorkflowJoinOccurrenceDeliveryTarget projects one strict lifecycle
// handle into the exact authored receiver and selected-run target. Synthetic
// lifecycle event names never become an alternate handler authority.
func ResolveWorkflowJoinOccurrenceDeliveryTarget(
	source semanticview.Source,
	evt events.Event,
) (events.DeliveryRecipient, events.RouteIdentity, DeliveryTargetHandler, bool, error) {
	var noRecipient events.DeliveryRecipient
	resolution, ok, err := resolveWorkflowJoinOccurrence(source, evt)
	if err != nil || !ok {
		return noRecipient, events.RouteIdentity{}, DeliveryTargetHandler{}, ok, err
	}
	executionFlowID := resolution.Ref.FlowID()
	target := events.RouteIdentity{EntityID: strings.TrimSpace(evt.EntityID())}
	if executionFlowID == "" {
		executionFlowID = semanticview.RootExecutionFlowID(source)
		target.FlowID = executionFlowID
		target.FlowInstance = strings.TrimSpace(evt.RunID())
	} else {
		target.FlowID = executionFlowID
		target.FlowInstance = strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	}
	target = target.Normalized()
	if target.FlowID == "" || target.FlowInstance == "" || target.EntityID == "" {
		return noRecipient, events.RouteIdentity{}, DeliveryTargetHandler{}, false,
			fmt.Errorf("join lifecycle delivery target requires exact flow, instance, and entity identity")
	}
	handler, err := NewDeliveryTargetHandler(resolution.Ref.Node())
	if err != nil {
		return noRecipient, events.RouteIdentity{}, DeliveryTargetHandler{}, false, err
	}
	handler = handler.ForEvent(events.EventType(resolution.Ref.HandlerEvent()))
	return events.MustNodeDeliveryRecipient(resolution.Ref.Node()), target, handler, true, nil
}

func resolveWorkflowJoinOccurrence(source semanticview.Source, evt events.Event) (workflowJoinOccurrenceResolution, bool, error) {
	if !isJoinLifecycleEvent(evt.Type()) {
		return workflowJoinOccurrenceResolution{}, false, nil
	}
	handle, ref, ok := timeridentity.ParseJoinHandle(parsePayloadMap(evt.Payload()))
	if !ok {
		return workflowJoinOccurrenceResolution{}, false, fmt.Errorf("join lifecycle event is missing its strict typed timer handle")
	}
	if handle.EventType() != strings.TrimSpace(string(evt.Type())) || handle.TaskID() != evt.TaskID() {
		return workflowJoinOccurrenceResolution{}, false, fmt.Errorf("join lifecycle event contradicts its typed timer handle")
	}
	producer := evt.Producer()
	if producer.Type() != events.EventProducerPlatform || producer.ID() != runtimegenericschedule.OccurrenceProducerID() {
		return workflowJoinOccurrenceResolution{}, false, fmt.Errorf("join lifecycle event requires the generic-schedule occurrence producer")
	}
	routing := evt.RoutingSource()
	route := routing.Route().Normalized()
	if route.EntityID != strings.TrimSpace(evt.EntityID()) {
		return workflowJoinOccurrenceResolution{}, false, fmt.Errorf("join lifecycle event routing source disagrees with its entity")
	}
	if ref.FlowID() == "" {
		flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
		if routing.Kind() != events.RoutingSourceRoot || route.FlowID != "" ||
			(flowInstance != "" && flowInstance != strings.TrimSpace(evt.RunID())) {
			return workflowJoinOccurrenceResolution{}, false, fmt.Errorf(
				"root join lifecycle event contradicts its explicit root declaration: source=%v route=%#v flow_instance=%q",
				routing.Kind(), route, evt.FlowInstance(),
			)
		}
	} else if routing.Kind() != events.RoutingSourceFlowOwnedControl || route.FlowID != ref.FlowID() || route.FlowInstance != strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") {
		return workflowJoinOccurrenceResolution{}, false, fmt.Errorf("flow-owned join lifecycle event contradicts its declaration route")
	}
	plan, ok := semanticview.WorkflowJoinPlanForRef(source, ref)
	if !ok {
		return workflowJoinOccurrenceResolution{}, false, fmt.Errorf("join lifecycle event references an unknown exact declaration")
	}
	handler, ok := source.ExecutableNodeEventHandlers(ref.Node())[ref.HandlerEvent()]
	if !ok || handler.Join == nil {
		return workflowJoinOccurrenceResolution{}, false, fmt.Errorf("join lifecycle declaration has no executable handler")
	}
	return workflowJoinOccurrenceResolution{Handle: handle, Ref: ref, Plan: plan, Handler: handler}, true, nil
}

func workflowJoinDeclarationForExecution(
	source semanticview.Source,
	evt events.Event,
	node runtimeidentity.ExecutableNode, handlerEvent string,
	handler runtimecontracts.SystemNodeEventHandler,
) (timeridentity.JoinRef, error) {
	if handler.Join == nil {
		return timeridentity.JoinRef{}, nil
	}
	if isJoinLifecycleEvent(evt.Type()) {
		resolution, ok, err := resolveWorkflowJoinOccurrence(source, evt)
		if err != nil {
			return timeridentity.JoinRef{}, err
		}
		if !ok || !resolution.Ref.Node().Equal(node) || resolution.Ref.HandlerEvent() != strings.TrimSpace(handlerEvent) {
			return timeridentity.JoinRef{}, fmt.Errorf("join lifecycle event does not select the exact handler declaration")
		}
		return resolution.Ref.Declaration(), nil
	}
	return workflowJoinDeclarationRef(source, node, handlerEvent, handler)
}

func workflowJoinDeclarationRef(source semanticview.Source, node runtimeidentity.ExecutableNode, handlerEvent string, handler runtimecontracts.SystemNodeEventHandler) (timeridentity.JoinRef, error) {
	if source == nil || handler.Join == nil {
		return timeridentity.JoinRef{}, fmt.Errorf("join declaration resolution requires semantic source and join handler")
	}
	if !node.Valid() {
		return timeridentity.JoinRef{}, fmt.Errorf("join handler requires its exact compiled execution scope")
	}
	plan, ok := semanticview.WorkflowJoinPlanForExecutionHandler(source, node, handlerEvent, *handler.Join)
	if !ok {
		return timeridentity.JoinRef{}, fmt.Errorf("join handler has no declaration in exact executable node scope %q", node.Key())
	}
	ref, err := timeridentity.NewJoinRef(plan.Node, handlerEvent, handler.Join.Stage, handler.Join.EffectiveID(), "")
	if err != nil {
		return timeridentity.JoinRef{}, err
	}
	if resolved, ok := semanticview.WorkflowJoinPlanForRef(source, ref); !ok || !resolved.Node.Equal(plan.Node) {
		return timeridentity.JoinRef{}, fmt.Errorf("join handler has no exact semantic declaration")
	}
	return ref, nil
}
