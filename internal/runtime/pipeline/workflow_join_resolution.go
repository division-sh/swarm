package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
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
	return workflowJoinOccurrenceDeliveryTarget(source, evt, resolution.Ref)
}

// The sealed publication has already proved the original occurrence. Selection
// maps that declaration to compiled counterfactual code; it does not rewrite the
// original event or relax the live event's exact-artifact validation.
func ResolvePublishedJoinOccurrenceDeliveryTarget(source semanticview.Source, publication runfork.PublishedJoinOccurrence) (events.DeliveryRecipient, events.RouteIdentity, DeliveryTargetHandler, bool, error) {
	if source == nil {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, fmt.Errorf("published join requires selected semantic source")
	}
	command := publication.Command()
	if err := command.Validate(); err != nil {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, err
	}
	payload, object := command.Payload.Interface().(map[string]any)
	_, original, valid := timeridentity.ParseJoinHandle(payload)
	if !object || !valid {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, fmt.Errorf("published join lacks its exact retained declaration")
	}
	if original.Mode() != timeridentity.JoinRefModeFanOutDelivery {
		return ResolveWorkflowJoinOccurrenceDeliveryTarget(source, publication.Event())
	}
	ref, err := selectedRetainedBarrierDeclaration(source, original)
	if err != nil {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, err
	}
	return workflowJoinOccurrenceDeliveryTarget(source, publication.Event(), ref)
}

func selectedRetainedBarrierDeclaration(source semanticview.Source, original timeridentity.JoinRef) (timeridentity.JoinRef, error) {
	retained, valid := original.FanOutDelivery()
	selected, found := semanticview.WorkflowJoinPlanForHandler(source, original.Node(), original.HandlerEvent())
	if !valid || !found || selected.Mode != runtimecontracts.WorkflowJoinModeFanOutDelivery || selected.Spec.EffectiveID() != original.JoinID() ||
		selected.FanOut.FanOut.ElementRef != runtimecontracts.FanOutElementRefFrom(retained.DeclarationIdentity()) {
		return timeridentity.JoinRef{}, fmt.Errorf("retained barrier has no exact selected declaration counterpart")
	}
	carriage, err := semanticview.CompileFanOutPlanCarriage(source)
	if err != nil {
		return timeridentity.JoinRef{}, err
	}
	compiled, _, err := carriage.Resolve(selected.FanOut.FanOut.ElementRef)
	if err != nil {
		return timeridentity.JoinRef{}, err
	}
	if compiled != selected.FanOut.FanOut {
		return timeridentity.JoinRef{}, fmt.Errorf("retained barrier selected join contradicts its compiled fan-out evidence")
	}
	handler, found := source.ExecutableNodeEventHandlers(original.Node())[original.HandlerEvent()]
	if !found || handler.Join == nil {
		return timeridentity.JoinRef{}, fmt.Errorf("retained barrier selected declaration has no executable handler")
	}
	ref, err := workflowJoinDeclarationRef(source, original.Node(), original.HandlerEvent(), handler)
	if err != nil {
		return timeridentity.JoinRef{}, err
	}
	return ref, nil
}

func workflowJoinOccurrenceDeliveryTarget(source semanticview.Source, evt events.Event, ref timeridentity.JoinRef) (events.DeliveryRecipient, events.RouteIdentity, DeliveryTargetHandler, bool, error) {
	return workflowJoinOwnerDeliveryTarget(source, ref, evt.RunID(), evt.EntityID(), evt.FlowInstance())
}

// Pending obligations use the already-bound owner and declaration, never an
// invented physical event or fresh membership/return-destination resolution.
func ResolvePendingJoinOccurrenceDeliveryTarget(source semanticview.Source, occurrence runtimegenericschedule.TransferredJoinOccurrence) (events.DeliveryRecipient, events.RouteIdentity, DeliveryTargetHandler, bool, error) {
	if source == nil {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, fmt.Errorf("pending join requires selected semantic source")
	}
	if err := occurrence.Validate(); err != nil {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, err
	}
	command := occurrence.Command
	_, ref, valid := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	if !valid {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, fmt.Errorf("pending join lacks exact retained declaration")
	}
	if ref.Mode() == timeridentity.JoinRefModeFanOutDelivery {
		var err error
		ref, err = selectedRetainedBarrierDeclaration(source, ref)
		if err != nil {
			return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, err
		}
	} else if _, found := semanticview.WorkflowJoinPlanForRef(source, ref); !found {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, fmt.Errorf("pending join references unknown exact selected declaration")
	}
	handler, found := source.ExecutableNodeEventHandlers(ref.Node())[ref.HandlerEvent()]
	if !found || handler.Join == nil {
		return events.DeliveryRecipient{}, events.RouteIdentity{}, DeliveryTargetHandler{}, false, fmt.Errorf("pending join has no executable selected handler")
	}
	return workflowJoinOwnerDeliveryTarget(source, ref, command.RunID, command.EntityID, command.FlowInstance)
}

func workflowJoinOwnerDeliveryTarget(source semanticview.Source, ref timeridentity.JoinRef, runID, entityID, instancePath string) (events.DeliveryRecipient, events.RouteIdentity, DeliveryTargetHandler, bool, error) {
	var noRecipient events.DeliveryRecipient
	executionFlowID := ref.FlowPath()
	target := events.RouteIdentity{EntityID: strings.TrimSpace(entityID)}
	if executionFlowID == semanticview.RootExecutionFlowID(source) {
		target.FlowID = executionFlowID
		target.FlowInstance = strings.TrimSpace(runID)
	} else {
		target.FlowID = executionFlowID
		target.FlowInstance = strings.Trim(strings.TrimSpace(instancePath), "/")
	}
	target = target.Normalized()
	if target.FlowID == "" || target.FlowInstance == "" || target.EntityID == "" {
		return noRecipient, events.RouteIdentity{}, DeliveryTargetHandler{}, false,
			fmt.Errorf("join lifecycle delivery target requires exact flow, instance, and entity identity")
	}
	handler, err := NewDeliveryTargetHandler(ref.Node())
	if err != nil {
		return noRecipient, events.RouteIdentity{}, DeliveryTargetHandler{}, false, err
	}
	handler = handler.ForEvent(events.EventType(ref.HandlerEvent()))
	return events.MustNodeDeliveryRecipient(ref.Node()), target, handler, true, nil
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
	if ref.FlowPath() == semanticview.RootExecutionFlowID(source) {
		flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
		if routing.Kind() != events.RoutingSourceRoot || route.FlowID != "" ||
			(flowInstance != "" && flowInstance != strings.TrimSpace(evt.RunID())) {
			return workflowJoinOccurrenceResolution{}, false, fmt.Errorf(
				"root join lifecycle event contradicts its explicit root declaration: source=%v route=%#v flow_instance=%q",
				routing.Kind(), route, evt.FlowInstance(),
			)
		}
	} else if routing.Kind() != events.RoutingSourceFlowOwnedControl || route.FlowID != ref.FlowPath() || route.FlowInstance != strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") {
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
	var ref timeridentity.JoinRef
	var err error
	switch plan.Mode {
	case runtimecontracts.WorkflowJoinModeArrival:
		ref, err = timeridentity.NewJoinRef(plan.Node, handlerEvent, handler.Join.Stage, handler.Join.EffectiveID())
	case runtimecontracts.WorkflowJoinModeFanOutDelivery:
		fanOutDeclaration, identityErr := plan.FanOut.FanOut.ElementRef.DeclarationIdentity()
		if identityErr != nil {
			err = identityErr
		} else {
			ref, err = timeridentity.NewFanOutDeliveryJoinRef(
				plan.Node, handlerEvent, handler.Join.EffectiveID(), fanOutDeclaration,
				plan.FanOut.FanOut.BundleHash, plan.FanOut.FanOut.SemanticDigest,
			)
		}
	default:
		err = fmt.Errorf("join handler has invalid compiled mode")
	}
	if err != nil {
		return timeridentity.JoinRef{}, err
	}
	if resolved, ok := semanticview.WorkflowJoinPlanForRef(source, ref); !ok || !resolved.Node.Equal(plan.Node) {
		return timeridentity.JoinRef{}, fmt.Errorf("join handler has no exact semantic declaration")
	}
	return ref, nil
}
