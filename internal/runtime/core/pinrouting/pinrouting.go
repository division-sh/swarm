package pinrouting

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type TargetFailure string

const (
	FailureTargetRequiredMissing       TargetFailure = "target_required_missing"
	FailureTargetNotSubscribed         TargetFailure = "target_not_subscribed"
	FailureTargetUnreachableTerminated TargetFailure = "target_unreachable_terminated"
	FailureParentRouteIncomplete       TargetFailure = "parent_route_incomplete"
	FailureReplyAlreadyTerminal        TargetFailure = "platform.reply_already_terminal"
	FailureStaleArrival                TargetFailure = "platform.stale_arrival"
)

type Descriptor struct {
	ID            string
	EntityID      string
	FlowInstance  string
	AddressFields map[string]string
}

type ResolutionInput struct {
	Source      semanticview.Source
	FlowID      string
	EventType   string
	SourceRoute events.RouteIdentity
	Inbound     events.Event
	ParentRoute events.RouteIdentity
	// Static child flows have no persisted ParentRoute row; they may route back to
	// the current delivery entity, but template/dynamic ParentRoute metadata must
	// remain complete and fail closed when partial.
	AllowEntityOnlyParentRoute bool
}

type Resolution struct {
	Event    events.Event
	Envelope events.EventEnvelope
	Target   events.RouteIdentity
	Failure  TargetFailure
}

func PinDeclaredOutput(source semanticview.Source, flowID, eventType string) bool {
	flowID = strings.TrimSpace(flowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return false
	}
	if flowID == "" {
		return rootPinDeclaredOutput(source, eventType)
	}
	if source.FlowHasOutputEvent(flowID, eventType) {
		return true
	}
	leaf := eventidentity.LeafName(eventType)
	if leaf != "" && source.FlowHasOutputEvent(flowID, leaf) {
		return true
	}
	resolved := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, eventType))
	if resolved != "" && source.FlowHasOutputEvent(flowID, resolved) {
		return true
	}
	for _, output := range source.FlowOutputEvents(flowID) {
		output = eventidentity.Normalize(output)
		if output == eventType || output == resolved || output == leaf {
			return true
		}
	}
	return false
}

func compositionConnectsFromOutputEvent(source semanticview.Source, flowID, eventType string) bool {
	return len(compositionConnectRoutePlansFromOutputEvent(source, flowID, eventType)) > 0
}

func compositionConnectRoutePlansFromOutputEvent(source semanticview.Source, flowID, eventType string) []ConnectRoutePlan {
	if source == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return nil
	}
	plans, _ := LowerCompositionConnectRoutePlans(source)
	out := make([]ConnectRoutePlan, 0)
	for _, plan := range plans {
		if strings.TrimSpace(plan.Source.FlowID) != flowID {
			continue
		}
		if eventReferencesOverlap(source, flowID, []string{eventType}, flowID, []string{plan.Source.Event, plan.Source.ResolvedEvent}) {
			out = append(out, plan)
		}
	}
	return out
}

func outputPinsForEvent(source semanticview.Source, flowID, eventType string) []runtimecontracts.FlowOutputEventPin {
	if source == nil {
		return nil
	}
	out := []runtimecontracts.FlowOutputEventPin{}
	for _, pin := range source.FlowOutputEventPins(flowID) {
		if eventReferencesOverlap(source, flowID, []string{eventType}, flowID, []string{pin.PinName(), pin.EventType()}) {
			out = append(out, pin)
		}
	}
	return out
}

func eventReferencesOverlap(source semanticview.Source, leftFlowID string, leftEvents []string, rightFlowID string, rightEvents []string) bool {
	leftRefs := eventReferences(source, leftFlowID, leftEvents...)
	rightRefs := eventReferences(source, rightFlowID, rightEvents...)
	for _, left := range leftRefs {
		for _, right := range rightRefs {
			if eventReferencesMatch(left, right) {
				return true
			}
		}
	}
	return false
}

func eventReferences(source semanticview.Source, flowID string, eventTypes ...string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	add := func(eventType string) {
		eventType = eventidentity.Normalize(eventType)
		if eventType == "" {
			return
		}
		if _, ok := seen[eventType]; ok {
			return
		}
		seen[eventType] = struct{}{}
		out = append(out, eventType)
	}
	for _, eventType := range eventTypes {
		add(eventType)
		if source != nil {
			add(source.ResolveFlowEventReference(flowID, eventType))
		}
	}
	return out
}

func eventReferencesMatch(left, right string) bool {
	left = eventidentity.Normalize(left)
	right = eventidentity.Normalize(right)
	if left == "" || right == "" {
		return false
	}
	if left == right {
		return true
	}
	leftQualified := strings.Contains(left, "/")
	rightQualified := strings.Contains(right, "/")
	if leftQualified == rightQualified {
		return false
	}
	return eventidentity.LeafName(left) == eventidentity.LeafName(right)
}

func rootPinDeclaredOutput(source semanticview.Source, eventType string) bool {
	if source == nil {
		return false
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false
	}
	for _, pin := range source.FlowOutputEventPins("") {
		output := eventidentity.Normalize(pin.EventType())
		if output == "" {
			continue
		}
		if output == eventType {
			return true
		}
		resolved := eventidentity.Normalize(source.ResolveFlowEventReference("", output))
		if resolved != "" && resolved == eventType {
			return true
		}
	}
	return false
}

func Resolve(input ResolutionInput, evt events.Event) Resolution {
	resolution := ResolveEnvelope(input, evt.NormalizedEnvelope())
	resolved, err := events.ResolveEnvelope(evt, resolution.Envelope)
	if err != nil {
		resolution.Failure = FailureTargetRequiredMissing
		return resolution
	}
	resolution.Event = resolved
	return resolution
}

func ResolveEnvelope(input ResolutionInput, envelope events.EventEnvelope) Resolution {
	input.FlowID = strings.TrimSpace(input.FlowID)
	input.EventType = strings.TrimSpace(input.EventType)
	input.SourceRoute = input.SourceRoute.Normalized()
	if input.SourceRoute.Empty() {
		input.SourceRoute = routeFromEvent(input.Inbound)
	}
	if !input.SourceRoute.Empty() {
		envelope = events.EnvelopeForSourceRoute(envelope, input.SourceRoute)
	}
	if !PinDeclaredOutput(input.Source, input.FlowID, input.EventType) {
		return Resolution{Envelope: envelope.Normalized()}
	}
	if OutputHarnessSink(input.Source, input.FlowID, input.EventType) {
		return Resolution{Envelope: envelope.Normalized()}
	}
	connectPlans := compositionConnectRoutePlansFromOutputEvent(input.Source, input.FlowID, input.EventType)
	if len(connectPlans) > 0 {
		if connectPlansContainRootReceiver(connectPlans) {
			parentRoute := input.ParentRoute.Normalized()
			if parentRoute.EntityID == "" {
				return Resolution{Envelope: envelope.Normalized(), Failure: FailureParentRouteIncomplete}
			}
			return Resolution{
				Envelope: events.EnvelopeForTargetRoute(envelope, parentRoute),
				Target:   parentRoute,
			}
		}
		return Resolution{Envelope: envelope.Normalized()}
	}
	parentRoute := input.ParentRoute.Normalized()
	if !parentRoute.Empty() {
		if input.AllowEntityOnlyParentRoute && parentRoute.FlowID == "" && parentRoute.FlowInstance == "" && parentRoute.EntityID != "" {
			return Resolution{Envelope: events.EnvelopeForTargetRoute(envelope, parentRoute), Target: parentRoute}
		}
		if parentRoute.FlowID == "" || parentRoute.FlowInstance == "" || parentRoute.EntityID == "" {
			return Resolution{Envelope: envelope.Normalized(), Failure: FailureParentRouteIncomplete}
		}
		return Resolution{Envelope: events.EnvelopeForTargetRoute(envelope, parentRoute), Target: parentRoute}
	}
	if OutputHasExternalConsumer(input.Source, input.FlowID, input.EventType) {
		return Resolution{Envelope: envelope.Normalized()}
	}
	return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetRequiredMissing}
}

func connectPlansContainRootReceiver(plans []ConnectRoutePlan) bool {
	for _, plan := range plans {
		if plan.Receiver.Root {
			return true
		}
	}
	return false
}

func OutputHarnessSink(source semanticview.Source, flowID, eventType string) bool {
	for _, pin := range outputPinsForEvent(source, flowID, eventType) {
		if pin.Sink == runtimecontracts.FlowOutputSinkHarness {
			return true
		}
	}
	return false
}

func OutputHasExternalConsumer(source semanticview.Source, flowID, eventType string) bool {
	if source == nil {
		return false
	}
	entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventType)
	return ok && len(entry.SwarmConsumer()) > 0
}

func routeFromEvent(evt events.Event) events.RouteIdentity {
	source := evt.SourceRoute()
	if !source.Empty() {
		return source
	}
	target := evt.TargetRoute()
	if !target.Empty() {
		return target
	}
	return events.RouteIdentity{
		FlowInstance: evt.FlowInstance(),
		EntityID:     evt.EntityID(),
	}.Normalized()
}

func descriptorRoute(source semanticview.Source, flowID string, descriptor Descriptor) events.RouteIdentity {
	flowInstance := strings.Trim(strings.TrimSpace(descriptor.FlowInstance), "/")
	entityID := strings.TrimSpace(descriptor.EntityID)
	if flowInstance == "" && entityID == "" {
		return events.RouteIdentity{}
	}
	if entityID == "" && flowInstance != "" {
		entityID = runtimeflowidentity.EntityID(flowInstance)
	}
	return events.RouteIdentity{
		FlowID:       strings.TrimSpace(flowIDForInstance(source, flowID, flowInstance)),
		FlowInstance: flowInstance,
		EntityID:     entityID,
	}.Normalized()
}

func flowIDForInstance(source semanticview.Source, fallbackFlowID, flowInstance string) string {
	fallbackFlowID = strings.TrimSpace(fallbackFlowID)
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if source == nil || flowInstance == "" {
		return fallbackFlowID
	}
	for _, scope := range source.FlowScopes() {
		scopePath := strings.Trim(strings.TrimSpace(scope.Path), "/")
		if scopePath != "" && (flowInstance == scopePath || strings.HasPrefix(flowInstance, scopePath+"/")) {
			return strings.TrimSpace(scope.ID)
		}
	}
	return fallbackFlowID
}

func routeMatchesFlow(source semanticview.Source, flowID, flowInstance string) bool {
	if source == nil {
		return true
	}
	scope := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/")
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	return scope == "" || flowInstance == scope || strings.HasPrefix(flowInstance, scope+"/")
}

func uniqueRoutes(in []events.RouteIdentity) []events.RouteIdentity {
	out := make([]events.RouteIdentity, 0, len(in))
	seen := map[string]struct{}{}
	for _, route := range in {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		key := route.FlowID + "\x00" + route.FlowInstance + "\x00" + route.EntityID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].FlowInstance == out[j].FlowInstance {
			return out[i].EntityID < out[j].EntityID
		}
		return out[i].FlowInstance < out[j].FlowInstance
	})
	return out
}

func FailureError(failure TargetFailure) error {
	if failure == "" {
		return nil
	}
	return fmt.Errorf("pin routing target resolution failed: %s", string(failure))
}
