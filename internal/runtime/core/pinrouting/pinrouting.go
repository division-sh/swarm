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
	FailureTargetRequiredMissing          TargetFailure = "target_required_missing"
	FailureTargetInvalidSyntax            TargetFailure = "target_invalid_syntax"
	FailureTargetMalformed                TargetFailure = FailureTargetInvalidSyntax
	FailureTargetUnreachableNoSub         TargetFailure = "target_unreachable_no_subscriber"
	FailureTargetNotSubscribed            TargetFailure = "target_not_subscribed"
	FailureTargetUnreachableTerminated    TargetFailure = "target_unreachable_terminated"
	FailureParentRouteIncomplete          TargetFailure = "parent_route_incomplete"
	FailureTargetAmbiguous                TargetFailure = "target_ambiguous"
	FailureTargetUnknownFlow              TargetFailure = "target_unknown_flow"
	FailureTargetSenderNoInboundRuntime   TargetFailure = "target_sender_no_inbound_runtime"
	FailureTargetSenderEmptySourceRuntime TargetFailure = "target_sender_empty_source_runtime"
	FailureProducerTargetCommonPath       TargetFailure = "producer_target_common_path_forbidden"
	FailureProducerBroadcastCommonPath    TargetFailure = "producer_broadcast_common_path_forbidden"
	FailureReplyAlreadyTerminal           TargetFailure = "platform.reply_already_terminal"
	FailureStaleArrival                   TargetFailure = "platform.stale_arrival"
)

type AuthoredTarget struct {
	Spec      runtimecontracts.EmitTargetSpec
	Broadcast bool
}

type Descriptor struct {
	ID            string
	EntityID      string
	FlowInstance  string
	AddressFields map[string]string
}

type ResolutionInput struct {
	Source       semanticview.Source
	FlowID       string
	EventType    string
	Emit         runtimecontracts.EmitSpec
	SourceRoute  events.RouteIdentity
	Inbound      events.Event
	ParentRoute  events.RouteIdentity
	MatchValues  map[string]string
	Descriptors  []Descriptor
	AllowMissing bool
	// Static child flows have no persisted ParentRoute row; they may route back to
	// the current delivery entity, but template/dynamic ParentRoute metadata must
	// remain complete and fail closed when partial.
	AllowEntityOnlyParentRoute bool
}

type Resolution struct {
	Event     events.Event
	Envelope  events.EventEnvelope
	Target    events.RouteIdentity
	Broadcast bool
	Failure   TargetFailure
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

func ProducerRouteCommonPathFailure(source semanticview.Source, flowID, eventType string, spec runtimecontracts.EmitSpec) TargetFailure {
	flowID = strings.TrimSpace(flowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" || !PinDeclaredOutput(source, flowID, eventType) {
		return ""
	}
	if flowID != "" {
		if _, ok := source.FlowScopeByID(flowID); !ok {
			return ""
		}
	} else if !compositionConnectsFromOutputEvent(source, flowID, eventType) {
		return ""
	}
	if spec.Broadcast {
		if compositionConnectsFromOutputEvent(source, flowID, eventType) ||
			(flowID != "" && producerRouteKnownReceiverConsumesOutput(source, flowID, eventType, "")) {
			return FailureProducerBroadcastCommonPath
		}
		return ""
	}
	target := spec.Target.Normalized()
	if target.Empty() {
		return ""
	}
	switch target.Kind {
	case runtimecontracts.EmitTargetKindSender, runtimecontracts.EmitTargetKindInstanceID:
		return ""
	case runtimecontracts.EmitTargetKindFlowMatch:
		if compositionConnectsFromOutputEventToFlow(source, flowID, eventType, target.Flow) ||
			(flowID != "" && producerRouteKnownReceiverConsumesOutput(source, flowID, eventType, target.Flow)) {
			return FailureProducerTargetCommonPath
		}
	}
	return ""
}

func compositionConnectsFromOutputEvent(source semanticview.Source, flowID, eventType string) bool {
	if source == nil {
		return false
	}
	for _, pin := range outputPinsForEvent(source, flowID, eventType) {
		if len(source.CompositionConnectsFrom(flowID, pin.PinName())) > 0 {
			return true
		}
	}
	return false
}

func compositionConnectsFromOutputEventToFlow(source semanticview.Source, flowID, eventType, targetFlowID string) bool {
	if source == nil {
		return false
	}
	targetFlowID = strings.TrimSpace(targetFlowID)
	if targetFlowID == "" {
		return false
	}
	if _, ok := source.FlowScopeByID(targetFlowID); !ok {
		return false
	}
	for _, pin := range outputPinsForEvent(source, flowID, eventType) {
		for _, connect := range source.CompositionConnectsFrom(flowID, pin.PinName()) {
			to, err := connect.ToRef()
			if err == nil && strings.TrimSpace(to.FlowID) == targetFlowID {
				return true
			}
		}
	}
	return false
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

func producerRouteKnownReceiverConsumesOutput(source semanticview.Source, producerFlowID, eventType, targetFlowID string) bool {
	if source == nil {
		return false
	}
	producerFlowID = strings.TrimSpace(producerFlowID)
	targetFlowID = strings.TrimSpace(targetFlowID)
	if targetFlowID != "" {
		if _, ok := source.FlowScopeByID(targetFlowID); !ok {
			return false
		}
		return targetFlowID != producerFlowID && flowInputConsumesOutput(source, producerFlowID, eventType, targetFlowID)
	}
	for _, scope := range source.FlowScopes() {
		receiverFlowID := strings.TrimSpace(scope.ID)
		if receiverFlowID == "" || receiverFlowID == producerFlowID {
			continue
		}
		if flowInputConsumesOutput(source, producerFlowID, eventType, receiverFlowID) {
			return true
		}
	}
	return false
}

func flowInputConsumesOutput(source semanticview.Source, producerFlowID, eventType, receiverFlowID string) bool {
	if source == nil {
		return false
	}
	for _, pin := range source.FlowInputEventPins(receiverFlowID) {
		if eventReferencesOverlap(source, producerFlowID, []string{eventType}, receiverFlowID, []string{pin.PinName(), pin.EventType()}) {
			return true
		}
	}
	return false
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
	resolution.Event = events.Project(evt, events.ProjectEnvelope(resolution.Envelope))
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
	if failure := ProducerRouteCommonPathFailure(input.Source, input.FlowID, input.EventType, input.Emit); failure != "" {
		return Resolution{Envelope: envelope.Normalized(), Failure: failure}
	}
	if compositionConnectsFromOutputEvent(input.Source, input.FlowID, input.EventType) {
		if failure := ValidateTargetSpec(input.Source, input.FlowID, input.Emit, true); failure != "" {
			return Resolution{Envelope: envelope.Normalized(), Failure: failure}
		}
		return Resolution{Envelope: envelope.Normalized()}
	}
	if input.Emit.Broadcast {
		return Resolution{Envelope: events.EnvelopeForBroadcast(envelope), Broadcast: true}
	}
	target := input.Emit.Target.Normalized()
	if target.Empty() {
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
		return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetRequiredMissing}
	}
	switch target.Kind {
	case runtimecontracts.EmitTargetKindSender:
		route := input.Inbound.SourceRoute()
		if route.Empty() {
			route = routeFromEvent(input.Inbound)
		}
		if route.Empty() {
			if input.Inbound.ID() == "" && strings.TrimSpace(string(input.Inbound.Type())) == "" {
				return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetSenderNoInboundRuntime}
			}
			return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetSenderEmptySourceRuntime}
		}
		return Resolution{Envelope: events.EnvelopeForTargetRoute(envelope, route), Target: route}
	case runtimecontracts.EmitTargetKindInstanceID:
		instanceID := strings.TrimSpace(target.InstanceID)
		if instanceID == "" {
			return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetMalformed}
		}
		flowID := strings.TrimSpace(target.Flow)
		if flowID == "" {
			flowID = input.FlowID
		}
		route := routeForInstance(input.Source, flowID, instanceID)
		return Resolution{Envelope: events.EnvelopeForTargetRoute(envelope, route), Target: route}
	case runtimecontracts.EmitTargetKindFlowMatch:
		route, failure := resolveFlowMatch(input, target)
		if failure != "" {
			return Resolution{Envelope: envelope.Normalized(), Failure: failure}
		}
		return Resolution{Envelope: events.EnvelopeForTargetRoute(envelope, route), Target: route}
	default:
		return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetMalformed}
	}
}

func TargetMechanismPresent(spec runtimecontracts.EmitSpec, structuralParentRouteEligible bool) bool {
	return spec.Broadcast || spec.HasTarget() || structuralParentRouteEligible
}

func ValidateTargetSpec(source semanticview.Source, flowID string, spec runtimecontracts.EmitSpec, structuralParentRouteEligible bool) TargetFailure {
	if !TargetMechanismPresent(spec, structuralParentRouteEligible) {
		return FailureTargetRequiredMissing
	}
	if spec.Broadcast {
		if spec.HasTarget() {
			return FailureTargetMalformed
		}
		return ""
	}
	target := spec.Target.Normalized()
	if target.Empty() {
		return ""
	}
	switch target.Kind {
	case runtimecontracts.EmitTargetKindSender:
		return ""
	case runtimecontracts.EmitTargetKindInstanceID:
		if strings.TrimSpace(target.InstanceID) == "" {
			return FailureTargetMalformed
		}
		return ""
	case runtimecontracts.EmitTargetKindFlowMatch:
		if strings.TrimSpace(target.Flow) == "" || len(target.Match) == 0 {
			return FailureTargetMalformed
		}
		if source != nil {
			if _, ok := source.FlowScopeByID(strings.TrimSpace(target.Flow)); !ok {
				return FailureTargetUnknownFlow
			}
		}
		return ""
	default:
		return FailureTargetMalformed
	}
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

func routeForInstance(source semanticview.Source, flowID, instanceID string) events.RouteIdentity {
	instanceID = strings.TrimSpace(instanceID)
	flowID = strings.TrimSpace(flowID)
	flowInstance := strings.Trim(strings.TrimSpace(runtimeflowidentity.InstancePath(source, flowID, instanceID)), "/")
	return events.RouteIdentity{
		FlowID:       flowID,
		FlowInstance: flowInstance,
		EntityID:     runtimeflowidentity.EntityID(flowInstance),
	}.Normalized()
}

func resolveFlowMatch(input ResolutionInput, target runtimecontracts.EmitTargetSpec) (events.RouteIdentity, TargetFailure) {
	flowID := strings.TrimSpace(target.Flow)
	if flowID == "" {
		return events.RouteIdentity{}, FailureTargetMalformed
	}
	if input.Source != nil {
		if _, ok := input.Source.FlowScopeByID(flowID); !ok {
			return events.RouteIdentity{}, FailureTargetUnknownFlow
		}
	}
	matches := make([]events.RouteIdentity, 0, len(input.Descriptors))
	for _, descriptor := range input.Descriptors {
		route := descriptorRoute(input.Source, flowID, descriptor)
		if route.Empty() {
			continue
		}
		if flowID != "" && route.FlowID != "" && route.FlowID != flowID {
			continue
		}
		if flowID != "" && route.FlowID == "" && !routeMatchesFlow(input.Source, flowID, route.FlowInstance) {
			continue
		}
		if !descriptorMatches(target.Match, input.MatchValues, descriptor, route) {
			continue
		}
		matches = append(matches, route)
	}
	if len(matches) == 0 {
		return events.RouteIdentity{}, FailureTargetUnreachableNoSub
	}
	matches = uniqueRoutes(matches)
	if len(matches) > 1 {
		return events.RouteIdentity{}, FailureTargetAmbiguous
	}
	return matches[0], ""
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
		if scopePath == "" {
			continue
		}
		if flowInstance == scopePath || strings.HasPrefix(flowInstance, scopePath+"/") {
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

func descriptorMatches(match map[string]runtimecontracts.ExpressionValue, values map[string]string, descriptor Descriptor, route events.RouteIdentity) bool {
	if len(match) == 0 {
		return false
	}
	for key := range match {
		key = strings.TrimSpace(key)
		want := strings.TrimSpace(values[key])
		if want == "" {
			return false
		}
		switch key {
		case "entity_id":
			if strings.TrimSpace(descriptor.EntityID) != want && route.EntityID != want {
				return false
			}
		case "flow_instance":
			if strings.Trim(strings.TrimSpace(descriptor.FlowInstance), "/") != strings.Trim(want, "/") && route.FlowInstance != strings.Trim(want, "/") {
				return false
			}
		case "instance_id":
			if runtimeflowidentity.LogicalInstanceID(route.FlowInstance) != want {
				return false
			}
		default:
			return false
		}
	}
	return true
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
