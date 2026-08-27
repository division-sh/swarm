package pinrouting

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type TargetFailure uint8

const (
	FailureTargetRequiredMissing TargetFailure = iota + 1
	FailureTargetNotSubscribed
	FailureTargetUnreachableTerminated
	FailureParentRouteIncomplete
	FailureReplyAlreadyTerminal
	FailureStaleArrival
)

const targetFailureConnectOffset TargetFailure = 32

func TargetFailureFromConnect(failure ConnectRoutePlanFailure) TargetFailure {
	if failure.Empty() {
		return 0
	}
	return targetFailureConnectOffset + TargetFailure(failure)
}

func (f TargetFailure) Empty() bool { return f == 0 }

func (f TargetFailure) Code() string {
	switch f {
	case FailureTargetRequiredMissing:
		return "target_required_missing"
	case FailureTargetNotSubscribed:
		return "target_not_subscribed"
	case FailureTargetUnreachableTerminated:
		return "target_unreachable_terminated"
	case FailureParentRouteIncomplete:
		return "parent_route_incomplete"
	case FailureReplyAlreadyTerminal:
		return "platform.reply_already_terminal"
	case FailureStaleArrival:
		return "platform.stale_arrival"
	default:
		if f > targetFailureConnectOffset {
			return ConnectRoutePlanFailure(f - targetFailureConnectOffset).Code()
		}
		return ""
	}
}

func ParseTargetFailure(code string) (TargetFailure, error) {
	switch strings.TrimSpace(code) {
	case "":
		return 0, nil
	case "target_required_missing":
		return FailureTargetRequiredMissing, nil
	case "target_not_subscribed":
		return FailureTargetNotSubscribed, nil
	case "target_unreachable_terminated":
		return FailureTargetUnreachableTerminated, nil
	case "parent_route_incomplete":
		return FailureParentRouteIncomplete, nil
	case "platform.reply_already_terminal":
		return FailureReplyAlreadyTerminal, nil
	case "platform.stale_arrival":
		return FailureStaleArrival, nil
	default:
		for failure := ConnectFailureSourceMissing; failure <= ConnectFailureLifecycleUnavailable; failure++ {
			if failure.Code() == strings.TrimSpace(code) {
				return TargetFailureFromConnect(failure), nil
			}
		}
		return 0, fmt.Errorf("target failure code %q is invalid", code)
	}
}

type Descriptor struct {
	ID            string
	EntityID      string
	FlowInstance  string
	AddressFields map[string]string
}

type targetEvidenceState uint8

const (
	targetEvidenceAbsent targetEvidenceState = iota
	targetEvidenceExact
	targetEvidenceInvalid
)

// PersistedStructuralParent is the classified parent route stored when a
// child/template instance is created. Its zero value is explicit absence.
type PersistedStructuralParent struct {
	state targetEvidenceState
	route events.RouteIdentity
}

func ClassifyPersistedStructuralParent(route events.RouteIdentity) PersistedStructuralParent {
	route = route.Normalized()
	if route.Empty() {
		return PersistedStructuralParent{}
	}
	if route.FlowID == "" || route.FlowInstance == "" || route.EntityID == "" {
		return PersistedStructuralParent{state: targetEvidenceInvalid}
	}
	return PersistedStructuralParent{state: targetEvidenceExact, route: route}
}

// CurrentDeliveryTarget is exact target evidence obtained from an admitted
// DeliveryRoute. It cannot be constructed from an event or source identity.
type CurrentDeliveryTarget struct {
	state targetEvidenceState
	route events.RouteIdentity
}

func ClassifyCurrentDeliveryTarget(route events.DeliveryRoute, present bool) CurrentDeliveryTarget {
	if !present {
		return CurrentDeliveryTarget{}
	}
	route = route.Normalized()
	if _, err := route.Identity(); err != nil || route.Target.EntitylessReceiver() {
		return CurrentDeliveryTarget{state: targetEvidenceInvalid}
	}
	target := route.Target.Route().Normalized()
	if target.FlowID == "" || target.FlowInstance == "" || target.EntityID == "" {
		return CurrentDeliveryTarget{state: targetEvidenceInvalid}
	}
	return CurrentDeliveryTarget{state: targetEvidenceExact, route: target}
}

type ResolutionInput struct {
	Source               semanticview.Source
	FlowID               string
	EventType            string
	RoutingSource        events.RoutingSource
	StructuralParent     PersistedStructuralParent
	CurrentDeliveryOwner CurrentDeliveryTarget
}

type Resolution struct {
	Event    events.Event
	Envelope events.EventEnvelope
	Target   events.RouteIdentity
	Failure  TargetFailure
}

type OutputConsumerClass uint8

const (
	OutputConsumerNone OutputConsumerClass = iota
	OutputConsumerHarness
	OutputConsumerSameFlow
	OutputConsumerConnect
	OutputConsumerStructuralParent
	OutputConsumerExternal
)

type OutputConsumerClassification struct {
	classes     map[OutputConsumerClass]struct{}
	invalidSink bool
	connects    []ConnectRoutePlan
}

func (c OutputConsumerClassification) Has(class OutputConsumerClass) bool {
	_, ok := c.classes[class]
	return ok
}

func (c OutputConsumerClassification) InvalidSink() bool {
	return c.invalidSink
}

func (c OutputConsumerClassification) HasRuntimeConsumer() bool {
	return c.Has(OutputConsumerSameFlow) || c.Has(OutputConsumerConnect) || c.Has(OutputConsumerStructuralParent) || c.Has(OutputConsumerExternal)
}

func (c OutputConsumerClassification) DeliberateNoSubscriber() bool {
	return (c.Has(OutputConsumerHarness) || c.Has(OutputConsumerExternal)) &&
		!c.Has(OutputConsumerSameFlow) && !c.Has(OutputConsumerConnect) && !c.Has(OutputConsumerStructuralParent)
}

func ClassifyOutputConsumer(source semanticview.Source, flowID, eventType string) OutputConsumerClassification {
	return classifyOutputConsumer(source, flowID, eventType, events.NoRoutingSource())
}

func ClassifyRoutingSourceOutputConsumer(source semanticview.Source, eventType string, routingSource events.RoutingSource) OutputConsumerClassification {
	return classifyOutputConsumer(source, routingSource.Route().FlowID, eventType, routingSource)
}

func classifyOutputConsumer(source semanticview.Source, flowID, eventType string, routingSource events.RoutingSource) OutputConsumerClassification {
	classification := OutputConsumerClassification{classes: map[OutputConsumerClass]struct{}{}}
	if source == nil {
		return classification
	}
	outputPins := outputPinsForEvent(source, flowID, eventType)
	graph := CompileConnectGraph(source)
	for _, pin := range outputPins {
		if !pin.Sink().Valid() {
			classification.invalidSink = true
			continue
		}
		if pin.Sink() == runtimecontracts.FlowOutputSinkHarness {
			classification.classes[OutputConsumerHarness] = struct{}{}
		}
		if routingSource.Empty() {
			classification.connects = append(classification.connects, graph.PlansFromOutputPin(flowID, pin)...)
		}
	}
	if !routingSource.Empty() {
		if sourceEvent, err := AdmitSourceEvent(events.EventType(eventType), routingSource); err == nil {
			classification.connects = append(classification.connects, graph.MatchingSourceEvent(sourceEvent)...)
		}
	}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).MatchingConsumers(flowID, eventType) {
		if endpoint.Kind != semanticview.EventEndpointExternal {
			classification.classes[OutputConsumerSameFlow] = struct{}{}
			break
		}
	}
	if len(classification.connects) > 0 {
		classification.classes[OutputConsumerConnect] = struct{}{}
	}
	if structuralParentRouteEligible(source, flowID) {
		classification.classes[OutputConsumerStructuralParent] = struct{}{}
	}
	entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventType)
	if ok && entry.AcceptedConsumerBoundary() == runtimecontracts.EventConsumerBoundaryExternal {
		classification.classes[OutputConsumerExternal] = struct{}{}
	}
	return classification
}

func structuralParentRouteEligible(source semanticview.Source, flowID string) bool {
	if source == nil {
		return false
	}
	if schema, ok := source.FlowSchemaByID(flowID); ok && strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
		return true
	}
	path := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/")
	return strings.Contains(path, "/")
}

func PinDeclaredOutput(source semanticview.Source, flowID, eventType string) bool {
	if source == nil {
		return false
	}
	proof := semanticview.ResolveFlowEventProof(source, flowID, eventType)
	for _, candidate := range []string{proof.Authored, proof.Local, proof.Canonical} {
		if strings.TrimSpace(candidate) != "" && source.FlowHasOutputEvent(flowID, candidate) {
			return true
		}
	}
	if len(outputPinsForEvent(source, flowID, eventType)) > 0 {
		return true
	}
	eventKey := proof.EventKey()
	for _, output := range source.FlowOutputEvents(flowID) {
		if semanticview.ResolveFlowEventProof(source, flowID, output).EventKey() == eventKey {
			return true
		}
	}
	return false
}

func outputPinsForEvent(source semanticview.Source, flowID, eventType string) []runtimecontracts.CompiledFlowOutputPin {
	if source == nil {
		return nil
	}
	eventKey := semanticview.ResolveFlowEventProof(source, flowID, eventType).EventKey()
	if eventKey == "" {
		return nil
	}
	out := []runtimecontracts.CompiledFlowOutputPin{}
	for _, pin := range source.FlowOutputEventPins(flowID) {
		if semanticview.ResolveFlowEventProof(source, flowID, pin.EventType()).EventKey() == eventKey {
			out = append(out, pin)
		}
	}
	return out
}

func Resolve(input ResolutionInput, evt events.Event) Resolution {
	if input.RoutingSource.Empty() {
		input.RoutingSource = evt.RoutingSource()
	}
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
	sourceRoute := input.RoutingSource.Route().Normalized()
	if !sourceRoute.Empty() {
		envelope = events.EnvelopeForSourceRoute(envelope, sourceRoute)
	}
	if !PinDeclaredOutput(input.Source, input.FlowID, input.EventType) {
		return Resolution{Envelope: envelope.Normalized()}
	}
	consumer := classifyOutputConsumer(input.Source, input.FlowID, input.EventType, input.RoutingSource)
	if consumer.InvalidSink() || (consumer.Has(OutputConsumerHarness) && consumer.HasRuntimeConsumer()) {
		return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetRequiredMissing}
	}
	if consumer.Has(OutputConsumerHarness) || consumer.Has(OutputConsumerSameFlow) || consumer.Has(OutputConsumerExternal) {
		return Resolution{Envelope: envelope.Normalized()}
	}
	if len(consumer.connects) > 0 {
		return Resolution{Envelope: envelope.Normalized()}
	}
	if input.StructuralParent.state == targetEvidenceInvalid {
		return Resolution{Envelope: envelope.Normalized(), Failure: FailureParentRouteIncomplete}
	}
	if input.StructuralParent.state == targetEvidenceExact {
		parentRoute := input.StructuralParent.route
		return Resolution{Envelope: events.EnvelopeForTargetRoute(envelope, parentRoute), Target: parentRoute}
	}
	if nestedStaticCurrentDeliveryTargetEligible(input.Source, input.FlowID) {
		if input.CurrentDeliveryOwner.state != targetEvidenceExact {
			return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetRequiredMissing}
		}
		target := input.CurrentDeliveryOwner.route
		return Resolution{Envelope: events.EnvelopeForTargetRoute(envelope, target), Target: target}
	}
	if OutputHasExternalConsumer(input.Source, input.FlowID, input.EventType) {
		return Resolution{Envelope: envelope.Normalized()}
	}
	return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetRequiredMissing}
}

func nestedStaticCurrentDeliveryTargetEligible(source semanticview.Source, flowID string) bool {
	scope, ok := semanticview.FlowScopeByID(source, strings.TrimSpace(flowID))
	if !ok || !strings.EqualFold(strings.TrimSpace(scope.Mode), string(runtimecontracts.FlowModeStatic)) {
		return false
	}
	return strings.Contains(strings.Trim(strings.TrimSpace(scope.Path), "/"), "/")
}

func OutputHarnessSink(source semanticview.Source, flowID, eventType string) bool {
	return ClassifyOutputConsumer(source, flowID, eventType).Has(OutputConsumerHarness)
}

func OutputHasExternalConsumer(source semanticview.Source, flowID, eventType string) bool {
	return ClassifyOutputConsumer(source, flowID, eventType).Has(OutputConsumerExternal)
}

func descriptorRoute(flowID string, descriptor Descriptor) events.RouteIdentity {
	flowInstance := strings.Trim(strings.TrimSpace(descriptor.FlowInstance), "/")
	entityID := strings.TrimSpace(descriptor.EntityID)
	if flowInstance == "" || entityID == "" {
		return events.RouteIdentity{}
	}
	return events.RouteIdentity{
		FlowID:       strings.TrimSpace(flowID),
		FlowInstance: flowInstance,
		EntityID:     entityID,
	}.Normalized()
}

func uniqueRoutes(in []events.RouteIdentity) []events.RouteIdentity {
	out := make([]events.RouteIdentity, 0, len(in))
	seen := map[events.RouteIdentity]struct{}{}
	for _, route := range in {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		if _, ok := seen[route]; ok {
			continue
		}
		seen[route] = struct{}{}
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
	if failure.Empty() {
		return nil
	}
	return fmt.Errorf("pin routing target resolution failed: %s", failure.Code())
}
