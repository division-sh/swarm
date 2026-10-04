package pinrouting

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type TargetFailure uint8

const (
	FailureTargetRequiredMissing TargetFailure = iota + 1
	FailureTargetNotSubscribed
	FailureTargetUnreachableTerminated
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

type ResolutionInput struct {
	Source        semanticview.Source
	FlowID        string
	EventType     string
	RoutingSource events.RoutingSource
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
	OutputConsumerSameFlow
	OutputConsumerConnect
	OutputConsumerRootExport
)

type OutputConsumerClassification struct {
	classes  map[OutputConsumerClass]struct{}
	connects []ConnectRoutePlan
}

func (c OutputConsumerClassification) Has(class OutputConsumerClass) bool {
	_, ok := c.classes[class]
	return ok
}

func (c OutputConsumerClassification) HasRuntimeConsumer() bool {
	return c.Has(OutputConsumerSameFlow) || c.Has(OutputConsumerConnect)
}

func (c OutputConsumerClassification) DeliberateNoSubscriber() bool {
	return c.Has(OutputConsumerRootExport) && !c.HasRuntimeConsumer()
}

func ClassifyOutputConsumer(source semanticview.Source, flowID, eventType string) OutputConsumerClassification {
	return classifyOutputConsumer(source, flowID, eventType, events.NoRoutingSource())
}

func ClassifyRoutingSourceOutputConsumer(source semanticview.Source, eventType string, routingSource events.RoutingSource) OutputConsumerClassification {
	return classifyOutputConsumer(source, routingSource.Route().FlowID, eventType, routingSource)
}

// OutputConsumerResolver belongs to one preparation operation against one
// admitted source. Only compiled declaration evidence is shared, never the
// event-dependent classification or any selected-store observation.
type OutputConsumerResolver struct {
	source semanticview.Source
	once   sync.Once
	graph  CompiledConnectGraph
	census semanticview.AuthoredEventEndpointCensus
}

func NewOutputConsumerResolver(source semanticview.Source) *OutputConsumerResolver {
	return &OutputConsumerResolver{source: source}
}

func (r *OutputConsumerResolver) Classify(eventType string, routingSource events.RoutingSource) OutputConsumerClassification {
	return r.classify(routingSource.Route().FlowID, eventType, routingSource)
}

func classifyOutputConsumer(source semanticview.Source, flowID, eventType string, routingSource events.RoutingSource) OutputConsumerClassification {
	return NewOutputConsumerResolver(source).classify(flowID, eventType, routingSource)
}

func (r *OutputConsumerResolver) classify(flowID, eventType string, routingSource events.RoutingSource) OutputConsumerClassification {
	source := r.source
	classification := OutputConsumerClassification{classes: map[OutputConsumerClass]struct{}{}}
	if source == nil {
		return classification
	}
	if routingSource.Kind() == events.RoutingSourceDeploymentFeed &&
		AdmitDeploymentFeedDeclaration(source, events.EventType(eventType), routingSource) != nil {
		return classification
	}
	outputPins := outputPinsForEvent(source, flowID, eventType)
	r.once.Do(func() { r.graph, r.census = compileConnectGraphWithCensus(source) })
	graph, census := r.graph, r.census
	if routingSource.Kind() == events.RoutingSourceConcreteTemplateInstance {
		if _, err := PublicationDeclarationForSourceEvent(events.EventType(eventType), routingSource); err != nil {
			return classification
		}
	}
	for _, pin := range outputPins {
		if routingSource.Empty() {
			classification.connects = append(classification.connects, graph.PlansFromOutputPin(flowID, pin)...)
		}
	}
	if !routingSource.Empty() {
		if sourceEvent, err := AdmitSourceEvent(events.EventType(eventType), routingSource); err == nil {
			classification.connects = append(classification.connects, graph.MatchingSourceEvent(sourceEvent)...)
		}
	}
	consumerEvent := eventType
	if routingSource.Kind() == events.RoutingSourceConcreteTemplateInstance {
		consumerEvent = semanticview.ResolveFlowEventProof(source, flowID, eventType).Local
	}
	for _, endpoint := range census.MatchingConsumers(flowID, consumerEvent) {
		switch endpoint.Kind {
		case semanticview.EventEndpointNodeHandler, semanticview.EventEndpointAgent, semanticview.EventEndpointTimer:
			classification.classes[OutputConsumerSameFlow] = struct{}{}
		}
	}
	if len(classification.connects) > 0 {
		classification.classes[OutputConsumerConnect] = struct{}{}
	}
	if flowID == "" || flowID == "." {
		if _, exported := semanticview.SelectedRootOutputPin(source, eventType); exported {
			classification.classes[OutputConsumerRootExport] = struct{}{}
		}
	}
	return classification
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
	if consumer.Has(OutputConsumerSameFlow) || consumer.Has(OutputConsumerRootExport) {
		return Resolution{Envelope: envelope.Normalized()}
	}
	if len(consumer.connects) > 0 {
		return Resolution{Envelope: envelope.Normalized()}
	}
	return Resolution{Envelope: envelope.Normalized(), Failure: FailureTargetRequiredMissing}
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
