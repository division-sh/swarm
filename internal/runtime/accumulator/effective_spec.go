package accumulator

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func EffectiveSpecForHandler(source semanticview.Source, node runtimeidentity.ExecutableNode, handlerEvent string, spec *runtimecontracts.AccumulateSpec) (*runtimecontracts.AccumulateSpec, error) {
	if spec == nil {
		return nil, nil
	}
	pin, ok, err := FanInInputPinForHandler(source, node, handlerEvent)
	if err != nil {
		return nil, err
	}
	if !ok {
		return spec, nil
	}
	if aggregation := strings.ToLower(strings.TrimSpace(pin.Resolution().Aggregation)); aggregation != "stream" {
		return nil, fmt.Errorf("receiver handler %s.%s declares accumulate but fan-in input pin %s.%s uses aggregation %q; accumulate accepts only aggregation: stream and finite barriers must use handler.join", node.NodeID(), strings.TrimSpace(handlerEvent), node.FlowPath(), pin.EventType(), aggregation)
	}
	handlerEvent = strings.TrimSpace(handlerEvent)
	if handlerEvent == "" {
		handlerEvent = strings.TrimSpace(pin.EventType())
	}
	if dedup := strings.TrimSpace(spec.DedupBy); dedup != "" {
		return nil, fmt.Errorf("receiver handler %s.%s accumulate.dedup_by %q must not redeclare fan-in dedup_by; declare it once on the receiver input pin resolution", node.NodeID(), handlerEvent, dedup)
	}
	if window := strings.TrimSpace(spec.Window); window != "" {
		return nil, fmt.Errorf("receiver handler %s.%s accumulate.window %q must not redeclare fan-in window; declare it once on the receiver input pin resolution", node.NodeID(), handlerEvent, window)
	}
	resolution := pin.Resolution()
	window := strings.TrimSpace(resolution.Window)
	if window == "" {
		return nil, fmt.Errorf("resolution mode fan-in stream requires window for receiver input pin %s.%s", node.FlowPath(), pin.EventType())
	}
	dedupBy := normalizedStrings(resolution.DedupBy)
	if len(dedupBy) == 0 {
		return nil, fmt.Errorf("resolution mode fan-in stream requires dedup_by for receiver input pin %s.%s; sender identity is not an implicit default", node.FlowPath(), pin.EventType())
	}
	if len(dedupBy) != 1 {
		return nil, fmt.Errorf("resolution mode fan-in stream supports exactly one dedup_by field in this slice for receiver input pin %s.%s, got %v", node.FlowPath(), pin.EventType(), dedupBy)
	}
	effective := *spec
	effective.Window = window
	effective.WindowPath = paths.Parse(window)
	effective.DedupBy = dedupBy[0]
	effective.DedupPath = paths.Parse(dedupBy[0])
	return &effective, nil
}

func FanInInputPinForHandler(source semanticview.Source, node runtimeidentity.ExecutableNode, handlerEvent string) (runtimecontracts.CompiledFlowInputPin, bool, error) {
	if source == nil {
		return runtimecontracts.CompiledFlowInputPin{}, false, nil
	}
	handlerEvent = strings.TrimSpace(handlerEvent)
	if !node.Valid() || handlerEvent == "" {
		return runtimecontracts.CompiledFlowInputPin{}, false, nil
	}
	result := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveFanInInputForHandler(node, handlerEvent)
	if result.Status == semanticview.EndpointAssociationNotFound {
		return runtimecontracts.CompiledFlowInputPin{}, false, nil
	}
	if result.Status == semanticview.EndpointAssociationAmbiguous {
		matchedPins := make([]string, 0, len(result.Candidates))
		for _, candidate := range result.Candidates {
			matchedPins = append(matchedPins, strings.TrimSpace(candidate.PinName))
		}
		return runtimecontracts.CompiledFlowInputPin{}, false, fmt.Errorf("receiver handler %s.%s matches multiple fan-in input pins %v; fan-in accumulator semantics require exactly one receiver input pin owner", node.NodeID(), handlerEvent, matchedPins)
	}
	endpoint, ok := result.Endpoint()
	if !ok {
		return runtimecontracts.CompiledFlowInputPin{}, false, result.Err()
	}
	pin, ok := source.FlowInputEventPin(node.FlowPath(), endpoint.PinName)
	if !ok {
		return runtimecontracts.CompiledFlowInputPin{}, false, fmt.Errorf("canonical fan-in endpoint %s references missing receiver input pin %s.%s", endpoint.ID, node.FlowPath(), endpoint.PinName)
	}
	return pin, true, nil
}

func normalizedStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
