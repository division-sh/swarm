package accumulator

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventidentity "github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func EffectiveSpecForHandler(source semanticview.Source, flowID, nodeID, handlerEvent string, spec *runtimecontracts.AccumulateSpec) (*runtimecontracts.AccumulateSpec, error) {
	if spec == nil {
		return nil, nil
	}
	pin, ok, err := FanInInputPinForHandler(source, flowID, nodeID, handlerEvent)
	if err != nil {
		return nil, err
	}
	if !ok {
		return spec, nil
	}
	handlerEvent = strings.TrimSpace(handlerEvent)
	if handlerEvent == "" {
		handlerEvent = strings.TrimSpace(pin.EventType())
	}
	if dedup := strings.TrimSpace(spec.DedupBy); dedup != "" {
		return nil, fmt.Errorf("receiver handler %s.%s accumulate.dedup_by %q must not redeclare fan-in dedup_by; declare it once on the receiver input pin resolution", strings.TrimSpace(nodeID), handlerEvent, dedup)
	}
	if window := strings.TrimSpace(spec.Window); window != "" {
		return nil, fmt.Errorf("receiver handler %s.%s accumulate.window %q must not redeclare fan-in window; declare it once on the receiver input pin resolution", strings.TrimSpace(nodeID), handlerEvent, window)
	}
	resolution := pin.Resolution
	window := strings.TrimSpace(resolution.Window)
	if window == "" {
		return nil, fmt.Errorf("resolution mode fan-in stream requires window for receiver input pin %s.%s", strings.TrimSpace(flowID), pin.PinName())
	}
	dedupBy := normalizedStrings(resolution.DedupBy)
	if len(dedupBy) == 0 {
		return nil, fmt.Errorf("resolution mode fan-in stream requires dedup_by for receiver input pin %s.%s; sender identity is not an implicit default", strings.TrimSpace(flowID), pin.PinName())
	}
	if len(dedupBy) != 1 {
		return nil, fmt.Errorf("resolution mode fan-in stream supports exactly one dedup_by field in this slice for receiver input pin %s.%s, got %v", strings.TrimSpace(flowID), pin.PinName(), dedupBy)
	}
	effective := *spec
	effective.Window = window
	effective.WindowPath = paths.Parse(window)
	effective.DedupBy = dedupBy[0]
	effective.DedupPath = paths.Parse(dedupBy[0])
	return &effective, nil
}

func FanInInputPinForHandler(source semanticview.Source, flowID, nodeID, handlerEvent string) (runtimecontracts.FlowInputEventPin, bool, error) {
	if source == nil {
		return runtimecontracts.FlowInputEventPin{}, false, nil
	}
	flowID = strings.TrimSpace(flowID)
	handlerEvent = strings.TrimSpace(handlerEvent)
	if flowID == "" || handlerEvent == "" {
		return runtimecontracts.FlowInputEventPin{}, false, nil
	}
	var matched runtimecontracts.FlowInputEventPin
	var matchedPins []string
	for _, pin := range source.FlowInputEventPins(flowID) {
		if pin.Resolution.Mode != runtimecontracts.FlowInputResolutionModeFanIn {
			continue
		}
		if fanInInputPinMatchesHandlerEvent(source, flowID, pin, handlerEvent) {
			matched = pin
			matchedPins = append(matchedPins, pin.PinName())
		}
	}
	if len(matchedPins) > 1 {
		return runtimecontracts.FlowInputEventPin{}, false, fmt.Errorf("receiver handler %s.%s matches multiple fan-in input pins %v; fan-in accumulator semantics require exactly one receiver input pin owner", strings.TrimSpace(nodeID), handlerEvent, matchedPins)
	}
	if len(matchedPins) == 1 {
		return matched, true, nil
	}
	return runtimecontracts.FlowInputEventPin{}, false, nil
}

func fanInInputPinMatchesHandlerEvent(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin, handlerEvent string) bool {
	handlerEvent = strings.TrimSpace(handlerEvent)
	inputEvent := strings.TrimSpace(pin.EventType())
	if source == nil || handlerEvent == "" || inputEvent == "" {
		return false
	}
	if source.FlowEventMatches(flowID, handlerEvent, inputEvent) {
		return true
	}
	resolvedInput := runtimeeventidentity.Normalize(source.ResolveFlowEventReference(flowID, inputEvent))
	handler := runtimeeventidentity.Normalize(handlerEvent)
	return handler == resolvedInput || handler == runtimeeventidentity.Normalize(inputEvent) || handler == runtimeeventidentity.Normalize(pin.PinName())
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
