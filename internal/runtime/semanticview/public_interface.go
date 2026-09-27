package semanticview

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

// SelectedRootInputPin is public write eligibility, not general schema lookup.
// Choosing another source root changes the compiled interface; child pins do not.
func SelectedRootInputPin(source Source, event string) (runtimecontracts.CompiledFlowInputPin, bool) {
	if source == nil || !eventidentity.IsValidName(event) {
		return runtimecontracts.CompiledFlowInputPin{}, false
	}
	pin, ok := source.FlowInputEventPin(".", event)
	return pin, ok && pin.Source() != runtimecontracts.FlowInputPinSourceHarness
}

// SelectedRootInputEndpoints preserves the full internal census while exposing
// only the chosen artifact's public input interface.
func SelectedRootInputEndpoints(source Source) []AuthoredEventEndpoint {
	if source == nil {
		return nil
	}
	var endpoints []AuthoredEventEndpoint
	for _, endpoint := range BuildAuthoredEventEndpointCensus(source).InputPins() {
		if endpoint.FlowID != "." {
			continue
		}
		if _, ok := SelectedRootInputPin(source, endpoint.PinName); ok {
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
}

func SelectedRootOutputPin(source Source, event string) (runtimecontracts.CompiledFlowOutputPin, bool) {
	if source == nil {
		return runtimecontracts.CompiledFlowOutputPin{}, false
	}
	return source.FlowOutputEventPin(".", event)
}
