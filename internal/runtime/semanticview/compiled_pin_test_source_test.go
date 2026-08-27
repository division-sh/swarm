package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type compiledPinTestSource struct {
	Source
	inputs  map[string][]runtimecontracts.CompiledFlowInputPin
	outputs map[string][]runtimecontracts.CompiledFlowOutputPin
}

func withCompiledTestPins(t testing.TB, source Source, inputs map[string][]runtimecontracts.FlowInputEventPin, outputs map[string][]runtimecontracts.FlowOutputEventPin) *compiledPinTestSource {
	t.Helper()
	out := &compiledPinTestSource{Source: source, inputs: map[string][]runtimecontracts.CompiledFlowInputPin{}, outputs: map[string][]runtimecontracts.CompiledFlowOutputPin{}}
	for flowID, pins := range inputs {
		for _, authored := range pins {
			pin, err := runtimecontracts.CompileFlowInputPin(runtimecontracts.FlowPinCompilationContext{FlowID: flowID, FlowPath: flowID}, authored)
			if err != nil {
				t.Fatalf("compile test input pin %s/%s: %v", flowID, authored.EventType(), err)
			}
			out.inputs[flowID] = append(out.inputs[flowID], pin)
		}
	}
	for flowID, pins := range outputs {
		for _, authored := range pins {
			pin, err := runtimecontracts.CompileFlowOutputPin(runtimecontracts.FlowPinCompilationContext{FlowID: flowID, FlowPath: flowID}, authored)
			if err != nil {
				t.Fatalf("compile test output pin %s/%s: %v", flowID, authored.EventType(), err)
			}
			out.outputs[flowID] = append(out.outputs[flowID], pin)
		}
	}
	return out
}

func (s *compiledPinTestSource) FlowInputEventPins(flowID string) []runtimecontracts.CompiledFlowInputPin {
	return append([]runtimecontracts.CompiledFlowInputPin(nil), s.inputs[flowID]...)
}

func (s *compiledPinTestSource) FlowOutputEventPins(flowID string) []runtimecontracts.CompiledFlowOutputPin {
	return append([]runtimecontracts.CompiledFlowOutputPin(nil), s.outputs[flowID]...)
}

func (s *compiledPinTestSource) FlowInputEvents(flowID string) []string {
	return compiledInputEventTypes(s.inputs[flowID])
}

func (s *compiledPinTestSource) FlowOutputEvents(flowID string) []string {
	out := make([]string, 0, len(s.outputs[flowID]))
	for _, pin := range s.outputs[flowID] {
		out = append(out, pin.EventType())
	}
	return out
}

func (s *compiledPinTestSource) FlowInputEventPin(flowID, eventType string) (runtimecontracts.CompiledFlowInputPin, bool) {
	for _, pin := range s.inputs[flowID] {
		if pin.EventType() == eventType {
			return pin, true
		}
	}
	return runtimecontracts.CompiledFlowInputPin{}, false
}

func (s *compiledPinTestSource) FlowOutputEventPin(flowID, eventType string) (runtimecontracts.CompiledFlowOutputPin, bool) {
	for _, pin := range s.outputs[flowID] {
		if pin.EventType() == eventType {
			return pin, true
		}
	}
	return runtimecontracts.CompiledFlowOutputPin{}, false
}

func (s *compiledPinTestSource) FlowHasInputEvent(flowID, eventType string) bool {
	_, ok := s.FlowInputEventPin(flowID, eventType)
	return ok
}

func (s *compiledPinTestSource) FlowHasOutputEvent(flowID, eventType string) bool {
	_, ok := s.FlowOutputEventPin(flowID, eventType)
	return ok
}

func compiledInputEventTypes(pins []runtimecontracts.CompiledFlowInputPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}
