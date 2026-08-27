package contracts

import "testing"

func mustCompileInputPinForTest(t *testing.T, flowID, eventType string) CompiledFlowInputPin {
	t.Helper()
	pin, err := CompileFlowInputPin(
		FlowPinCompilationContext{FlowID: flowID, FlowPath: flowID},
		FlowInputEventPin{Event: eventType},
	)
	if err != nil {
		t.Fatalf("compile input pin %s/%s: %v", flowID, eventType, err)
	}
	return pin
}
