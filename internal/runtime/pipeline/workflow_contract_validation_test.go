package pipeline

import "testing"

func TestValidateWorkflowContracts_CurrentBundle(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, nil)
	if err := pc.ValidateWorkflowContracts(); err != nil {
		t.Fatalf("expected current workflow contracts to validate: %v", err)
	}
}
