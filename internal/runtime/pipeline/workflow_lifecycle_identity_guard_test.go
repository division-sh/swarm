package pipeline

import (
	"os"
	"strings"
	"testing"
)

func TestWorkflowLifecycleIdentityConsumersDoNotReintroduceFallbacks(t *testing.T) {
	files := []string{
		"workflow_lifecycle_plan.go",
		"workflow_timer_owner.go",
		"workflow_join_lifecycle.go",
		"workflow_gate_lifecycle.go",
		"workflow_gate_decision.go",
		"workflow_gate_terminal.go",
		"workflow_state_persistence.go",
		"select_entity.go",
		"workflow_instance_route_recovery.go",
	}
	forbidden := []string{
		"StoredFlowInstance(",
		"workflowTimerCanonicalEntityID",
		"FlowInstanceEntityID(selected.StorageRef)",
		"func (pc *PipelineCoordinator) applyWorkflowJoinIntents",
		"func (pc *PipelineCoordinator) applyWorkflowGateIntents",
		"func (pc *PipelineCoordinator) reconcileClosedJoinSchedules",
		"func (pc *PipelineCoordinator) reconcileSupersededLoopSchedules",
		"func (l *WorkflowTimerLifecycle) CancelSupersededGenerations",
	}
	for _, name := range files {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, token := range forbidden {
			if strings.Contains(string(raw), token) {
				t.Errorf("%s reintroduced non-authoritative lifecycle identity path %q", name, token)
			}
		}
	}
}
