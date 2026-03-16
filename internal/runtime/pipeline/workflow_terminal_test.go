package pipeline

import "testing"

func TestWorkflowDefinition_BlocksTerminalExitByDefault(t *testing.T) {
	workflow := NewWorkflowDefinition("demo", []WorkflowStage{
		{Name: "pending"},
		{Name: "done", Terminal: true},
		{Name: "reopened"},
	}, []WorkflowTransition{
		{
			Name:    "reopen",
			From:    []WorkflowStateID{"*"},
			To:      "reopened",
			Trigger: "task.reopen_requested",
		},
	})
	if workflow.CanTransition(WorkflowState{Stage: "done"}, "reopened") {
		t.Fatal("expected terminal state transition to be blocked by default")
	}
}

func TestWorkflowDefinition_AllowsExplicitTerminalExit(t *testing.T) {
	workflow := NewWorkflowDefinition("demo", []WorkflowStage{
		{Name: "pending"},
		{Name: "done", Terminal: true},
		{Name: "reopened"},
	}, []WorkflowTransition{
		{
			Name:              "reopen",
			From:              []WorkflowStateID{"*"},
			To:                "reopened",
			Trigger:           "task.reopen_requested",
			AllowTerminalExit: true,
		},
	})
	if !workflow.CanTransition(WorkflowState{Stage: "done"}, "reopened") {
		t.Fatal("expected explicit allow_terminal_exit transition to pass")
	}
}
