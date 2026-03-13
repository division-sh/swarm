package pipeline

import "strings"

type WorkflowStateID string

func NormalizeWorkflowStateID(raw string) WorkflowStateID {
	return WorkflowStateID(strings.TrimSpace(raw))
}

func CanTransitionWorkflowState(from, to WorkflowStateID) bool {
	workflow := DefaultPipelineWorkflow()
	if workflow == nil {
		return from == to
	}
	return workflow.CanTransition(WorkflowState{Stage: NormalizeWorkflowStateID(string(from))}, NormalizeWorkflowStateID(string(to)))
}

func WorkflowStateTransition(from, to WorkflowStateID) (WorkflowTransition, bool) {
	workflow := DefaultPipelineWorkflow()
	if workflow == nil {
		return WorkflowTransition{}, false
	}
	return workflow.Transition(WorkflowState{Stage: NormalizeWorkflowStateID(string(from))}, NormalizeWorkflowStateID(string(to)))
}
