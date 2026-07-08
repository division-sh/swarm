package pipeline

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

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

func TestLoadWorkflowDefinitionUsesFlowScopedTerminalOwnership(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Package: runtimecontracts.ProjectPackageDocument{Name: "demo", Version: "1.0.0"},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			StageDeclarations: runtimecontracts.FlowStageDeclarations{
				Declared: true,
				Entries: []runtimecontracts.FlowStageDeclaration{
					{ID: "ready", Initial: true},
					{ID: "done"},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "demo",
			Stages: []runtimecontracts.WorkflowStageContract{
				{ID: "ready"},
				{ID: "done"},
			},
			TerminalStages: []string{"done"},
			FlowTerminal: map[string][]string{
				"child": []string{"done"},
			},
		},
	}

	workflow, err := LoadWorkflowDefinition(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	done, ok := workflow.Stage("done")
	if !ok {
		t.Fatal("missing done stage")
	}
	if done.Terminal {
		t.Fatalf("done terminal = true, want false because only child flow owns done as terminal")
	}
}
