package pipelinepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestWorkflowEngineStateRevisionConflictIsRetryable(t *testing.T) {
	err := workflowEngineStateRevisionConflict(runtimepipeline.WorkflowEngineStateRecord{
		ExpectedRevision: 3,
		ExpectedState:    "collecting",
	})
	failure, ok := runtimefailures.As(err)
	if !ok {
		t.Fatalf("workflow revision conflict is untyped: %v", err)
	}
	if failure.Failure.Class != runtimefailures.ClassLifecycleConflict ||
		failure.Failure.Detail.Code != "workflow_engine_state_revision_conflict" ||
		!failure.Failure.Retryable {
		t.Fatalf("workflow revision conflict = %#v, want retryable lifecycle conflict", failure.Failure)
	}
	if got := runtimeengine.FailureDispositionFor(err); got != runtimeengine.FailureDispositionRetry {
		t.Fatalf("workflow revision conflict disposition = %s, want retry", got)
	}
}

func TestWorkflowEngineStateDecisionOwnsCompanionAndHistory(t *testing.T) {
	for _, test := range []struct {
		name            string
		transition      runtimepipeline.WorkflowEngineStateTransition
		createState     bool
		createCompanion bool
		historyStep     string
	}{
		{"create state and companion", runtimepipeline.WorkflowEngineStateTransitionCreateStateAndCompanion, true, true, "create"},
		{"update state and companion", runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion, false, false, "mutate"},
		{"update state create companion", runtimepipeline.WorkflowEngineStateTransitionUpdateStateCreateCompanion, false, true, "mutate"},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision, err := decideWorkflowEngineState(test.transition)
			if err != nil {
				t.Fatal(err)
			}
			if decision.createState != test.createState || decision.createCompanion != test.createCompanion || decision.historyStep != test.historyStep {
				t.Fatalf("decision = %+v, want createState=%t createCompanion=%t historyStep=%q", decision, test.createState, test.createCompanion, test.historyStep)
			}
		})
	}
	for _, transition := range []runtimepipeline.WorkflowEngineStateTransition{
		runtimepipeline.WorkflowEngineStateTransitionUnknown,
		runtimepipeline.WorkflowEngineStateTransition(255),
	} {
		if decision, err := decideWorkflowEngineState(transition); err == nil || decision != (workflowEngineStateDecision{}) {
			t.Fatalf("unknown transition %d returned decision %+v, error %v", transition, decision, err)
		}
	}
}

func TestWorkflowEngineDialectWritersUseSharedDecision(t *testing.T) {
	source, err := os.ReadFile("workflow_engine_mutation_commit.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "workflow_engine_mutation_commit.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "commitPostgresWorkflowEngineState" && function.Name.Name != "commitSQLiteWorkflowEngineState" {
			continue
		}
		seen[function.Name.Name] = true
		usesStateDecision, usesCompanionDecision := false, false
		ast.Inspect(function.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			owner, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if owner.Name == "record" && selector.Sel.Name == "Transition" {
				t.Errorf("%s independently interprets the transition", function.Name.Name)
			}
			if owner.Name == "decision" && selector.Sel.Name == "createState" {
				usesStateDecision = true
			}
			if owner.Name == "decision" && selector.Sel.Name == "createCompanion" {
				usesCompanionDecision = true
			}
			return true
		})
		if !usesStateDecision || !usesCompanionDecision {
			t.Errorf("%s bypasses the shared state/companion decision", function.Name.Name)
		}
	}
	if !seen["commitPostgresWorkflowEngineState"] || !seen["commitSQLiteWorkflowEngineState"] {
		t.Fatal("a workflow engine dialect writer is missing")
	}
}
