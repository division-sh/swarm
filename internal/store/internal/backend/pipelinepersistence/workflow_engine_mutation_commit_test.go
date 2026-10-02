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
		name        string
		transition  runtimepipeline.WorkflowEngineStateTransition
		createState bool
		historyStep string
	}{
		{"create state and companion", runtimepipeline.WorkflowEngineStateTransitionCreateStateAndCompanion, true, "create"},
		{"update state and companion", runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion, false, "mutate"},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision, err := decideWorkflowEngineState(test.transition)
			if err != nil {
				t.Fatal(err)
			}
			if decision.createState != test.createState || decision.historyStep != test.historyStep {
				t.Fatalf("decision = %+v, want createState=%t historyStep=%q", decision, test.createState, test.historyStep)
			}
		})
	}
	for _, transition := range []runtimepipeline.WorkflowEngineStateTransition{
		runtimepipeline.WorkflowEngineStateTransitionUnknown,
		runtimepipeline.WorkflowEngineStateTransition(3),
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
		if !ok || function.Name.Name != "commitPostgresWorkflowEngineState" && function.Name.Name != "commitSQLiteWorkflowEngineState" && function.Name.Name != "commitWorkflowHeaderAndFields" {
			continue
		}
		seen[function.Name.Name] = true
		sharedOwner := function.Name.Name == "commitWorkflowHeaderAndFields"
		delegations := 0
		usesStateDecision := false
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if callee, ok := call.Fun.(*ast.Ident); ok && callee.Name == "commitWorkflowHeaderAndFields" {
					delegations++
				}
			}
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if !sharedOwner && (selector.Sel.Name == "ExecContext" || selector.Sel.Name == "QueryContext" || selector.Sel.Name == "QueryRowContext") {
				t.Errorf("%s bypasses the shared header/field writer", function.Name.Name)
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
			return true
		})
		if sharedOwner {
			if !usesStateDecision {
				t.Errorf("%s bypasses the shared construction/mutation decision", function.Name.Name)
			}
		} else if delegations != 1 || usesStateDecision {
			t.Errorf("%s must delegate once without interpreting construction: delegations=%d", function.Name.Name, delegations)
		}
	}
	if !seen["commitPostgresWorkflowEngineState"] || !seen["commitSQLiteWorkflowEngineState"] || !seen["commitWorkflowHeaderAndFields"] {
		t.Fatal("a workflow engine dialect or shared writer is missing")
	}
}
