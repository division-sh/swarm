package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestSelectedHandlerCommitResultHasOneInterpreter(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "engine_adapter.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var entry, shared bool
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		switch function.Name.Name {
		case "CommitEngineMutation":
			entry = true
		case "commitPreparedEngineMutation":
			shared = true
		}
		callsShared, callsStore := 0, 0
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "commitPreparedEngineMutation":
				callsShared++
			case "CommitWorkflowEngineMutation":
				callsStore++
			}
			return true
		})
		if function.Name.Name == "CommitEngineMutation" && callsShared < 2 {
			t.Error("stateful and entityless selected handlers must share commit-result consumption")
		}
		if function.Name.Name == "commitPreparedEngineMutation" && callsStore != 1 {
			t.Errorf("shared result owner has %d durable commit calls, want one", callsStore)
		}
		if function.Name.Name != "commitPreparedEngineMutation" && callsStore != 0 {
			t.Errorf("%s bypasses the shared commit-result owner", function.Name.Name)
		}
	}
	if !entry || !shared {
		t.Fatal("selected handler entry or shared commit-result owner is missing")
	}
}

func TestRetiredHandlerExecutionInterpretersStayAbsent(t *testing.T) {
	retired := []string{
		"executeAuthoritativeNodeHandler",
		"workflowNodeExecutors",
		"coordinatorHandlerExecutionEngine",
		"ExecuteHandlerSteps",
		"commitEntitylessEngineMutation",
	}
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		name := file.Name()
		if file.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, symbol := range retired {
			if strings.Contains(string(body), symbol) {
				t.Errorf("%s restored retired handler interpreter %s", name, symbol)
			}
		}
	}
}
