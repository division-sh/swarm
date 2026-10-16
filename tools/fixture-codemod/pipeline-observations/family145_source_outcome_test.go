package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

func assertFamily145SourceOutcomeProofs(t testing.TB, function, before, after string) {
	t.Helper()
	if function == "seedSourceOutcomeThatMustNotSuppressFork" {
		if sourceOutcomeFailureAssignment(before) == "" || sourceOutcomeFailureAssignment(before) != sourceOutcomeFailureAssignment(after) {
			t.Fatal("source outcome changed its exact normalized historical failure")
		}
		if !reflect.DeepEqual(sourceOutcomeFixtureCoordinates(after), map[string]string{
			"RunID": "sourceRunID", "EventID": "sourceEventID", "EntityID": "entityID", "CreatedAt": "at", "Failure": "failure",
		}) || !strings.Contains(after, "storetest.SeedSelectedSourceOutcome(ctx, selected,") {
			t.Fatal("source outcome lost original owner or exact historical coordinates")
		}
		if strings.Contains(after, "ExecContext") || strings.Contains(after, "*sql.DB") || strings.Contains(after, "context.Background()") {
			t.Fatal("source outcome retained raw or source-free authority")
		}
		return
	}
	for _, call := range []string{"RequireRun", "ExistingRunRootIngressWithRoutingSourceAndMode", "CommitSemanticEventWithRoutes", "commitSelectedOperationRootFixture"} {
		if !reflect.DeepEqual(sourceOutcomePreparationCalls(before, call), sourceOutcomePreparationCalls(after, call)) {
			t.Fatalf("source preparation changed %s", call)
		}
	}
	if strings.Contains(after, "*sql.DB") || strings.Contains(after, "BeginTx") || strings.Contains(after, "CaptureSQLite") ||
		!strings.Contains(after, `if backend == "postgres"`) || !strings.Contains(after, "storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)") {
		t.Fatal("source preparation retained a pool/protocol or lost its original backend posture")
	}
}

func sourceOutcomeAST(source string) *ast.File {
	file, err := parser.ParseFile(token.NewFileSet(), "outcome.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		panic(err)
	}
	return file
}

func sourceOutcomeFailureAssignment(source string) string {
	var out string
	ast.Inspect(sourceOutcomeAST(source), func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if ok && len(assignment.Lhs) == 1 {
			if name, ok := assignment.Lhs[0].(*ast.Ident); ok && name.Name == "failure" {
				out = family145NodeString(assignment)
			}
		}
		return true
	})
	return out
}

func sourceOutcomeFixtureCoordinates(source string) map[string]string {
	out := map[string]string{}
	ast.Inspect(sourceOutcomeAST(source), func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || family145NodeString(literal.Type) != "storetest.SelectedSourceOutcomeFixture" {
			return true
		}
		for _, element := range literal.Elts {
			field := element.(*ast.KeyValueExpr)
			out[family145NodeString(field.Key)] = family145NodeString(field.Value)
		}
		return true
	})
	return out
}

func sourceOutcomePreparationCalls(source, name string) []string {
	var out []string
	ast.Inspect(sourceOutcomeAST(source), func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		var called string
		switch function := call.Fun.(type) {
		case *ast.Ident:
			called = function.Name
		case *ast.SelectorExpr:
			called = function.Sel.Name
		}
		if called == name {
			out = append(out, family145NodeString(call))
		}
		return true
	})
	return out
}
