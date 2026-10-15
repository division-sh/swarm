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

func assertFamily145DiagnosticHistoryProofs(t testing.TB, name, before, after string) {
	t.Helper()
	for _, raw := range []string{"*sql.DB", "ExecContext", "context.Background()", "authoractivityfixture.Dialect", "InsertDiagnosticDirectEventRecordForRun"} {
		if strings.Contains(after, raw) {
			t.Fatalf("diagnostic history retained %s", raw)
		}
	}
	if name == "seedSelectedExecutionDiagnosticPlatformDeadLetter" {
		beforePayload, afterPayload := sourceOutcomePreparationCalls(before, "Marshal"), sourceOutcomePreparationCalls(after, "Marshal")
		if len(beforePayload) != 1 || !reflect.DeepEqual(beforePayload, afterPayload) ||
			!strings.Contains(after, "storetest.SeedRunDiagnosticDeadLetterHistory(t, ctx, selected, sourceRunID, diagnosticEventID, payload, at)") {
			t.Fatal("runtime diagnostic history changed original payload/owner/coordinates")
		}
		return
	}
	original := diagnosticHistoryCallArgs(before, "InsertDiagnosticDirectEventRecordForRun")
	replacement := diagnosticHistoryCallArgs(after, "SeedRunDiagnosticDeadLetterHistory")
	if len(original) != 10 || len(replacement) != 7 || original[8] != replacement[5] ||
		strings.Join(replacement[:5], ",") != "t,ctx,selected,runID,eventID" || replacement[6] != "at" ||
		!strings.Contains(after, "storetest.CaptureSelectedRunForkRevision(t, ctx, selected, runID, runforkrevision.FamilyEvents, runforkrevision.FamilyEventReceipts)") {
		t.Fatal("served diagnostic history lost exact payload, coordinates or original two-family capture")
	}
}

func diagnosticHistoryCallArgs(source, name string) []string {
	var out []string
	ast.Inspect(sourceOutcomeAST(source), func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != name {
			return true
		}
		for _, arg := range call.Args {
			out = append(out, family145NodeString(arg))
		}
		return true
	})
	return out
}

func TestFamily145DiagnosticHistoryConstructorRetiredAndAllCallersUseOriginalOwner(t *testing.T) {
	public, err := os.ReadFile("../../../internal/store/storetest/event.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(public), "InsertDiagnosticDirectEventRecordForRun") {
		t.Fatal("run diagnostic history retained the supplied-pool constructor")
	}
	var calls int
	for _, path := range []string{"internal/runtime/runforkexecution/execution_test.go", "internal/serveapp/run_fork_runtime_test.go"} {
		source, err := os.ReadFile("../../../" + path)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			switch fn.Name {
			case "seedSelectedExecutionDiagnosticPlatformDeadLetter":
				calls++
				if len(call.Args) != 6 || family145NodeString(call.Args[1]) != "ctx" || family145NodeString(call.Args[2]) != "pg" {
					t.Fatal("runtime diagnostic caller lost original context/selected owner")
				}
			case "seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter":
				calls++
				if len(call.Args) != 6 || family145NodeString(call.Args[1]) != "context.Background()" || family145NodeString(call.Args[2]) != "pg" {
					t.Fatal("served diagnostic caller lost its original selected owner")
				}
			}
			return true
		})
	}
	if calls != 3 {
		t.Fatalf("diagnostic history caller inventory=%d, want three", calls)
	}
}
