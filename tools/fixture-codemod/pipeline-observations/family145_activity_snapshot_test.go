package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145ActivitySnapshotError(source string) error {
	want := `func (f activityForkFixture) capture(t *testing.T, runID string) {
	t.Helper()
	captureSelectedExecutionSourceRevision(t, f.owner.ports.fork, runID)
}`
	actual, err := canonicalFunction(source)
	expected, wantErr := canonicalFunction(want)
	if err != nil || wantErr != nil || actual != expected {
		return fmt.Errorf("activity snapshot bypasses its exact original selected capture owner")
	}
	return nil
}

func TestFamily145ActivitySnapshotRetiresIndependentProtocolAndRejectsBorrowedOwner(t *testing.T) {
	path := "../../../internal/runtime/runforkexecution/activity_fork_execution_test.go"
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{"BeginTx", "CaptureSQLite", "f.sqlite", "sqlite *store.SQLiteRuntimeStore"} {
		if strings.Contains(string(source), retired) {
			t.Fatalf("activity capture retained %s", retired)
		}
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var owners, callers int
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "capture" && fn.Recv != nil {
			owners++
			current := family145NodeString(fn)
			if err := family145ActivitySnapshotError(current); err != nil {
				t.Fatal(err)
			}
			for _, hostile := range []string{
				strings.Replace(current, "f.owner.ports.fork", "f.db.DB", 1),
				strings.Replace(current, "runID)", "foreignRunID)", 1),
				strings.Replace(current, "runID)", "runID, runforkrevision.FamilyEvents)", 1),
				strings.Replace(current, "t.Helper()", "t.Helper(); f.db.BeginTx(context.Background(), nil)", 1),
			} {
				if err := family145ActivitySnapshotError(hostile); err == nil {
					t.Fatal("borrowed transaction, owner, identity or reduced family set accepted")
				}
			}
		}
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "capture" {
			return true
		}
		callers++
		if family145NodeString(call.Fun) != "fixture.capture" || len(call.Args) != 2 || family145NodeString(call.Args[0]) != "t" || family145NodeString(call.Args[1]) != "sourceRunID" {
			t.Fatal("activity case loop changed its original snapshot cut/identity")
		}
		return true
	})
	if owners != 1 || callers != 1 {
		t.Fatalf("activity snapshot inventory=%d owners/%d loop callsites, want1/1", owners, callers)
	}
}
