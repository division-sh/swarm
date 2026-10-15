package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFamily145NotifyRuntimePropagationOnlyRemovesTheReviewedPoolArgument(t *testing.T) {
	before := `package proof
func caller() {
	newNotifyAllChildrenRuntime(t, selected, db, source, time.Now)
	newNotifyAllChildrenRuntime(t, f.selected, f.db, f.source, clock, options)
	newNotifyAllChildrenRuntime(t, backend, db, source, fixedClock, options)
	assertBusinessValues(t, selected, db, source)
}`
	after, count, err := rewriteFamily145NotifyRuntimeCalls("proof_test.go", []byte(before))
	if err != nil || count != 3 {
		t.Fatalf("finite pool propagation: calls=%d/%v", count, err)
	}
	want := strings.ReplaceAll(before, "selected, db, source", "selected, source")
	want = strings.ReplaceAll(want, "f.selected, f.db, f.source", "f.selected, f.source")
	want = strings.ReplaceAll(want, "backend, db, source", "backend, source")
	// The unrelated business call must keep its original arguments.
	want = strings.Replace(want, "assertBusinessValues(t, selected, source)", "assertBusinessValues(t, selected, db, source)", 1)
	afterFile, err := parser.ParseFile(token.NewFileSet(), "after.go", after, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	wantFile, err := parser.ParseFile(token.NewFileSet(), "want.go", want, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	afterFunction, err := uniqueFunction(afterFile, "caller")
	if err != nil {
		t.Fatal(err)
	}
	wantFunction, err := uniqueFunction(wantFile, "caller")
	if err != nil || family145NodeString(afterFunction) != family145NodeString(wantFunction) {
		t.Fatalf("propagation changed another argument or statement: %s/%v", after, err)
	}
}

func TestFamily145NotifyRuntimePropagationRejectsUnreviewedBindings(t *testing.T) {
	for _, arguments := range []string{
		"t, selected, peerPool, source, time.Now", "t, selected, openPool(), source, time.Now",
		"t, f.selected, selected.db, source, time.Now", "otherTest, selected, db, source, time.Now",
		"t, selected, db, source", "t, selected, source, time.Now",
	} {
		if _, _, err := rewriteFamily145NotifyRuntimeCalls("proof_test.go", []byte("package proof\nfunc caller(){newNotifyAllChildrenRuntime("+arguments+")}")); err == nil {
			t.Fatalf("unreviewed source/owner binding accepted: %s", arguments)
		}
	}
}

func TestFamily145NotifyRuntimeWholeCallerInventoryHasNoPoolCarrier(t *testing.T) {
	var calls int
	for _, input := range family145NotifyRuntimeSources {
		path := filepath.Join("../../..", "internal/runtime/conformance", input.name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.Ident)
			if !ok || fn.Name != "newNotifyAllChildrenRuntime" {
				return true
			}
			calls++
			if len(call.Args) < 4 || len(call.Args) > 5 || family145NodeString(call.Args[2]) == "db" || family145NodeString(call.Args[2]) == "f.db" {
				t.Errorf("%s retained a runtime pool carrier", input.name)
			}
			return true
		})
		if input.name == "notify_all_children_runtime_conformance_test.go" {
			fn, err := uniqueFunction(file, "newNotifyAllChildrenRuntime")
			if err != nil || strings.Contains(family145NodeString(fn), "*sql.DB") {
				t.Fatalf("shared runtime construction retained SQL authority: %v", err)
			}
		}
	}
	if calls != 24 {
		t.Fatalf("whole runtime caller inventory changed: %d", calls)
	}
}
