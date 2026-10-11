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

func TestFamily145NumericRegistrationPropagationPreservesEveryOtherArgument(t *testing.T) {
	before := `package proof
func caller() {
	loadNotifyAllChildrenNumericRegistrations(t, ctx, selected, db, runID)
	loadNotifyAllChildrenNumericRegistrations(t, mixedCtx, f.selected, f.db, mixedRunID)
	assertOriginalBusinessValues(t, ctx, selected, db, runID)
}`
	after, calls, err := rewriteFamily145NumericRegistrationCalls("proof_test.go", []byte(before))
	if err != nil || calls != 2 {
		t.Fatalf("numeric registration propagation: %d/%v", calls, err)
	}
	want := strings.Replace(before, "ctx, selected, db, runID)", "ctx, selected, runID)", 1)
	want = strings.Replace(want, "mixedCtx, f.selected, f.db, mixedRunID)", "mixedCtx, f.selected, mixedRunID)", 1)
	parse := func(source []byte) string {
		t.Helper()
		file, err := parser.ParseFile(token.NewFileSet(), "proof.go", source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		fn, err := uniqueFunction(file, "caller")
		if err != nil {
			t.Fatal(err)
		}
		return family145NodeString(fn)
	}
	if parse(after) != parse([]byte(want)) {
		t.Fatalf("changed context, scope or business call: %s", after)
	}
}

func TestFamily145NumericRegistrationPropagationRejectsForeignAndChangedBindings(t *testing.T) {
	for _, arguments := range []string{
		"t, ctx, selected, peerPool, runID", "t, ctx, selected, openPool(), runID",
		"t, ctx, f.selected, db, runID", "otherTest, ctx, selected, db, runID",
		"t, ctx, selected, runID", "t, ctx, selected, db, runID, override",
	} {
		if _, _, err := rewriteFamily145NumericRegistrationCalls("proof_test.go", []byte("package proof\nfunc caller(){loadNotifyAllChildrenNumericRegistrations("+arguments+")}")); err == nil {
			t.Fatalf("unreviewed owner binding accepted: %s", arguments)
		}
	}
}

func TestFamily145NumericRegistrationWholeCallerInventoryIsNative(t *testing.T) {
	var calls int
	for _, input := range family145NumericRegistrationSources {
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
			if !ok || fn.Name != "loadNotifyAllChildrenNumericRegistrations" {
				return true
			}
			calls++
			if len(call.Args) != 4 || family145NodeString(call.Args[3]) == "db" || family145NodeString(call.Args[3]) == "f.db" {
				t.Errorf("%s retained registration SQL authority", input.name)
			}
			return true
		})
		if input.name == "empty_handler_construction_test.go" || input.name == "fan_out_shared_serving_conformance_test.go" {
			if err := family145PublicationRawEscape(data); err != nil {
				t.Fatalf("%s retained a raw fixture dependency: %v", input.name, err)
			}
		}
	}
	if calls != 8 {
		t.Fatalf("whole registration caller inventory changed: %d", calls)
	}
}

func TestFamily145NumericRegistrationBusinessOracleRejectsLostAccountAndDecodeAssertions(t *testing.T) {
	data, err := os.ReadFile("../../../internal/runtime/conformance/notify_all_children_runtime_conformance_test.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "notify.go", data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "loadNotifyAllChildrenNumericRegistrations")
	if err != nil {
		t.Fatal(err)
	}
	source := family145NodeString(fn)
	original, err := family145DiagnosticBusinessAssertions(source)
	if err != nil || len(original) != 3 {
		t.Fatalf("fixed query, payload decode and account refusal assertions: %v/%v", original, err)
	}
	for _, assertion := range original {
		hostile := strings.Replace(source, assertion, "ignore()", 1)
		changed, err := family145DiagnosticBusinessAssertions(hostile)
		if err != nil || strings.Join(original, "\n") == strings.Join(changed, "\n") {
			t.Fatalf("removed business assertion was invisible: %s/%v", assertion, err)
		}
	}
}
