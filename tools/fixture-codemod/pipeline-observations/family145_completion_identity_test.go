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

func TestFamily145CompletionIdentityWholeCallerInventoryUsesSelectedOwner(t *testing.T) {
	paths := []string{
		"notify_all_children_runtime_conformance_test.go",
		"fan_out_nested_capacity_conformance_test.go",
		"fan_out_nested_interrupted_group_test.go",
	}
	var calls int
	for _, path := range paths {
		data, err := os.ReadFile("../../../internal/runtime/conformance/" + path)
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
			if !ok || fn.Name != "loadNotifyAllChildrenSingleEventID" {
				return true
			}
			calls++
			if len(call.Args) != 5 || (family145NodeString(call.Args[2]) != "selected" && family145NodeString(call.Args[2]) != "rt.selected") {
				t.Errorf("completion identity retained a foreign owner or pool: %s", family145NodeString(call))
			}
			return true
		})
		if path == "notify_all_children_runtime_conformance_test.go" {
			root, err := uniqueFunction(file, "TestFanOutDeliveryBarrierCompletesThroughRealEventBusAndPublicReadbackOnBothBackends")
			if err != nil || family145PublicationRawEscape([]byte("package proof\n"+family145NodeString(root))) != nil {
				t.Fatalf("complete barrier fixture retained raw construction or observation: %v", err)
			}
			read, err := uniqueFunction(file, "loadNotifyAllChildrenSingleEventID")
			if err != nil || strings.Contains(family145NodeString(read), "*sql.DB") {
				t.Fatalf("shared identity helper retained raw authority: %v", err)
			}
		}
	}
	if calls != 3 {
		t.Fatalf("completion identity caller inventory changed: %d", calls)
	}
}

func TestFamily145CompletionIdentityRefusalAssertionRejectsDroppedCardinality(t *testing.T) {
	data, err := os.ReadFile("../../../internal/runtime/conformance/notify_all_children_runtime_conformance_test.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "completion.go", data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "loadNotifyAllChildrenSingleEventID")
	if err != nil {
		t.Fatal(err)
	}
	source := family145NodeString(fn)
	if err := family145CompletionCardinalityAssertion(source); err != nil {
		t.Fatal(err)
	}
	for _, condition := range []string{"false", "len(ids) == 0", "len(ids) > 1"} {
		hostile := strings.Replace(source, "len(ids) != 1", condition, 1)
		if err := family145CompletionCardinalityAssertion(hostile); err == nil {
			t.Fatalf("zero/multiple identity refusal weakened: %s", condition)
		}
	}
}

func family145CompletionCardinalityAssertion(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "completion.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return err
	}
	var found bool
	ast.Inspect(file, func(node ast.Node) bool {
		conditional, ok := node.(*ast.IfStmt)
		if !ok || family145NodeString(conditional.Cond) != "len(ids) != 1" {
			return true
		}
		want := `t.Fatalf("events %s for run %s = %#v, want exactly one", eventName, runID, ids)`
		found = len(conditional.Body.List) == 1 && family145NodeString(conditional.Body.List[0]) == want
		return false
	})
	if !found {
		return fmt.Errorf("complete zero/multiple physical identity refusal changed")
	}
	return nil
}
