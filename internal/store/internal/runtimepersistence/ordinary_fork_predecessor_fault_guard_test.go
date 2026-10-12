package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestOrdinaryForkAbsentCutFaultOwnsExactOriginCAS(t *testing.T) {
	path := filepath.Join(repoRootForRuntimeWriterGuard(t), "internal/store/internal/backend/runlifecycle/test_receiver_fork_cut.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := "UPDATE runs SET forked_from_revision=$1 WHERE run_id=$2 AND origin_kind=$3 AND forked_from_run_id=$4 AND forked_from_point_kind=$5 AND forked_from_revision=$6 AND COALESCE(CAST(forked_from_event_id AS TEXT),'')=$7"
	calls := 0
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "ExecContext" {
			return true
		}
		calls++
		if len(call.Args) != 9 {
			t.Fatalf("origin fault lost its exact seven-coordinate CAS: %+v", call.Args)
		}
		literal, ok := call.Args[1].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			t.Fatal("origin fault regained arbitrary statement authority")
		}
		query, err := strconv.Unquote(literal.Value)
		if err != nil || strings.Join(strings.Fields(query), " ") != want {
			t.Fatalf("origin fault changed its sole column or complete origin fence: %s %v", query, err)
		}
		return true
	})
	if calls != 1 {
		t.Fatalf("origin fault has %d write sites, want exactly one", calls)
	}
}
