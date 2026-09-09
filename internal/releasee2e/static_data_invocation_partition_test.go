package releasee2e

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestStaticDataInvocationShardCoverage(t *testing.T) {
	want := []string{
		"relative-inside", "absolute-inside", "relative-outside", "absolute-outside",
		"omitted", "dot", "symlink-cwd", "alias", "chain", "ancestor",
		"relative-alias-parent", "absolute-alias-parent",
	}
	var got []string
	for shard := range 6 {
		cells := staticDataInvocationCells(shard, "/fixture")
		if len(cells) != 2 {
			t.Fatalf("shard%d has%d geometries", shard+1, len(cells))
		}
		for _, cell := range cells {
			got = append(got, cell.name)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("invocation leaves=%v, want%v", got, want)
	}
	// Validate the actual scheduled wrappers, not only the partition arithmetic.
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/static_data_invocation_test.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for shard := range 6 {
		name := fmt.Sprintf("TestDurableDataInvocationInvarianceSQLitePostgresShard%d", shard+1)
		found := false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != name {
				continue
			}
			if len(fn.Body.List) != 1 {
				t.Fatalf("%s is not a direct proof delegation", name)
			}
			expr, ok := fn.Body.List[0].(*ast.ExprStmt)
			if !ok {
				t.Fatalf("%s is not a call", name)
			}
			call, ok := expr.X.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				t.Fatalf("%s changed proof arguments", name)
			}
			callee, ok := call.Fun.(*ast.Ident)
			if !ok || callee.Name != "testDurableDataInvocation" {
				t.Fatalf("%s bypasses proof", name)
			}
			index, ok := call.Args[1].(*ast.BasicLit)
			if !ok || index.Value != strconv.Itoa(shard) {
				t.Fatalf("%s selects wrong shard", name)
			}
			found = true
		}
		if !found {
			t.Fatalf("missing executable proof %s", name)
		}
	}
}
