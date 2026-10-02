package testutil

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func capacityPrecedesAllocation(t *testing.T, source, function string, allocationCalls []string) bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "allocation.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function {
			continue
		}
		var admission token.Pos
		var count int
		mutations := make(map[string]token.Pos)
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			var name string
			switch callee := call.Fun.(type) {
			case *ast.Ident:
				name = callee.Name
			case *ast.SelectorExpr:
				name = callee.Sel.Name
			}
			if name == "ValidateServerCapacity" {
				admission = call.Pos()
				count++
			}
			for _, mutation := range allocationCalls {
				if name == mutation && mutations[name] == 0 {
					mutations[name] = call.Pos()
				}
			}
			return true
		})
		if count != 1 {
			return false
		}
		for _, name := range allocationCalls {
			if mutations[name] == 0 || mutations[name] < admission {
				return false
			}
		}
		return true
	}
	return false
}

func TestPostgresCapacityAllocationGuard(t *testing.T) {
	for _, owner := range []struct {
		path     string
		function string
		writes   []string
	}{
		{"internal/testpostgres/manager.go", "NewManager", []string{"ensureControlDatabase", "Reconcile"}},
		{"internal/releasee2e/golden_agent_workload_test.go", "goldenPostgresStore", []string{"ExecContext"}},
	} {
		t.Run(owner.function, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(testRepoRoot(t), owner.path))
			if err != nil {
				t.Fatal(err)
			}
			if !capacityPrecedesAllocation(t, string(source), owner.function, owner.writes) {
				t.Fatal("capacity admission must precede every resource writer")
			}
			mutated := strings.Replace(string(source), "ValidateServerCapacity", "omittedCapacityAdmission", 1)
			if capacityPrecedesAllocation(t, mutated, owner.function, owner.writes) {
				t.Fatal("guard accepted removal of allocation admission")
			}
		})
	}
}
