package releasee2e

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestGoldenBurstPartitionsPreserveBothRepetitionsAndWorkload(t *testing.T) {
	if goldenBurstCandidateN != 10 || goldenBurstIterations != 2 || goldenBurstGOMAXPROCS != 2 {
		t.Fatal("partitioning changed the admitted burst workload")
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/golden_agent_workload_test.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	raceBuilds, backendExecutions := 0, 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if strings.HasPrefix(fn.Name.Name, "TestGoldenAgentWorkloadBurstConcurrency") {
			if len(fn.Body.List) != 1 {
				t.Fatalf("%s must execute exactly one unfiltered iteration", fn.Name.Name)
			}
			expr, ok := fn.Body.List[0].(*ast.ExprStmt)
			if !ok {
				t.Fatalf("%s has no direct iteration call", fn.Name.Name)
			}
			call, ok := expr.X.(*ast.CallExpr)
			if !ok || len(call.Args) != 3 {
				t.Fatalf("%s has invalid iteration arguments", fn.Name.Name)
			}
			owner, ok := call.Fun.(*ast.Ident)
			if !ok || owner.Name != "runGoldenAgentWorkloadBurstIteration" {
				t.Fatalf("%s bypasses the burst proof", fn.Name.Name)
			}
			literal, ok := call.Args[1].(*ast.BasicLit)
			if !ok || literal.Kind != token.INT {
				t.Fatalf("%s must name an exact iteration", fn.Name.Name)
			}
			iteration, err := strconv.Atoi(literal.Value)
			if err != nil || iteration < 1 || iteration > goldenBurstIterations || !strings.HasSuffix(fn.Name.Name, "Iteration"+literal.Value) {
				t.Fatalf("duplicate, missing, or renamed burst iteration: %s", fn.Name.Name)
			}
			backendLiteral, ok := call.Args[2].(*ast.BasicLit)
			if !ok || backendLiteral.Kind != token.STRING {
				t.Fatalf("%s must name an exact backend", fn.Name.Name)
			}
			backend, err := strconv.Unquote(backendLiteral.Value)
			if err != nil || (backend != "sqlite" && backend != "postgres") {
				t.Fatalf("invalid burst backend in %s", fn.Name.Name)
			}
			key := fmt.Sprintf("%s/%d", backend, iteration)
			if seen[key] {
				t.Fatalf("duplicate burst cell %s", key)
			}
			seen[key] = true
		}
		if fn.Name.Name == "runGoldenAgentWorkloadBurstIteration" {
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if ok {
					switch name.Name {
					case "buildRaceReleaseBinary":
						raceBuilds++
					case "runGoldenAgentWorkload":
						backendExecutions++
					}
				}
				return true
			})
		}
	}
	if len(seen) != 2*goldenBurstIterations || raceBuilds != 1 || backendExecutions != 1 {
		t.Fatalf("burst coverage: iterations=%v race builds=%d backend executions=%d", seen, raceBuilds, backendExecutions)
	}
}
