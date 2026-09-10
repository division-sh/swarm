package contracts

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestJoinCapturedLoopOwnershipGuard(t *testing.T) {
	if violations := capturedLoopOwnershipViolations(t, nil); len(violations) != 0 {
		t.Fatalf("captured loop ownership census: %v", violations)
	}
}

func TestJoinCapturedLoopOwnershipGuardHostileApprovedFile(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal/runtime/engine/loop_execution.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	hostile := append(append([]byte(nil), original...), []byte(`
func hostileCapturedOwner(unfamiliar loopruntime.Activation, arbitrary *executionFrame) {
    arbitrary.state.SetLoop(unfamiliar.Context())
}
`)...)
	violations := capturedLoopOwnershipViolations(t, map[string][]byte{path: hostile})
	if len(violations) != 2 || !strings.Contains(strings.Join(violations, "\n"), "hostileCapturedOwner") {
		t.Fatalf("approved file or arbitrary receiver spelling hid a current-context reconstruction: %v", violations)
	}
}

func TestJoinCapturedLoopOwnershipGuardHostileApprovedMethod(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal/runtime/engine/loop_execution.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	from := "context, err := activation.CapturedContext(generation)"
	if strings.Count(string(original), from) != 1 {
		t.Fatal("captured owner probe site moved")
	}
	hostile := strings.Replace(string(original), from, from+"\n_ = activation.Context()", 1)
	violations := capturedLoopOwnershipViolations(t, map[string][]byte{path: []byte(hostile)})
	if len(violations) != 1 || !strings.Contains(violations[0], "Executor.bindJoinLoopContext") {
		t.Fatalf("approved method gained a competing current-context owner: %v", violations)
	}
}

func TestJoinCapturedLoopOwnershipGuardHostileDirectMap(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal/runtime/engine/loop_execution.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	hostile := append(append([]byte(nil), original...), []byte(`
func hostileBusinessRevision(arbitrary *executionFrame) {
    arbitrary.state.Loop = map[string]any{"revision_id": arbitrary.payload["revision_id"]}
}
`)...)
	violations := capturedLoopOwnershipViolations(t, map[string][]byte{path: hostile})
	if len(violations) != 1 || !strings.Contains(violations[0], "hostileBusinessRevision") {
		t.Fatalf("direct business-map reconstruction escaped typed field census: %v", violations)
	}
}

func capturedLoopOwnershipViolations(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	const module = "github.com/division-sh/swarm/internal/"
	root := handlerRuleIdentityGuardRepoRoot(t)
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, "./internal/runtime/loopruntime", "./internal/runtime/joinruntime", "./internal/runtime/engine", "./internal/runtime/pipeline", "./internal/runtime/genericschedule", "./internal/runtime/runforkexecution", "./internal/runtime/semanticview", "./internal/runtime/bootverify", "./internal/runtime/workflowexpr", "./internal/store/internal/backend/runforkpersistence", "./internal/store/internal/backend/pipelinepersistence", "./internal/store/internal/backend/genericschedule")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("captured loop guard requires successful compiler resolution: %v", err)
	}
	counts := map[string]int{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				scope := strings.TrimPrefix(pkg.PkgPath, module) + "::<package>"
				if fn, ok := decl.(*ast.FuncDecl); ok {
					resolved, ok := pkg.TypesInfo.Defs[fn.Name].(*types.Func)
					if !ok {
						t.Fatal("unresolved function in captured-loop census")
					}
					scope = strings.TrimPrefix(pkg.PkgPath, module) + "::" + capturedLoopFunctionName(resolved)
				}
				ast.Inspect(decl, func(node ast.Node) bool {
					if selector, ok := node.(*ast.SelectorExpr); ok {
						if selection := pkg.TypesInfo.Selections[selector]; selection != nil && selection.Obj().Name() == "Loop" && capturedLoopTypeName(selection.Recv()) == module+"runtime/engine.ExecutionState" {
							counts[scope+" -> ExecutionState.Loop"]++
						}
					}
					if literal, ok := node.(*ast.CompositeLit); ok && capturedLoopTypeName(pkg.TypesInfo.TypeOf(literal)) == module+"runtime/engine.ExecutionState" {
						for _, field := range literal.Elts {
							if keyed, ok := field.(*ast.KeyValueExpr); ok {
								if name, ok := keyed.Key.(*ast.Ident); ok && name.Name == "Loop" {
									counts[scope+" -> ExecutionState.Loop"]++
								}
							}
						}
					}
					id, ok := node.(*ast.Ident)
					if !ok {
						return true
					}
					fn, ok := pkg.TypesInfo.Uses[id].(*types.Func)
					if !ok || fn.Pkg() == nil {
						return true
					}
					owner := strings.TrimPrefix(fn.Pkg().Path(), module) + "::" + capturedLoopFunctionName(fn)
					switch owner {
					case "runtime/loopruntime::Activation.Context", "runtime/loopruntime::Activation.CapturedContext", "runtime/loopruntime::GenerationCurrent", "runtime/engine::ExecutionState.SetLoop", "runtime/engine::Executor.bindJoinLoopContext":
						counts[scope+" -> "+owner]++
					}
					return true
				})
			}
		}
	}
	budget := map[string]int{
		// These checks suppress stale work or prevent state mutation. They do
		// not elect an expression context from a replacement generation.
		"runtime/engine::Executor.advanceAdmittedLoop -> runtime/loopruntime::GenerationCurrent":             1,
		"runtime/engine::Executor.stepFanOutDeliveryJoin -> runtime/loopruntime::GenerationCurrent":          1,
		"runtime/engine::Executor.stepJoin -> runtime/loopruntime::GenerationCurrent":                        1,
		"runtime/pipeline::workflowLoopGenerationCurrentInBuckets -> runtime/loopruntime::GenerationCurrent": 1,
		"runtime/engine::ExecutionState.LoopBucket -> ExecutionState.Loop":                                   1,
		"runtime/engine::ExecutionState.SetLoop -> ExecutionState.Loop":                                      1,
		"runtime/engine::Executor.EvaluateFanOutOrdinal -> ExecutionState.Loop":                              1,
		"runtime/engine::Executor.currentContext -> ExecutionState.Loop":                                     1,
		"runtime/engine::Executor.newExecutionFrame -> ExecutionState.Loop":                                  1,
		"runtime/engine::evalWorkflowValueExpression -> ExecutionState.Loop":                                 1,
		// The typed result adapter passes the already-selected context through;
		// it neither loads a current activation nor reconstructs a generation.
		"runtime/engine::evalWorkflowValueResult -> ExecutionState.Loop":                                     1,
		"runtime/loopruntime::Activation.CapturedContext -> runtime/loopruntime::Activation.Context":         1,
		"runtime/loopruntime::ForkChildReference.Context -> runtime/loopruntime::Activation.CapturedContext": 1,
		"runtime/engine::Executor.stepLoop -> runtime/loopruntime::Activation.Context":                       1,
		"runtime/engine::Executor.stepLoop -> runtime/engine::ExecutionState.SetLoop":                        1,
		"runtime/engine::storeLoopActivation -> runtime/loopruntime::Activation.Context":                     1,
		"runtime/engine::storeLoopActivation -> runtime/engine::ExecutionState.SetLoop":                      1,
		"runtime/engine::Executor.bindJoinLoopContext -> runtime/loopruntime::Activation.CapturedContext":    1,
		"runtime/engine::Executor.bindJoinLoopContext -> runtime/engine::ExecutionState.SetLoop":             2,
		"runtime/engine::Executor.selectJoinOutcome -> runtime/engine::Executor.bindJoinLoopContext":         1,
		"runtime/engine::Executor.stepFanOutDeliveryJoin -> runtime/engine::Executor.bindJoinLoopContext":    1,
	}
	var violations []string
	for site, count := range counts {
		if count != budget[site] {
			violations = append(violations, fmt.Sprintf("%s: got %d, audited %d", site, count, budget[site]))
		}
		delete(budget, site)
	}
	for site := range budget {
		violations = append(violations, "missing captured-loop consumer: "+site)
	}
	sort.Strings(violations)
	return violations
}

func capturedLoopFunctionName(fn *types.Func) string {
	name := fn.Name()
	if receiver := fn.Type().(*types.Signature).Recv(); receiver != nil {
		typ := receiver.Type()
		if ptr, ok := typ.(*types.Pointer); ok {
			typ = ptr.Elem()
		}
		if named, ok := types.Unalias(typ).(*types.Named); ok {
			name = named.Obj().Name() + "." + name
		}
	}
	return name
}

func capturedLoopTypeName(typ types.Type) string {
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = ptr.Elem()
	}
	if typ == nil {
		return ""
	}
	if named, ok := types.Unalias(typ).(*types.Named); ok && named.Obj().Pkg() != nil {
		return named.Obj().Pkg().Path() + "." + named.Obj().Name()
	}
	return ""
}
