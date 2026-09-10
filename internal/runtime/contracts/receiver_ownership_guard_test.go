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

// Counts fence exact semantic functions, not files or receiver variable names.
// Executing the hostile overlays below is part of the guard's proof.
func receiverOwnerConstructorBudget() map[string]int {
	return map[string]int{
		"runtime/bus::filterDeliveryRecipientCandidates":                                       1,
		"runtime/bus::agentDeliveryRoutesForCandidates":                                        1,
		"runtime/bus::selectedRunTargetOwnerProjection.resolveRoutePlan":                       1,
		"runtime/bus::selectedRunTargetOwnerProjection.resolveSelectedRoute":                   2,
		"runtime/bus::deliveryTargetOwnershipFromDescriptor":                                   2,
		"runtime/pipeline::ClassifyDeliveryTargetOwnership":                                    4,
		"runtime/bus::selectedRunTargetOwnerProjection.withActivationPlans [descriptor]":       1,
		"runtime/bus::ActiveAgentDescriptor.TargetDescriptor [descriptor]":                     1,
		"runtime/bus::ActiveFlowInstanceDescriptor.TargetDescriptor [descriptor]":              1,
		"runtime/bus::ActiveTargetDescriptor.Normalized [descriptor]":                          1,
		"store/internal/backend/pipelinepersistence::scanSelectedRunTargetOwners [descriptor]": 1,
		// Approved #2433 fork projection preserves the three admitted target kinds;
		// it does not elect a new receiver from producer context.
		"store/internal/backend/runforkpersistence::projectRunForkFanOutExecutionOwnership": 3,
		// Ordinary agent replay consumes the fixed-revision initializer proof
		// and reconstructed receiver; it cannot request future initialization.
		"store/internal/backend/runforkpersistence::projectRunForkReplayInitializedReceiver": 1,
	}
}

func TestReceiverCompositionOwnershipGuard(t *testing.T) {
	if violations := receiverOwnerConstructorViolations(t, nil); len(violations) != 0 {
		t.Fatalf("receiver ownership construction escaped audited semantic functions: %v", violations)
	}
}

func TestReceiverCompositionOwnershipGuardHostileApprovedFile(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal/runtime/bus/target_owner_projection.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Arbitrary receiver spelling and placement alongside an
	// approved constructor must not permit source-to-target reclassification.
	hostile := append(append([]byte(nil), original...), []byte(`
func hostileReceiverReclassification(unfamiliar events.DeliveryTargetOwnership) (events.DeliveryTargetOwnership, error) {
    return events.NewExistingEntityTarget(unfamiliar.Route())
}

`)...)
	violations := receiverOwnerConstructorViolations(t, map[string][]byte{path: hostile})
	if len(violations) != 1 || !strings.Contains(violations[0], "hostileReceiverReclassification") {
		t.Fatalf("guard missed illegal constructor in approved file: %v", violations)
	}
}

func TestReceiverCompositionOwnershipGuardHostileApprovedFunction(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal/runtime/bus/target_owner_projection.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	from := "return events.NewMaterializingEntityTarget(route)"
	if strings.Count(string(original), from) != 1 {
		t.Fatal("exact descriptor conversion probe site moved")
	}
	hostile := strings.Replace(string(original), from, "_, _ = events.NewExistingEntityTarget(route)\n"+from, 1)
	violations := receiverOwnerConstructorViolations(t, map[string][]byte{path: []byte(hostile)})
	if len(violations) != 1 || !strings.Contains(violations[0], "deliveryTargetOwnershipFromDescriptor has 3 constructors; audited 2") {
		t.Fatalf("approved function admitted another owner interpretation: %v", violations)
	}
}

func TestReceiverCompositionOwnershipGuardRejectsSpeculativeDescriptor(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal/runtime/bus/target_owner_projection.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	hostile := append(append([]byte(nil), original...), []byte(`
func hostileDescriptor(unfamiliar events.DeliveryTargetOwnership) ActiveTargetDescriptor {
    return ActiveTargetDescriptor{EntityID: unfamiliar.Route().EntityID, Materializing: true}
}
`)...)
	violations := receiverOwnerConstructorViolations(t, map[string][]byte{path: hostile})
	if len(violations) != 1 || !strings.Contains(violations[0], "hostileDescriptor [descriptor]") {
		t.Fatalf("guard missed synthetic descriptor in approved file: %v", violations)
	}
}

func TestReceiverCompositionOwnershipGuardHostileFanOutProjection(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal/store/internal/backend/runforkpersistence/run_fork_fan_out_generation.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	from := "delivery.Target, err = events.NewEntitylessReceiverTarget(receiver)"
	if strings.Count(string(original), from) != 1 {
		t.Fatal("exact fan-out target projection probe site moved")
	}
	hostile := strings.Replace(string(original), from, "_, _ = events.NewExistingEntityTarget(receiver)\n"+from, 1)
	violations := receiverOwnerConstructorViolations(t, map[string][]byte{path: []byte(hostile)})
	if len(violations) != 1 || !strings.Contains(violations[0], "projectRunForkFanOutExecutionOwnership has 4 constructors; audited 3") {
		t.Fatalf("approved fan-out projection admitted another interpretation: %v", violations)
	}
}

func receiverOwnerConstructorViolations(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	root := handlerRuleIdentityGuardRepoRoot(t)
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, "./internal/runtime/core/pinrouting", "./internal/runtime/bus", "./internal/runtime/pipeline", "./internal/runtime/bootverify", "./internal/store/...")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) != 0 {
		t.Fatal("receiver owner guard requires successful compiler resolution")
	}
	counts := map[string]int{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				scope := strings.TrimPrefix(pkg.PkgPath, "github.com/division-sh/swarm/internal/") + "::<package>"
				if fn, ok := decl.(*ast.FuncDecl); ok {
					resolved, ok := pkg.TypesInfo.Defs[fn.Name].(*types.Func)
					if !ok {
						t.Fatal("function was not resolved")
					}
					scope = strings.TrimPrefix(pkg.PkgPath, "github.com/division-sh/swarm/internal/") + "::"
					if recv := resolved.Type().(*types.Signature).Recv(); recv != nil {
						typ := recv.Type()
						if ptr, ok := typ.(*types.Pointer); ok {
							typ = ptr.Elem()
						}
						named, ok := types.Unalias(typ).(*types.Named)
						if !ok {
							t.Fatal("method receiver has no semantic type")
						}
						scope += named.Obj().Name() + "."
					}
					scope += resolved.Name()
				}
				ast.Inspect(decl, func(node ast.Node) bool {
					if literal, ok := node.(*ast.CompositeLit); ok {
						if typ, ok := types.Unalias(pkg.TypesInfo.TypeOf(literal)).(*types.Named); ok && typ.Obj().Pkg() != nil && typ.Obj().Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/bus" && typ.Obj().Name() == "ActiveTargetDescriptor" {
							counts[scope+" [descriptor]"]++
						}
					}
					id, ok := node.(*ast.Ident)
					if !ok {
						return true
					}
					fn, ok := pkg.TypesInfo.Uses[id].(*types.Func)
					if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "github.com/division-sh/swarm/internal/events" || (!strings.HasPrefix(fn.Name(), "New") && !strings.HasPrefix(fn.Name(), "Must")) {
						return true
					}
					results := fn.Type().(*types.Signature).Results()
					if results.Len() == 0 {
						return true
					}
					typ, ok := types.Unalias(results.At(0).Type()).(*types.Named)
					if ok && typ.Obj().Pkg().Path() == fn.Pkg().Path() && typ.Obj().Name() == "DeliveryTargetOwnership" {
						counts[scope]++
					}
					return true
				})
			}
		}
	}
	budget := receiverOwnerConstructorBudget()
	var violations []string
	for scope, count := range counts {
		if count != budget[scope] {
			violations = append(violations, fmt.Sprintf("%s has %d constructors; audited %d", scope, count, budget[scope]))
		}
		delete(budget, scope)
	}
	for scope := range budget {
		violations = append(violations, "stale audited constructor: "+scope)
	}
	sort.Strings(violations)
	return violations
}
