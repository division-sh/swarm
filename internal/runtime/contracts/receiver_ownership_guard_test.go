package contracts

import (
	"fmt"
	"go/ast"
	"go/types"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// Counts fence exact semantic functions, not files or receiver variable names.
func receiverOwnerConstructorBudget() map[string]int {
	return map[string]int{
		// #2413 child 2: actor ownership and header dependency have distinct
		// canonical projections; the old agent reclassification is retired.
		"runtime/bus::selectedRunTargetOwnerProjection.admitAgentTarget":            3,
		"runtime/bus::selectedRunTargetOwnerProjection.agentConstructionDependency": 1,
		"runtime/bus::admitObservedAgentConstructionDependency":                     2,
		"runtime/bus::selectedRunTargetOwnerProjection.replyOriginOwner":            2,
		"runtime/pipeline::SelectDeliveryTargetOwner":                               2,
		// Approved #2433 fork projection preserves the three admitted target kinds;
		// it does not elect a new receiver from producer context.
		"store/internal/backend/runforkpersistence::projectRunForkFanOutExecutionOwnership": 3,
		// Ordinary agent replay consumes the fixed-revision initializer proof
		// and reconstructed receiver; it cannot request future initialization.
		"store/internal/backend/runforkpersistence::projectRunForkReplayInitializedReceiver": 1,
		// Fixed-cut replay projects captured actor ownership separately from construction (2617:6177786309).
		"store/internal/backend/runforkpersistence::projectRunForkReplayActorTarget": 2,
		// #642 6190024778: admitted join continuation projects only its exact
		// child command's receiver and construction context after admission.
		"store/internal/backend/runforkpersistence::projectRunForkPublishedJoinRoute": 1,
		// Fixed-cut unfinished-recipient selection and materialization validation
		// precede these two captured entityless/entity-bearing projections; the
		// latter checks template/path before ProjectEntityOwnership.
		"store/internal/backend/runforkpersistence::projectSelectedFanOutNodeReceiver": 2,
	}
}

func TestReceiverCompositionOwnershipGuard(t *testing.T) {
	if violations := receiverOwnerConstructorViolations(t); len(violations) != 0 {
		t.Fatalf("receiver ownership construction escaped audited semantic functions: %v", violations)
	}
}

func receiverOwnerConstructorViolations(t *testing.T) []string {
	t.Helper()
	root := handlerRuleIdentityGuardRepoRoot(t)
	pkgs, err := packages.Load(&packages.Config{
		Dir:  root,
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
