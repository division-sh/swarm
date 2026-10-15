package store_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
)

// #642/6190983544: these imports consume the reviewed selected construction
// and execution ports. Every selected-qualified use must retain its exact body.
func debtReviewedNativeSelectedImport(path, alias string, file *ast.File) bool {
	var pins map[string]string
	switch path {
	case "internal/runtime/pipeline/fork_fan_out_crash_external_test.go":
		pins = map[string]string{"TestIssue642MixedFanOutCrashRestartBothStores": "3b09d43a4504650fddd1759cbc936094b8ec5267dd0484f4f91848630496e821"}
	case "internal/runtime/pipeline/fork_fan_out_refork_external_test.go":
		pins = map[string]string{"proveMixedFanOutChildRefork": "69b01db6be47fbd7a0e940eee058a80e50bfddf61117dea284eeb992f87c2776"}
	case "internal/runtime/pipeline/fork_published_barrier_crash_external_test.go":
		pins = map[string]string{
			"publishBarrierCrashCheckpoint":       "4154a9d434713083c9082d47d11d3b996586046f7fdbf3f6f1103bda0882345c",
			"verifyBarrierCrashRestartBothStores": "6a6c8593e82984d18640dd41af36ee4bf8b86c87941a5fab094feb424542ad47",
		}
	case "internal/runtime/pipeline/fork_published_barrier_failure_external_test.go":
		pins = map[string]string{"verifyPublishedBarrierCompletionFailure": "9671e34719bb8d35ea395c00b69ae49bd6510698187aa2dfdc64ac48247e35fe"}
	default:
		return false
	}
	seen := map[string]bool{}
	for _, decl := range file.Decls {
		used := false
		ast.Inspect(decl, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok {
				qualifier, ok := selector.X.(*ast.Ident)
				used = used || ok && qualifier.Name == alias
			}
			return true
		})
		if !used {
			continue
		}
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || seen[fn.Name.Name] || pins[fn.Name.Name] == "" {
			return false
		}
		var canonical bytes.Buffer
		if err := format.Node(&canonical, token.NewFileSet(), fn); err != nil ||
			fmt.Sprintf("%x", sha256.Sum256(canonical.Bytes())) != pins[fn.Name.Name] {
			return false
		}
		seen[fn.Name.Name] = true
	}
	return len(seen) == len(pins)
}

func debtReviewedNativeReopenDeclaration(path string) string {
	switch path {
	case "internal/runtime/runforkexecution/activity_result_recovery_process_test.go":
		return "proveSelectedActivityResultCrashRecovery"
	case "internal/runtime/runforkexecution/nested_reply_deadline_execution_test.go":
		return "proveSelectedNestedDeadlines"
	case "internal/runtime/runforkexecution/one_shot_reply_execution_test.go":
		return "TestIssue642SupportedOneShotReturnsOwnChildReplyBothStores"
	case "internal/runtime/runforkexecution/pending_effect_reply_recovery_test.go":
		return "TestIssue642FreshPendingEffectBeforeJournalSIGKILLRecoveryBothStores"
	}
	return ""
}

// Only the second result of each exact public factory can supply these eight
// zero-argument opens. Inspect resolved objects so unrelated/shadowed names do
// not gain authority and the factory closure cannot be reassigned or escape.
func debtReviewedNativeReopenCalls(path string, file *ast.File, info *types.Info) map[token.Pos]bool {
	approved := map[token.Pos]bool{}
	declaration := debtReviewedNativeReopenDeclaration(path)
	if declaration == "" {
		return approved
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != declaration {
			continue
		}
		type binding struct {
			factory string
			closure types.Object
		}
		var bindings []binding
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 2 || len(assignment.Rhs) != 1 {
				return true
			}
			call, ok := assignment.Rhs[0].(*ast.CallExpr)
			if !ok {
				return true
			}
			name := debtAuthorityCallName(call.Fun, info)
			if name != "github.com/division-sh/swarm/internal/store/storetest.StartSQLiteRuntimeStoreWithReopen" &&
				name != "github.com/division-sh/swarm/internal/store/storetest.StartPostgresRuntimeStoreWithReopen" {
				return true
			}
			var factory types.Object
			switch target := call.Fun.(type) {
			case *ast.Ident:
				factory = info.Uses[target]
			case *ast.SelectorExpr:
				factory = info.Uses[target.Sel]
			}
			function, ok := factory.(*types.Func)
			if !ok {
				return true
			}
			signature := function.Type().(*types.Signature)
			if signature.Results().Len() != 2 {
				return true
			}
			open, ok := signature.Results().At(1).Type().Underlying().(*types.Signature)
			if !ok || open.Params().Len() != 0 || open.Variadic() || open.Results().Len() != 1 ||
				!types.Identical(open.Results().At(0).Type(), signature.Results().At(0).Type()) {
				return true
			}
			closure, ok := assignment.Lhs[1].(*ast.Ident)
			if ok && closure.Name != "_" && info.Defs[closure] != nil {
				bindings = append(bindings, binding{name, info.Defs[closure]})
			}
			return true
		})
		if len(bindings) != 2 || bindings[0].factory == bindings[1].factory {
			return approved
		}
		for _, binding := range bindings {
			var call *ast.CallExpr
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				invocation, ok := node.(*ast.CallExpr)
				if ok {
					name, ok := invocation.Fun.(*ast.Ident)
					if ok && info.Uses[name] == binding.closure && len(invocation.Args) == 0 {
						call = invocation
					}
				}
				return true
			})
			if call == nil {
				continue
			}
			uses := 0
			for name, object := range info.Uses {
				if object == binding.closure {
					uses++
					if name != call.Fun {
						uses++ // Reassignment, forwarding or another invocation.
					}
				}
			}
			if uses == 1 {
				approved[call.Pos()] = true
			}
		}
	}
	return approved
}
