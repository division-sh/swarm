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
// Current bodies compose the landed embedded-spec/nullable-model setup only.
func debtReviewedNativeSelectedImport(path, alias string, file *ast.File) bool {
	var pins map[string]string
	switch path {
	case "internal/runtime/pipeline/fork_fan_out_crash_external_test.go":
		pins = map[string]string{"TestIssue642MixedFanOutCrashRestartBothStores": "b451555fab08deea56a83f724f7cf0077d78821eee240d253b6568e93a2e19f0"}
	case "internal/runtime/pipeline/fork_fan_out_refork_external_test.go":
		pins = map[string]string{"proveMixedFanOutChildRefork": "ee5d1ddc90f8d518bd58a34dfa1ebd0462ac0f8b969c9e28167877e18d53dbe9"}
	case "internal/runtime/pipeline/fork_published_barrier_crash_external_test.go":
		pins = map[string]string{
			"publishBarrierCrashCheckpoint":       "3e68bccee6ade3f449a47c7b4fe147082dc2e03f1afb3a1366d852381ddd2fc0",
			"verifyBarrierCrashRestartBothStores": "96030b8011ea63afde9e7acdbae45a3e2b67a7d177cff5f93fd6e7200a1f1bb2",
		}
	case "internal/runtime/pipeline/fork_published_barrier_failure_external_test.go":
		pins = map[string]string{"verifyPublishedBarrierCompletionFailure": "9f11b5dea9efb5084ffe34e8054132e92abab1862d80a84a50c2d24ff6d2f516"}
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
		if err := format.Node(&canonical, token.NewFileSet(), fn); err != nil {
			return false
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(canonical.Bytes()))
		// The same selected uses retain original-process carriage under #2794/6508200313.
		if digest != pins[fn.Name.Name] && !(path == "internal/runtime/pipeline/fork_fan_out_crash_external_test.go" &&
			fn.Name.Name == "TestIssue642MixedFanOutCrashRestartBothStores" &&
			digest == "c183201c71250e318f7729b79a03aff654808fdadfd25e7597daa6220f782646") &&
			!(path == "internal/runtime/pipeline/fork_published_barrier_failure_external_test.go" &&
				fn.Name.Name == "verifyPublishedBarrierCompletionFailure" &&
				digest == "bc39f68af70f7fdda75ab91e05ccc946d3558d341e12cab46066e5a6ccfa8e62") {
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
	case "internal/runtime/runforkexecution/q1_session_fork_execution_test.go":
		return "proveSelectedQ1SessionSiblingReforkBothStores"
	}
	return ""
}

// Only the second result of each exact public factory can supply these ten
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
