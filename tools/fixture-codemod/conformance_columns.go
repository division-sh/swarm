package main

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"strconv"
	"strings"
)

func rewriteConformanceColumnOwner(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl) (string, bool) {
	var api, parameters, context, selected string
	var expected [][]string
	bootstrap := true
	switch fn.Name.Name {
	case "requireCanonicalConversationSurface":
		api, expected = "CheckConversationStorageColumns", [][]string{{"agent_turns", "turn_id", "turn_blocks"}, {"agent_conversation_audits", "session_id"}}
	case "requireCanonicalRuntimeLogSurface":
		api, expected = "CheckRuntimeLogStorageColumns", [][]string{{"events", "event_id", "event_name", "payload", "scope", "created_at"}}
	case "requireMutationSurface":
		api, expected, bootstrap = "CheckMutationStorageColumns", [][]string{{"entity_state", "entity_id", "current_state", "fields", "bookkeeping", "gates", "accumulator"}, {"entity_mutations", "entity_id", "domain", "path", "old_value", "new_value", "writer_type", "writer_id", "handler_step", "created_at"}}, false
	case "requireCanonicalDeliveryLifecycleSurface":
		api, expected = "CheckDeliveryLifecycleStorageColumns", [][]string{{"event_deliveries", "delivery_id", "event_id", "route_identity", "subscriber_type", "subscriber_id", "status", "retry_count", "max_retries", "claim_version", "current_attempt_version", "current_attempt_open", "settled_at"}, {"event_delivery_attempts", "delivery_id", "claim_version", "claim_token", "started_at", "lease_expires_at", "current_delivery_id", "active_session_id", "session_delivery_id", "session_run_id", "session_subscriber_type", "session_agent_id", "open_marker", "closure_kind", "outcome", "side_effects", "duration_ms", "completed_at"}}
	default:
		return "", false
	}
	print := func(n ast.Node) string {
		var out bytes.Buffer
		if err := format.Node(&out, fset, n); err != nil {
			panic(err)
		}
		return out.String()
	}
	parameters, context, selected = "t *testing.T, ctx context.Context, pg *store.PostgresStore", "ctx", "pg"
	prefix := 2
	if !bootstrap {
		parameters, context, selected = "t *testing.T, db *sql.DB", "testAuthorActivityContext(context.Background())", "db"
		prefix = 1
	}
	if fn.Body == nil || print(fn.Type) != "func("+parameters+")" || len(fn.Body.List) != prefix+len(expected) || print(fn.Body.List[0]) != "t.Helper()" {
		return "", false
	}
	valid := true
	if bootstrap {
		if print(fn.Body.List[1]) != "storetest.BootstrapPostgresRuntimeStore(t, pg)" {
			return "", false
		}
		ast.Inspect(fn.Body.List[1], func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "BootstrapPostgresRuntimeStore" {
				object, ok := info.Uses[selector.Sel].(*types.Func)
				valid = valid && ok && object.Pkg() != nil && object.Pkg().Path() == storetestPackage
			}
			return true
		})
	}
	for index, want := range expected {
		statement, ok := fn.Body.List[prefix+index].(*ast.ExprStmt)
		if !ok {
			return "", false
		}
		call, ok := statement.X.(*ast.CallExpr)
		if !ok || len(call.Args) != 3+len(want) || call.Ellipsis.IsValid() {
			return "", false
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || id.Name != "requireTableColumns" {
			return "", false
		}
		object, ok := info.Uses[id].(*types.Func)
		valid = valid && ok && object.Pkg() != nil && object.Pkg().Path() == conformancePackage
		if print(call.Args[0]) != "t" || print(call.Args[1]) != context {
			return "", false
		}
		receiver := selected
		if bootstrap {
			receiver = "storetest.DatabaseForTest(pg)"
		}
		if print(call.Args[2]) != receiver {
			return "", false
		}
		if bootstrap {
			ast.Inspect(call.Args[2], func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "DatabaseForTest" {
					object, ok := info.Uses[selector.Sel].(*types.Func)
					valid = valid && ok && object.Pkg() != nil && object.Pkg().Path() == storetestPackage
				}
				return true
			})
		}
		for i, text := range want {
			literal, ok := call.Args[3+i].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return "", false
			}
			got, err := strconv.Unquote(literal.Value)
			if err != nil || got != text {
				return "", false
			}
		}
	}
	if !valid {
		return "", false
	}
	if !bootstrap {
		parameters, selected = "t *testing.T, selected any", "selected"
	}
	updated := "func " + fn.Name.Name + "(" + parameters + ") { t.Helper(); if err:=storetest." + api + "(" + context + ", " + selected + ");err!=nil{t.Fatal(err)} }"
	formatted, err := format.Source([]byte("package fixture\n" + updated))
	if err != nil {
		panic(err)
	}
	return strings.TrimPrefix(string(formatted), "package fixture\n\n"), true
}

func matchConformanceMutationColumnOwner(info *types.Info, fn *ast.FuncDecl, call *ast.CallExpr) (string, bool) {
	id, ok := call.Fun.(*ast.Ident)
	if !ok || id.Name != "requireMutationSurface" || len(call.Args) != 2 || call.Ellipsis.IsValid() {
		return "", false
	}
	object, ok := info.Uses[id].(*types.Func)
	if !ok || object.Pkg() == nil || object.Pkg().Path() != conformancePackage {
		return "", false
	}
	argument, ok := call.Args[1].(*ast.Ident)
	if !ok || argument.Name != "db" {
		return "", false
	}
	pointer, ok := types.Unalias(info.TypeOf(argument)).(*types.Pointer)
	if !ok {
		return "", false
	}
	named, ok := types.Unalias(pointer.Elem()).(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "database/sql" || named.Obj().Name() != "DB" {
		return "", false
	}
	selected := ""
	switch fn.Name.Name {
	case "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites", "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForToolWrites":
		selected = "selected"
	case "TestCanonicalMutationSurface_FailsOnMalformedCanonicalMutationField":
		selected = "pg"
	default:
		return "", false
	}
	var nearest *types.Scope
	for _, scope := range info.Scopes {
		if scope.Contains(call.Pos()) && (nearest == nil || nearest.Pos() <= scope.Pos() && scope.End() <= nearest.End()) {
			nearest = scope
		}
	}
	if nearest == nil {
		return "", false
	}
	_, owner := nearest.LookupParent(selected, call.Pos())
	if owner == nil {
		return "", false
	}
	ownerPointer, ok := types.Unalias(owner.Type()).(*types.Pointer)
	if !ok {
		return "", false
	}
	ownerNamed, ok := types.Unalias(ownerPointer.Elem()).(*types.Named)
	if !ok || ownerNamed.Obj().Pkg() == nil || ownerNamed.Obj().Pkg().Path() != "github.com/division-sh/swarm/internal/store/internal/runtimepersistence" || ownerNamed.Obj().Name() != "PostgresStore" {
		return "", false
	}
	return selected, true
}
