package main

import (
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"strconv"
	"strings"
)

type conformanceColumnRewrite struct {
	api       string
	inventory [][]string
	bootstrap bool
}

func conformanceColumnRule(name string) (conformanceColumnRewrite, bool) {
	var rule conformanceColumnRewrite
	rule.bootstrap = true
	switch name {
	case "requireCanonicalConversationSurface":
		rule.api, rule.inventory = "CheckConversationStorageColumns", [][]string{{"agent_turns", "turn_id", "turn_blocks"}, {"agent_conversation_audits", "session_id"}}
	case "requireCanonicalRuntimeLogSurface":
		rule.api, rule.inventory = "CheckRuntimeLogStorageColumns", [][]string{{"events", "event_id", "event_name", "payload", "scope", "created_at"}}
	case "requireMutationSurface":
		rule.api, rule.inventory, rule.bootstrap = "CheckMutationStorageColumns", [][]string{{"entity_state", "entity_id", "current_state", "fields", "bookkeeping", "gates", "accumulator"}, {"entity_mutations", "entity_id", "domain", "path", "old_value", "new_value", "writer_type", "writer_id", "handler_step", "created_at"}}, false
	case "requireCanonicalDeliveryLifecycleSurface":
		rule.api, rule.inventory = "CheckDeliveryLifecycleStorageColumns", [][]string{{"event_deliveries", "delivery_id", "event_id", "route_identity", "subscriber_type", "subscriber_id", "status", "retry_count", "max_retries", "claim_version", "current_attempt_version", "current_attempt_open", "settled_at"}, {"event_delivery_attempts", "delivery_id", "claim_version", "claim_token", "started_at", "lease_expires_at", "current_delivery_id", "active_session_id", "session_delivery_id", "session_run_id", "session_subscriber_type", "session_agent_id", "open_marker", "closure_kind", "outcome", "side_effects", "duration_ms", "completed_at"}}
	default:
		return rule, false
	}
	return rule, true
}

func rewriteConformanceColumnOwner(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl) (string, bool) {
	rule, ok := conformanceColumnRule(fn.Name.Name)
	if !ok || !matchConformanceColumnBody(fset, info, fn, rule) {
		return "", false
	}
	parameters, context, owner := "t *testing.T, ctx context.Context, pg *store.PostgresStore", "ctx", "pg"
	if !rule.bootstrap {
		parameters, context, owner = "t *testing.T, selected any", "testAuthorActivityContext(context.Background())", "selected"
	}
	updated := "func " + fn.Name.Name + "(" + parameters + ") {t.Helper();if err:=storetest." + rule.api + "(" + context + ", " + owner + ");err!=nil{t.Fatal(err)}}"
	formatted, err := format.Source([]byte("package fixture\n" + updated))
	if err != nil {
		panic(err)
	}
	return strings.TrimPrefix(string(formatted), "package fixture\n\n"), true
}

func matchConformanceColumnBody(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, rule conformanceColumnRewrite) bool {
	parameters, prefix := "t *testing.T, ctx context.Context, pg *store.PostgresStore", 2
	if !rule.bootstrap {
		parameters, prefix = "t *testing.T, db *sql.DB", 1
	}
	if fn.Body == nil || goNodeString(fset, fn.Type) != "func("+parameters+")" || len(fn.Body.List) != prefix+len(rule.inventory) {
		return false
	}
	if goNodeString(fset, fn.Body.List[0]) != "t.Helper()" {
		return false
	}
	if rule.bootstrap && !matchConformanceBootstrap(fset, info, fn.Body.List[1]) {
		return false
	}
	for index, want := range rule.inventory {
		if !matchConformanceColumnStatement(fset, info, fn.Body.List[prefix+index], rule.bootstrap, want) {
			return false
		}
	}
	return true
}

func matchConformanceBootstrap(fset *token.FileSet, info *types.Info, statement ast.Stmt) bool {
	expr, ok := statement.(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := expr.X.(*ast.CallExpr)
	return ok && goNodeString(fset, statement) == "storetest.BootstrapPostgresRuntimeStore(t, pg)" &&
		resolvedPackageFunction(info, call.Fun, storetestPackage, "BootstrapPostgresRuntimeStore")
}

func matchConformanceColumnStatement(fset *token.FileSet, info *types.Info, statement ast.Stmt, bootstrap bool, want []string) bool {
	expr, ok := statement.(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := expr.X.(*ast.CallExpr)
	if !ok || len(call.Args) != 3+len(want) || call.Ellipsis.IsValid() {
		return false
	}
	id, ok := call.Fun.(*ast.Ident)
	if !ok || id.Name != "requireTableColumns" || !resolvedPackageFunction(info, id, conformancePackage, "requireTableColumns") {
		return false
	}
	context, owner := "testAuthorActivityContext(context.Background())", "db"
	if bootstrap {
		context, owner = "ctx", "storetest.DatabaseForTest(pg)"
	}
	if goNodeString(fset, call.Args[0]) != "t" || goNodeString(fset, call.Args[1]) != context || goNodeString(fset, call.Args[2]) != owner {
		return false
	}
	if bootstrap {
		getter, ok := call.Args[2].(*ast.CallExpr)
		if !ok || !resolvedPackageFunction(info, getter.Fun, storetestPackage, "DatabaseForTest") {
			return false
		}
	}
	return exactStringArguments(call.Args[3:], want)
}

func resolvedPackageFunction(info *types.Info, expression ast.Expr, pkg, name string) bool {
	var id *ast.Ident
	switch value := expression.(type) {
	case *ast.Ident:
		id = value
	case *ast.SelectorExpr:
		id = value.Sel
	default:
		return false
	}
	object, ok := info.Uses[id].(*types.Func)
	return ok && object.Pkg() != nil && object.Pkg().Path() == pkg && object.Name() == name
}

func exactStringArguments(args []ast.Expr, want []string) bool {
	if len(args) != len(want) {
		return false
	}
	for i, arg := range args {
		literal, ok := arg.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return false
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil || text != want[i] {
			return false
		}
	}
	return true
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
