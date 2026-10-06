package main

import (
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"strconv"
	"strings"
)

func rewriteConformanceMutationProjectionOwner(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl) (string, bool) {
	if fn.Body == nil {
		return "", false
	}
	old := goNodeString(fset, fn)
	var updated string
	var matched bool
	switch fn.Name.Name {
	case "newEntityToolConformanceHarness":
		updated, matched = rewriteEntityToolProjectionHarness(fset, fn, old)
	case "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites", "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForToolWrites":
		updated, matched = rewriteTrackedProjectionCaller(fset, info, fn, old)
	case "trackedMutationStateMatchesEntityState":
		updated, matched = rewriteTrackedProjectionReader(fset, info, fn, old)
	default:
		return "", false
	}
	if !matched {
		return "", false
	}
	formatted, err := format.Source([]byte("package fixture\n" + updated))
	if err != nil {
		panic(err)
	}
	return strings.TrimPrefix(string(formatted), "package fixture\n\n"), true
}

func rewriteEntityToolProjectionHarness(fset *token.FileSet, fn *ast.FuncDecl, old string) (string, bool) {
	if goNodeString(fset, fn.Type) != "func(t *testing.T) (context.Context, *runtimetools.Executor, *sql.DB, string)" ||
		strings.Count(old, "_, db, _ := testutil.StartPostgres(t)") != 1 ||
		strings.Count(old, "pg := storetest.AdmitPostgresRuntimeStore(t, db)") != 1 ||
		strings.Count(old, "return ctx, exec, db, runID") != 1 {
		return "", false
	}
	updated := strings.Replace(old, "(context.Context, *runtimetools.Executor, *sql.DB, string)", "(context.Context, *runtimetools.Executor, operatorread.EntityReader, string)", 1)
	updated = strings.Replace(updated, "_, db, _ := testutil.StartPostgres(t)", "pg, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)", 1)
	updated = strings.Replace(updated, "pg := storetest.AdmitPostgresRuntimeStore(t, db)", "", 1)
	return strings.Replace(updated, "return ctx, exec, db, runID", "return ctx, exec, pg, runID", 1), true
}

func trackedProjectionCallerIsExact(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl) bool {
	count, valid := 0, true
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || id.Name != "trackedMutationStateMatchesEntityState" {
			return true
		}
		valid = valid && resolvedPackageFunction(info, id, conformancePackage, "trackedMutationStateMatchesEntityState") &&
			len(call.Args) == 3 && goNodeString(fset, call.Args[0]) == "db"
		count++
		return true
	})
	return valid && count == 1
}

func rewriteTrackedProjectionCaller(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, old string) (string, bool) {
	const before = "trackedMutationStateMatchesEntityState(db, runID, entityID)"
	if !trackedProjectionCallerIsExact(fset, info, fn) || strings.Count(old, before) != 1 {
		return "", false
	}
	updated := strings.Replace(old, before, "trackedMutationStateMatchesEntityState(selected, runID, entityID)", 1)
	if fn.Name.Name == "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites" {
		if strings.Count(updated, "_, db, _ := testutil.StartPostgres(t)") != 1 ||
			strings.Count(updated, "selected := storetest.AdmitPostgresRuntimeStore(t, db)") != 1 {
			return "", false
		}
		updated = strings.Replace(updated, "_, db, _ := testutil.StartPostgres(t)", "selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)", 1)
		updated = strings.Replace(updated, "selected := storetest.AdmitPostgresRuntimeStore(t, db)", "", 1)
	} else {
		const harness = "ctx, exec, db, runID := newEntityToolConformanceHarness(t)"
		if strings.Count(updated, harness) != 1 {
			return "", false
		}
		updated = strings.Replace(updated, harness, "ctx, exec, selected, runID := newEntityToolConformanceHarness(t)", 1)
	}
	return strings.Replace(updated, "requireMutationSurface(t, db)", "requireMutationSurface(t, selected)", 1), true
}

type trackedProjectionQueries struct {
	queries map[string]bool
	valid   bool
	scans   int
}

func (seen *trackedProjectionQueries) observe(fset *token.FileSet, info *types.Info, call *ast.CallExpr) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
	switch selector.Sel.Name {
	case "QueryContext", "QueryRowContext", "Scan":
	default:
		return
	}
	object, ok := info.Uses[selector.Sel].(*types.Func)
	if !ok || object.Pkg() == nil || object.Pkg().Path() != "database/sql" {
		seen.valid = false
	}
	if selector.Sel.Name == "Scan" {
		seen.scans++
		return
	}
	text, ok := exactTrackedProjectionQuery(fset, selector, call)
	if !ok {
		seen.valid = false
		return
	}
	seen.queries[text] = true
}

func exactTrackedProjectionQuery(fset *token.FileSet, selector *ast.SelectorExpr, call *ast.CallExpr) (string, bool) {
	if goNodeString(fset, selector.X) != "db" || len(call.Args) != 4 {
		return "", false
	}
	if goNodeString(fset, call.Args[0]) != "testAuthorActivityContext(context.Background())" ||
		goNodeString(fset, call.Args[2]) != "runID" || goNodeString(fset, call.Args[3]) != "entityID" {
		return "", false
	}
	literal, ok := call.Args[1].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	text, err := strconv.Unquote(literal.Value)
	return strings.Join(strings.Fields(text), " "), err == nil
}

func trackedProjectionQueriesAreExact(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl) bool {
	seen := trackedProjectionQueries{queries: map[string]bool{}, valid: true}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			seen.observe(fset, info, call)
		}
		return true
	})
	const projection = "SELECT COALESCE(current_state, ''), COALESCE(fields, '{}'::jsonb), COALESCE(bookkeeping, '{}'::jsonb), COALESCE(gates, '{}'::jsonb), COALESCE(accumulator, '{}'::jsonb) FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid"
	const mutations = "SELECT domain, path, new_value FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid ORDER BY created_at ASC, mutation_id ASC"
	return seen.valid && seen.scans == 2 && len(seen.queries) == 2 && seen.queries[projection] && seen.queries[mutations]
}

func rewriteTrackedProjectionReader(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, old string) (string, bool) {
	if goNodeString(fset, fn.Type) != "func(db *sql.DB, runID, entityID string) error" || !trackedProjectionQueriesAreExact(fset, info, fn) {
		return "", false
	}
	start, middle, end := strings.Index(old, "want := runtimemutationlog.EntityStateProjection{"), strings.Index(old, "records := make([]runtimemutationlog.ProjectionMutation, 0, 8)"), strings.Index(old, "if rowCount == 0 {")
	if start < 0 || middle <= start || end <= middle {
		return "", false
	}
	projection := old[start:middle]
	return `func trackedMutationStateMatchesEntityState(selected any,runID,entityID string)error{
 evidence,readErr:=storetest.ReadTrackedEntityMutationProjectionStorage(testAuthorActivityContext(context.Background()),selected,runID,entityID)
 if readErr!=nil{return readErr}
 currentState,fieldsRaw,bookRaw,gatesRaw,accRaw:=evidence.CurrentState,evidence.Fields,evidence.Bookkeeping,evidence.Gates,evidence.Accumulator
 ` + projection + `
 records:=make([]runtimemutationlog.ProjectionMutation,0,8)
 rowCount:=len(evidence.Mutations)
 for _,mutation:=range evidence.Mutations{
 value,err:=decodeJSONValueErr(mutation.NewValue);if err!=nil{return fmt.Errorf("decode mutation value: %w",err)}
 records=append(records,runtimemutationlog.ProjectionMutation{Domain:runtimemutationlog.Domain(strings.TrimSpace(mutation.Domain)),Path:strings.TrimSpace(mutation.Path),NewValue:value})
 }
 ` + old[end:], true
}
