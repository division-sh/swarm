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

func rewriteConformanceMutationProjectionOwner(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl) (string, bool) {
	print := func(node ast.Node) string {
		var out bytes.Buffer
		if err := format.Node(&out, fset, node); err != nil {
			panic(err)
		}
		return out.String()
	}
	old := print(fn)
	updated := ""
	switch fn.Name.Name {
	case "newEntityToolConformanceHarness":
		if print(fn.Type) != "func(t *testing.T) (context.Context, *runtimetools.Executor, *sql.DB, string)" || strings.Count(old, "_, db, _ := testutil.StartPostgres(t)") != 1 || strings.Count(old, "pg := storetest.AdmitPostgresRuntimeStore(t, db)") != 1 || strings.Count(old, "return ctx, exec, db, runID") != 1 {
			return "", false
		}
		updated = strings.Replace(old, "(context.Context, *runtimetools.Executor, *sql.DB, string)", "(context.Context, *runtimetools.Executor, operatorread.EntityReader, string)", 1)
		updated = strings.Replace(updated, "_, db, _ := testutil.StartPostgres(t)", "pg, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)", 1)
		updated = strings.Replace(updated, "pg := storetest.AdmitPostgresRuntimeStore(t, db)", "", 1)
		updated = strings.Replace(updated, "return ctx, exec, db, runID", "return ctx, exec, pg, runID", 1)
	case "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites", "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForToolWrites":
		calls, valid := 0, true
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || id.Name != "trackedMutationStateMatchesEntityState" {
				return true
			}
			object, ok := info.Uses[id].(*types.Func)
			valid = valid && ok && object.Pkg() != nil && object.Pkg().Path() == conformancePackage && len(call.Args) == 3 && print(call.Args[0]) == "db"
			calls++
			return true
		})
		if !valid || calls != 1 || strings.Count(old, "trackedMutationStateMatchesEntityState(db, runID, entityID)") != 1 {
			return "", false
		}
		updated = strings.Replace(old, "trackedMutationStateMatchesEntityState(db, runID, entityID)", "trackedMutationStateMatchesEntityState(selected, runID, entityID)", 1)
		if fn.Name.Name == "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites" {
			if strings.Count(updated, "_, db, _ := testutil.StartPostgres(t)") != 1 || strings.Count(updated, "selected := storetest.AdmitPostgresRuntimeStore(t, db)") != 1 {
				return "", false
			}
			updated = strings.Replace(updated, "_, db, _ := testutil.StartPostgres(t)", "selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)", 1)
			updated = strings.Replace(updated, "selected := storetest.AdmitPostgresRuntimeStore(t, db)", "", 1)
			updated = strings.Replace(updated, "requireMutationSurface(t, db)", "requireMutationSurface(t, selected)", 1)
		} else {
			if strings.Count(updated, "ctx, exec, db, runID := newEntityToolConformanceHarness(t)") != 1 {
				return "", false
			}
			updated = strings.Replace(updated, "ctx, exec, db, runID := newEntityToolConformanceHarness(t)", "ctx, exec, selected, runID := newEntityToolConformanceHarness(t)", 1)
			updated = strings.Replace(updated, "requireMutationSurface(t, db)", "requireMutationSurface(t, selected)", 1)
		}
	case "trackedMutationStateMatchesEntityState":
		if print(fn.Type) != "func(db *sql.DB, runID, entityID string) error" {
			return "", false
		}
		queries := map[string]bool{}
		valid, scans := true, 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name != "QueryContext" && selector.Sel.Name != "QueryRowContext" && selector.Sel.Name != "Scan" {
				return true
			}
			object, ok := info.Uses[selector.Sel].(*types.Func)
			valid = valid && ok && object.Pkg() != nil && object.Pkg().Path() == "database/sql"
			if selector.Sel.Name == "Scan" {
				scans++
				return true
			}
			if print(selector.X) != "db" || len(call.Args) != 4 || print(call.Args[0]) != "testAuthorActivityContext(context.Background())" || print(call.Args[2]) != "runID" || print(call.Args[3]) != "entityID" {
				valid = false
				return true
			}
			literal, ok := call.Args[1].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				valid = false
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			valid = valid && err == nil
			queries[strings.Join(strings.Fields(text), " ")] = true
			return true
		})
		wantQuery := `SELECT COALESCE(current_state, ''), COALESCE(fields, '{}'::jsonb), COALESCE(bookkeeping, '{}'::jsonb), COALESCE(gates, '{}'::jsonb), COALESCE(accumulator, '{}'::jsonb) FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
		wantRows := `SELECT domain, path, new_value FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid ORDER BY created_at ASC, mutation_id ASC`
		if !valid || scans != 2 || len(queries) != 2 || !queries[wantQuery] || !queries[wantRows] {
			return "", false
		}
		start, middle, end := strings.Index(old, "want := runtimemutationlog.EntityStateProjection{"), strings.Index(old, "records := make([]runtimemutationlog.ProjectionMutation, 0, 8)"), strings.Index(old, "if rowCount == 0 {")
		if start < 0 || middle <= start || end <= middle {
			return "", false
		}
		projection := old[start:middle]
		updated = `func trackedMutationStateMatchesEntityState(selected any,runID,entityID string)error{
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
 ` + old[end:]
	default:
		return "", false
	}
	formatted, err := format.Source([]byte("package fixture\n" + updated))
	if err != nil {
		panic(err)
	}
	return strings.TrimPrefix(string(formatted), "package fixture\n\n"), true
}
