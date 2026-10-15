package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var projectionShapeMethods = []string{"FieldsArray", "NumericGate", "AccumulatorArray", "MalformedTransitionHistory", "ConflictingInstanceID", "SlashOnlyFlowPath"}

func projectionShapeRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family == "native-projection-shapes" {
			return row
		}
	}
	t.Fatal("missing projection shape cohort")
	return recipe{}
}

func projectionShapeFunction(t *testing.T, source string) *ast.FuncDecl {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "shape.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	return file.Decls[0].(*ast.FuncDecl)
}

func projectionShapeCases(t *testing.T, fn *ast.FuncDecl) []map[string]ast.Expr {
	t.Helper()
	literal := fn.Body.List[0].(*ast.AssignStmt).Rhs[0].(*ast.CompositeLit)
	var out []map[string]ast.Expr
	for _, entry := range literal.Elts {
		row := map[string]ast.Expr{}
		for _, field := range entry.(*ast.CompositeLit).Elts {
			pair := field.(*ast.KeyValueExpr)
			row[pair.Key.(*ast.Ident).Name] = pair.Value
		}
		out = append(out, row)
	}
	return out
}
func projectionShapeString(t *testing.T, expr ast.Expr) string {
	t.Helper()
	value, err := strconv.Unquote(expr.(*ast.BasicLit).Value)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestNativeProjectionShapeRecipesPreserveSixFaultsAndEveryReadRefusal(t *testing.T) {
	row := projectionShapeRecipe(t)
	before, after := projectionShapeFunction(t, row.Before), projectionShapeFunction(t, row.After)
	oldCases, newCases := projectionShapeCases(t, before), projectionShapeCases(t, after)
	if len(oldCases) != 6 || len(newCases) != 6 {
		t.Fatal("original six malformed cases changed")
	}
	backendSource, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/store/internal/backend/pipelinepersistence/workflow_projection_shape_fault.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "fault.go", backendSource, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	backendFunctions := map[string]*ast.FuncDecl{}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			backendFunctions[fn.Name.Name] = fn
		}
	}
	for i, old := range oldCases {
		next := newCases[i]
		for _, key := range []string{"name", "mutateKey", "wantContains"} {
			if !reflect.DeepEqual(formattedNativeReadNode(old[key]), formattedNativeReadNode(next[key])) {
				t.Fatalf("fault %d changed %s", i, key)
			}
		}
		if projectionShapeString(t, old["wantContains"]) == "" {
			t.Fatal("cannot remove reachable successful-read branch")
		}
		mutate := next["mutate"].(*ast.FuncLit).Body.List[0].(*ast.ReturnStmt).Results[0].(*ast.CallExpr)
		if formattedNativeReadNode(mutate) != "fixture."+projectionShapeMethods[i]+"(ctx, testPipelineRunID, key)" {
			t.Fatalf("fault coordinate or named cut changed: %s", formattedNativeReadNode(mutate))
		}
		query := projectionShapeString(t, old["mutateSQL"])
		value := projectionShapeString(t, old["mutateArg"])
		want := strings.ReplaceAll(query, "$2::jsonb", "'"+value+"'")
		want = strings.ReplaceAll(want, "$1::uuid", "$1")
		want = strings.ReplaceAll(want, "$3::uuid", "$2")
		want = strings.ReplaceAll(want, "WHERE entity_id = $1 AND run_id = $2", "WHERE run_id = $2 AND entity_id = $1")
		want = strings.ReplaceAll(want, "WHERE instance_path = $1 AND run_id = $2", "WHERE run_id = $2 AND instance_path = $1")
		fn := backendFunctions["SetWorkflowProjection"+projectionShapeMethods[i]+"ForTest"]
		apply := fn.Body.List[0].(*ast.ReturnStmt).Results[0].(*ast.CallExpr)
		exec := apply.Args[0].(*ast.CallExpr)
		if projectionShapeString(t, exec.Args[1]) != want || formattedNativeReadNode(exec.Fun) != "tx.ExecContext" || len(exec.Args) != 4 || formattedNativeReadNode(exec.Args[0]) != "ctx" || formattedNativeReadNode(exec.Args[2]) != "key" || formattedNativeReadNode(exec.Args[3]) != "run" {
			t.Fatalf("original payload or physical scope changed for %s", projectionShapeMethods[i])
		}
	}
	if projectionShapeWorkload(t, row.Before, true) != projectionShapeWorkload(t, row.After, false) {
		t.Fatal("malformed read workload or assertion changed")
	}
}

func projectionShapeWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	loop := fn.Body.List[1].(*ast.RangeStmt)
	run := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
	body := run.Args[1].(*ast.FuncLit).Body
	setup := 2
	if predecessor {
		setup = 3
	}
	body.List = body.List[setup:]
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if formattedNativeReadNode(call.Fun) == "store.upsert" {
			call.Fun, _ = parser.ParseExpr("fixture.Construct")
			call.Args[0], _ = parser.ParseExpr("ctx")
		}
		return true
	})
	if predecessor {
		var normalized []ast.Stmt
		for _, statement := range body.List {
			text := formattedNativeReadNode(statement)
			switch {
			case strings.HasPrefix(text, "result, err := db.ExecContext("):
				if text != "result, err := db.ExecContext(testAuthorActivityContext(t, context.Background()), tc.mutateSQL, mutateID, tc.mutateArg, testPipelineRunID)" {
					t.Fatal("original mutation coordinates changed")
				}
				statement = projectionShapeStatement(t, "changed, err := tc.mutate(fixture, ctx, mutateID)")
			case strings.HasPrefix(text, "if changed, err := result.RowsAffected();"):
				if statement.(*ast.IfStmt).Cond == nil || formattedNativeReadNode(statement.(*ast.IfStmt).Cond) != "err != nil || changed != 1" {
					t.Fatal("original exact-row guard changed")
				}
				statement = projectionShapeStatement(t, `if changed != 1 {t.Fatalf("mutate exact persisted authority: rows=%d err=%v",changed,err)}`)
			case strings.HasPrefix(text, "loaded, ok, err := store.Load("):
				if text != "loaded, ok, err := store.Load(testWorkflowStoreRunContext(t, store), testRunScopedWorkflowInstance(storageRef))" {
					t.Fatal("original malformed read identity changed")
				}
				statement = projectionShapeStatement(t, "_, _, err = fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance(storageRef))")
			case strings.HasPrefix(text, `if tc.wantContains == "" {`):
				continue
			}
			normalized = append(normalized, statement)
		}
		body.List = normalized
	}
	return formattedNativeReadNode(body)
}
func projectionShapeStatement(t *testing.T, source string) ast.Stmt {
	t.Helper()
	return projectionShapeFunction(t, "func statement(){"+source+"}").Body.List[0]
}

func TestNativeProjectionShapeOracleRejectsWeakenedErrorAndRowAssertions(t *testing.T) {
	row := projectionShapeRecipe(t)
	for _, condition := range []string{"if changed != 1", "if err == nil", "if !strings.Contains(err.Error(), tc.wantContains)"} {
		if !strings.Contains(row.After, condition) {
			t.Fatal("required assertion missing")
		}
		changed := strings.Replace(row.After, condition, "if false", 1)
		if projectionShapeWorkload(t, row.Before, true) == projectionShapeWorkload(t, changed, false) {
			t.Fatalf("weakened assertion accepted: %s", condition)
		}
	}
}

func TestNativeProjectionShapeDiagnosticOwnerSuccessorPreservesExactHistory(t *testing.T) {
	row := projectionShapeRecipe(t)
	if row.File != "internal/runtime/pipeline/workflow_instance_store_projection_test.go" || row.Function != "TestWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapes" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "7c04e6b699650edc874d8c5e1b438aa2d2071d16be0a92e0a3fc1f3377cc2c69" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "62e03290fee8388610495540f2a57f3fa99c26c5790370fa15f1d4d3fa0775b6" {
		t.Fatal("diagnostic successor changed its historical source or owner")
	}
	want := row.After
	for _, field := range []string{"gates", "accumulator"} {
		prior := "entity_state." + field
		if strings.Count(want, prior) != 1 {
			t.Fatalf("historical diagnostic multiplicity changed: %s", field)
		}
		want = strings.Replace(want, prior, "flow_instances."+field, 1)
	}
	if row.Successor != want {
		t.Fatal("diagnostic successor changed more than the two exact owner labels")
	}
	if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("current diagnostic successor is not inert: %v", err)
	}
	for _, cut := range []string{"if changed != 1", "if err == nil", "if !strings.Contains(err.Error(), tc.wantContains)", "flow_instances.gates", "flow_instances.accumulator"} {
		mutant := strings.Replace(row.Successor, cut, "foreignCut", 1)
		if mutant == row.Successor {
			t.Fatalf("missing diagnostic/assertion hostility cut: %s", cut)
		}
		if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+mutant), row); err == nil && !changed {
			t.Fatalf("weakened diagnostic/assertion successor accepted: %s", cut)
		}
	}
}
