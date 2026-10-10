package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func projectionHeaderRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family == "native-projection-headers" {
			return row
		}
	}
	t.Fatal("missing constructed-header cohort")
	return recipe{}
}
func TestNativeProjectionHeaderRecipePreservesListAndBothShadowDialects(t *testing.T) {
	row := projectionHeaderRecipe(t)
	if projectionHeaderWorkload(t, row.Before, true) != projectionHeaderWorkload(t, row.After, false) {
		t.Fatal("constructed-header list workload or assertion changed")
	}
	old := projectionShapeFunction(t, row.Before)
	backendLoop := old.Body.List[0].(*ast.RangeStmt)
	if formattedNativeReadNode(backendLoop.X) != `[]string{"sqlite", "postgres"}` {
		t.Fatal("original supported-store cells changed")
	}
	var queries []string
	ast.Inspect(old, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || formattedNativeReadNode(assign.Lhs[0]) != "query" {
			return true
		}
		if literal, ok := assign.Rhs[0].(*ast.BasicLit); ok {
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			value = strings.ReplaceAll(value, "::jsonb", "")
			value = strings.ReplaceAll(value, "$1::uuid", "$1")
			value = strings.ReplaceAll(value, "?", "$1")
			queries = append(queries, value)
		}
		return true
	})
	if len(queries) != 2 || queries[0] != queries[1] {
		t.Fatal("shadow fault dialects no longer share exact payload and run scope")
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/store/internal/backend/pipelinepersistence/workflow_projection_shape_fault.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "fault.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "SetWorkflowProjectionObsoleteFieldRowsForTest" {
			continue
		}
		found = true
		call := fn.Body.List[0].(*ast.ReturnStmt).Results[0].(*ast.CallExpr).Args[0].(*ast.CallExpr)
		value, err := strconv.Unquote(call.Args[1].(*ast.BasicLit).Value)
		if err != nil {
			t.Fatal(err)
		}
		if value != queries[0] || formattedNativeReadNode(call.Fun) != "tx.ExecContext" || len(call.Args) != 3 || formattedNativeReadNode(call.Args[0]) != "ctx" || formattedNativeReadNode(call.Args[2]) != "run" {
			t.Fatal("native shadow fault changed original columns, values or run-only predicate")
		}
	}
	if !found {
		t.Fatal("missing native shadow fault owner")
	}
}
func projectionHeaderWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	body := fn.Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		run := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = run.Args[1].(*ast.FuncLit).Body
		body.List = body.List[4:]
	} else {
		body.List = body.List[2:]
	}
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch formattedNativeReadNode(call.Fun) {
		case "store.create":
			call.Fun, _ = parser.ParseExpr("fixture.Construct")
		case "store.list":
			call.Fun, _ = parser.ParseExpr("fixture.List")
		}
		return true
	})
	if predecessor {
		var normalized []ast.Stmt
		for _, statement := range body.List {
			text := formattedNativeReadNode(statement)
			switch {
			case strings.HasPrefix(text, "query := "), strings.HasPrefix(text, `if backend == "postgres" {`):
				continue
			case strings.HasPrefix(text, "result, err := store.testDB().ExecContext("):
				if text != "result, err := store.testDB().ExecContext(ctx, query, testPipelineRunID)" {
					t.Fatal("original shadow fault context or run changed")
				}
				statement = projectionShapeStatement(t, "changed, err := fixture.ObsoleteFieldRows(ctx, testPipelineRunID)")
			case strings.HasPrefix(text, "if changed, err := result.RowsAffected();"):
				if formattedNativeReadNode(statement.(*ast.IfStmt).Cond) != "err != nil || changed != 1" {
					t.Fatal("original one-row shadow witness changed")
				}
				statement = projectionShapeStatement(t, `if changed != 1{t.Fatalf("obsolete shadow fixture: rows=%d err=%v",changed,err)}`)
			}
			normalized = append(normalized, statement)
		}
		body.List = normalized
	}
	return formattedNativeReadNode(body)
}
func TestNativeProjectionHeaderOracleRejectsWeakenedCardinalityFieldsAndLifecycle(t *testing.T) {
	row := projectionHeaderRecipe(t)
	for _, condition := range []string{"changed != 1", "len(instances) != 2", `instance.CurrentState != "active"`, `instance.Fields["value"] != "business"`} {
		if !strings.Contains(row.After, condition) {
			t.Fatal("missing original header assertion")
		}
		changed := strings.Replace(row.After, condition, "false", 1)
		if projectionHeaderWorkload(t, row.Before, true) == projectionHeaderWorkload(t, changed, false) {
			t.Fatalf("weakened header assertion accepted: %s", condition)
		}
	}
}
