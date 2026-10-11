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
	if queries[0] != `UPDATE entity_state SET current_state = 'obsolete', gates = '{}', bookkeeping = '{}', accumulator = '{}' WHERE run_id = $1` {
		t.Fatal("frozen shadow fault changed original columns, values or run-only predicate")
	}
}

func TestNativeProjectionHeaderQ10RetiresShadowOwnerAndKeepsLiveAssertions(t *testing.T) {
	var liveProof, fixtureOwner string
	businessInputs := map[string]string{
		"current_state": `"business-stage"`, "gates": `"business-gates"`, "bookkeeping": `"business-bookkeeping"`,
		"accumulator": `"business-accumulator"`, "revision": "int64(73)", "entered_state_at": `"business-clock"`,
	}
	for _, path := range []string{
		"internal/store/internal/backend/pipelinepersistence/workflow_projection_shape_fault.go",
		"internal/store/internal/runtimepersistence/test_workflow_projection_shape_fault.go",
		"internal/store/storetest/workflow_projection_shape_fault.go",
		"internal/runtime/pipeline/workflow_projection_header_native_fixture_test.go",
		"internal/runtime/pipeline/workflow_projection_header_native_external_test.go",
		"internal/runtime/pipeline/workflow_instance_store_projection_test.go",
	} {
		source, err := os.ReadFile(filepath.Join("..", "..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok && (ident.Name == "SetWorkflowProjectionObsoleteFieldRowsForTest" || ident.Name == "SetWorkflowProjectionObsoleteFieldRows" || ident.Name == "ObsoleteFieldRows") {
				t.Fatalf("retired shadow authority survives in %s", path)
			}
			return true
		})
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				switch fn.Name.Name {
				case "VerifyWorkflowInstanceFixtureListUsesConstructedHeadersForTest":
					liveProof = formattedNativeReadNode(fn)
					ast.Inspect(fn, func(node ast.Node) bool {
						if pair, ok := node.(*ast.KeyValueExpr); ok {
							if key, ok := pair.Key.(*ast.BasicLit); ok && key.Kind == token.STRING {
								name, err := strconv.Unquote(key.Value)
								if err != nil {
									t.Fatal(err)
								}
								if value, required := businessInputs[name]; required && formattedNativeReadNode(pair.Value) == value {
									delete(businessInputs, name)
								}
							}
						}
						return true
					})
				case "workflowProjectionHeaderNativeFixture":
					fixtureOwner = formattedNativeReadNode(fn)
				}
			}
		}
	}
	if len(businessInputs) != 0 {
		t.Fatalf("live Q10 header witness lost conflicting business operands: %+v", businessInputs)
	}
	if !strings.Contains(fixtureOwner, "workflowProjectionNativeFixtureWithWrites(t, selected, reopen, runID, 2)") || !strings.Contains(fixtureOwner, "selected.ListWorkflowInstances") {
		t.Fatal("live list witness must retain exactly two native constructions and the canonical list owner")
	}
	for _, assertion := range []string{
		"len(instances) != 2", "instance.EntityID != want.EntityID", "instance.WorkflowName != want.WorkflowName",
		`instance.CurrentState != "active"`, "!instance.StageDefined", "instance.Revision != 1",
		`!instance.Gates["ready"]`, `instance.Bookkeeping["authority"] != "header"`, `instance.StateBuckets["count"] != int64(1)`,
		`instance.EntityType != "" || len(instance.Fields) != 0`, `instance.EntityType != "subject"`,
		`instance.Fields["value"] != "business"`, "!reflect.DeepEqual(instance.Fields, want.Fields)", "len(expected) != 0",
	} {
		if !strings.Contains(liveProof, assertion) {
			t.Fatalf("live Q10 header witness lost assertion %s", assertion)
		}
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
