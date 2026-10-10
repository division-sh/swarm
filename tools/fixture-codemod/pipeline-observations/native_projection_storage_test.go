package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

func TestNativeProjectionStorageRecipesPreserveAssertionsAndPhysicalScope(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, row := range rows {
		if row.Family != "native-projection-storage" {
			continue
		}
		seen++
		if nativeProjectionStorageWorkload(t, row.Before, row.Function, true) != nativeProjectionStorageWorkload(t, row.After, row.Function, false) {
			t.Fatalf("projection storage workload changed: %s", row.Function)
		}
	}
	if seen != 2 {
		t.Fatalf("projection storage cohort=%d, want both roots", seen)
	}
}

func nativeProjectionStorageWorkload(t *testing.T, source, root string, predecessor bool) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "projection.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	body := file.Decls[0].(*ast.FuncDecl).Body
	setup := 1
	if predecessor {
		setup = 3
	}
	body.List = body.List[setup:]
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch formattedNativeReadNode(call.Fun) {
		case "store.create", "workflowStore.upsert":
			call.Fun, _ = parser.ParseExpr("fixture.Construct")
		case "store.Load", "workflowStore.Load":
			call.Fun, _ = parser.ParseExpr("fixture.Persistence.LoadWorkflowInstance")
		}
		return true
	})
	for _, statement := range body.List {
		assign, ok := statement.(*ast.AssignStmt)
		if ok && len(assign.Rhs) == 1 && strings.HasPrefix(formattedNativeReadNode(assign.Rhs[0]), "testWorkflowStoreRunContext(") {
			assign.Rhs[0], _ = parser.ParseExpr("fixture.Context")
		}
	}
	if predecessor {
		replaceProjectionPhysicalRead(t, body, root)
	}
	return formattedNativeReadNode(body)
}

func replaceProjectionPhysicalRead(t *testing.T, body *ast.BlockStmt, root string) {
	t.Helper()
	control := root == "TestWorkflowInstanceStoreProjection_DoesNotExposeControlStatusAsEntityField"
	start, width := -1, 2
	for i, statement := range body.List {
		text := formattedNativeReadNode(statement)
		if text == "var currentState string" || strings.HasPrefix(text, "var (\n\trevision") {
			start = i
			break
		}
	}
	if control {
		width = 4
	}
	if start < 0 || start+width > len(body.List) {
		t.Fatal("original physical read disappeared")
	}
	guard := body.List[start+width-1].(*ast.IfStmt)
	scan := guard.Init.(*ast.AssignStmt).Rhs[0].(*ast.CallExpr)
	query := scan.Fun.(*ast.SelectorExpr).X.(*ast.CallExpr)
	queryText, err := strconv.Unquote(query.Args[1].(*ast.BasicLit).Value)
	if err != nil || !projectionPhysicalScopeMatches(query, scan, queryText, control) {
		t.Fatal("original physical read predicate or columns changed")
	}
	method, entity, message := "ReadDuplicate", "storageRef", "query persisted projection after duplicate create: %v"
	assignment := "revision, configName, fieldsRaw := observed.Revision, observed.FieldName, observed.Fields"
	if control {
		method, entity, message = "ReadControl", "entityID", "query entity_state projection: %v"
		assignment = "currentState, fieldsRaw, controlStatus := observed.CurrentState, observed.Fields, observed.ControlStatus"
	}
	replacement := `observed, err := fixture.` + method + `(ctx, testPipelineRunID, workflowInstanceRowID(` + entity + `)); if err != nil { t.Fatalf(` + strconv.Quote(message) + `, err) }; ` + assignment
	file, err := parser.ParseFile(token.NewFileSet(), "read.go", "package proof\nfunc read(){"+replacement+"}", parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	statements := file.Decls[0].(*ast.FuncDecl).Body.List
	body.List = append(append(body.List[:start:start], statements...), body.List[start+width:]...)
}

func projectionPhysicalScopeMatches(query, scan *ast.CallExpr, queryText string, control bool) bool {
	columns, entity, bindings := "es.revision, COALESCE(es.fields->>'name', ''), es.fields", "storageRef", []string{"&revision", "&configName", "&fieldsRaw"}
	if control {
		columns, entity, bindings = "es.current_state, es.fields, COALESCE(fi.config->>'status', '')", "entityID", []string{"&currentState", "&fieldsRaw", "&controlStatus"}
	}
	want := "SELECT " + columns + " FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE es.run_id = $1::uuid AND es.entity_id = $2::uuid"
	if strings.Join(strings.Fields(queryText), " ") != want || len(query.Args) != 4 || len(scan.Args) != 3 {
		return false
	}
	if formattedNativeReadNode(query.Fun) != "db.QueryRowContext" || formattedNativeReadNode(query.Args[0]) != "ctx" || formattedNativeReadNode(query.Args[2]) != "testPipelineRunID" || formattedNativeReadNode(query.Args[3]) != "workflowInstanceRowID("+entity+")" {
		return false
	}
	for i, arg := range scan.Args {
		if formattedNativeReadNode(arg) != bindings[i] {
			return false
		}
	}
	return true
}

func TestNativeProjectionStorageOracleRejectsWeakenedRevisionStatusAndDuplicateRefusal(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	conditions := []string{"got != \"active\"", "revision != 1", "failure.Failure.Detail.Code != \"flow_instance_already_exists\""}
	for _, row := range rows {
		if row.Family != "native-projection-storage" {
			continue
		}
		for _, condition := range conditions {
			if !strings.Contains(row.After, condition) {
				continue
			}
			mutated := strings.Replace(row.After, condition, "false", 1)
			if nativeProjectionStorageWorkload(t, row.Before, row.Function, true) == nativeProjectionStorageWorkload(t, mutated, row.Function, false) {
				t.Fatalf("weakened projection assertion accepted: %s", condition)
			}
		}
	}
}
