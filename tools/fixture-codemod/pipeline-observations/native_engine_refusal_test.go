package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeEngineRefusalRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-engine-physical-refusal" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 2 {
		t.Fatal("physical-count refusal cohort must cover both original roots")
	}
	return selected
}

func TestNativeEngineRefusalRecipesPreserveCandidatesErrorsAndWholeCounts(t *testing.T) {
	for _, row := range nativeEngineRefusalRecipes(t) {
		if nativeEngineRefusalWorkload(t, row.Before, true) != nativeEngineRefusalWorkload(t, row.After, false) {
			t.Fatalf("engine refusal/candidate/case/cardinality changed: %s", row.Function)
		}
	}
}

func nativeEngineRefusalWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		nativeEngineRefusalUnwrap(body)
	}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if nativeEngineRefusalSetup(cursor) || handlerNativeSetupStatement(t, cursor, predecessor) || nativeEngineReadSetup(t, cursor, "", predecessor) {
			return false
		}
		if predecessor {
			nativeEngineRefusalCounts(t, cursor)
			if call, ok := cursor.Node().(*ast.CallExpr); ok {
				handlerNativeRootCall(t, call, true)
			}
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeEngineRefusalUnwrap(body *ast.BlockStmt) {
	for i, stmt := range body.List {
		loop, ok := stmt.(*ast.RangeStmt)
		if !ok || formattedNativeReadNode(loop.Key) != "_" || formattedNativeReadNode(loop.Value) != "backend" {
			continue
		}
		inner := loop.Body.List
		if callStmt, ok := inner[0].(*ast.ExprStmt); ok {
			call := callStmt.X.(*ast.CallExpr)
			inner = call.Args[1].(*ast.FuncLit).Body.List
		}
		body.List = append(body.List[:i], inner...)
		return
	}
}

func nativeEngineRefusalSetup(cursor *astutil.Cursor) bool {
	assign, ok := cursor.Node().(*ast.AssignStmt)
	if ok && len(assign.Rhs) == 1 && strings.HasPrefix(formattedNativeReadNode(assign), "counts := nativeWorkflowEnginePhysicalCountsForTest(") {
		cursor.Delete()
		return true
	}
	return false
}

func nativeEngineRefusalCounts(t *testing.T, cursor *astutil.Cursor) {
	t.Helper()
	if declaration, ok := cursor.Node().(*ast.DeclStmt); ok && formattedNativeReadNode(declaration) == "var count int" {
		cursor.Delete()
		return
	}
	if conditional, ok := cursor.Node().(*ast.IfStmt); ok && conditional.Init != nil && strings.Contains(formattedNativeReadNode(conditional.Init), `db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)`) {
		conditional.Init = projectionShapeStatement(t, "count := counts[table]")
		conditional.Cond = mutationSeedExpression(t, "count != 0")
		call := conditional.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		literal := call.Args[0].(*ast.BasicLit)
		literal.Value = strings.Replace(literal.Value, ", err=%v", "", 1)
		call.Args = call.Args[:len(call.Args)-1]
	}
	if call, ok := cursor.Node().(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "t.Run" && formattedNativeReadNode(call.Args[0]) == `backend + "/" + label` {
		call.Args[0] = ast.NewIdent("label")
	}
}

func TestNativeEngineRefusalOracleRejectsLostCandidatesAndCountAssertions(t *testing.T) {
	for _, row := range nativeEngineRefusalRecipes(t) {
		for _, replacement := range [][2]string{{`[]string{"", "wrong_entity"}`, `[]string{""}`}, {"count != 0", "false"}, {`"entity_state", "flow_instances", "entity_mutations"`, `"entity_state"`}, {"!errors.Is(err, runtimeengine.ErrUnconstructedWorkflowTarget)", "false"}, {"disagrees with current root coordinate", "different refusal"}} {
			if !strings.Contains(row.After, replacement[0]) {
				continue
			}
			changed := strings.Replace(row.After, replacement[0], replacement[1], 1)
			if nativeEngineRefusalWorkload(t, row.Before, true) == nativeEngineRefusalWorkload(t, changed, false) {
				t.Fatalf("weakened refusal proof accepted: %s", replacement[0])
			}
		}
	}
}
