package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeMissingHeaderRecipe(t *testing.T, family string) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == family {
			found = append(found, row)
		}
	}
	if len(found) != 1 {
		t.Fatal("missing-header cohort must retain its complete root and source separately")
	}
	return found[0]
}

func nativeMissingHeaderConsumerWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
	}
	var statements []ast.Stmt
	proof := false
	for _, statement := range body.List {
		text := formattedNativeReadNode(statement)
		proof = proof || strings.HasPrefix(text, "node := pipelineNode(")
		coordinate := strings.HasPrefix(text, "instancePath := ") || strings.HasPrefix(text, "entityID := ") || strings.HasPrefix(text, "now := ")
		if proof || coordinate {
			if !strings.HasPrefix(text, "if !reflect.DeepEqual(prestate.State, persisted)") {
				statements = append(statements, statement)
			}
		}
	}
	if !proof {
		t.Fatal("missing-header consumer has no original handler witness")
	}
	selected := &ast.BlockStmt{List: statements}
	if predecessor {
		astutil.Apply(selected, func(cursor *astutil.Cursor) bool {
			if call, ok := cursor.Node().(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "store.LoadEntityState" {
				call.Fun = mutationSeedExpression(t, "fixture.Persistence.store.LoadEntityState")
			}
			return true
		}, nil)
	}
	return formattedNativeReadNode(selected)
}

func TestNativeMissingHeaderRecipePreservesOriginalIdentityAndRefusalAssertions(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-missing-header-target")
	if nativeMissingHeaderConsumerWorkload(t, row.Before, true) != nativeMissingHeaderConsumerWorkload(t, row.After, false) {
		t.Fatal("child relabeling identity/handler/refusal/field witness changed")
	}
	for _, required := range []string{
		`WorkflowName: "review/child"`, `CurrentState: "active"`, `EntityType: "review_item"`,
		`Fields: map[string]any{"marker": "unchanged"}`, `EnteredStageAt: now, CreatedAt: now, UpdatedAt: now`,
		`fixture.MissingHeader(ctx, testPipelineRunID, instancePath)`, `err != nil || changed != 1`,
		`prestate.Presence != WorkflowTargetPersistenceStateOnly`, `!reflect.DeepEqual(prestate.State, persisted)`,
	} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("native hostile prestate lost original row or exact fault: %s", required)
		}
	}
}

func TestNativeMissingHeaderOracleRejectsWeakenedRefusalAndReadback(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-missing-header-target")
	for _, required := range []string{`"review/child/instance"`, `!errors.Is(err, runtimeengine.ErrUnconstructedWorkflowTarget)`, `persisted.CurrentState != "active"`, `persisted.Revision != 1`, `fields["marker"] != "unchanged"`} {
		changed := strings.Replace(row.After, required, "false", 1)
		if nativeMissingHeaderConsumerWorkload(t, row.Before, true) == nativeMissingHeaderConsumerWorkload(t, changed, false) {
			t.Fatalf("changed coordinate or weakened assertion admitted: %s", required)
		}
	}
}

func TestNativeMissingHeaderSourceAdmitsBothOriginalScopesBeforeConsumption(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-missing-header-source")
	if strings.Contains(row.After, "semanticview.Bundle(") || strings.Contains(row.After, "parent.Children =") {
		t.Fatal("nested source regained post-admission mutation")
	}
	for _, required := range []string{`loadWorkflowTempSource(t, map[string]string{`, `"review/child/schema.yaml"`, `"review/child/entities.yaml"`, `"review/nodes.yaml"`, `advances_to: done`, `work.ready`, `review_item`, `review_entity`} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("admitted nested artifact lost original tested scope: %s", required)
		}
	}
}
