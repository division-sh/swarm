package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func handlerNativeRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-existing-handler" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 5 {
		t.Fatal("handler cohort must include shared owner and all four roots")
	}
	return selected
}

func TestNativeHandlerRecipesPreserveEverySourceCaseAndAssertion(t *testing.T) {
	for _, row := range handlerNativeRecipes(t) {
		if row.Function == "executeExistingOwnerBehavior" {
			if handlerNativeHelperWorkload(t, row.Before, true) != handlerNativeHelperWorkload(t, row.After, false) {
				t.Fatal("handler seed/execution/result workload changed")
			}
			continue
		}
		if handlerNativeRootWorkload(t, row.Before, true) != handlerNativeRootWorkload(t, row.After, false) {
			t.Fatalf("handler source/case/assertion changed: %s", row.Function)
		}
	}
}

func handlerNativeRootWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
	}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if handlerNativeSetupStatement(t, cursor, predecessor) {
			return false
		}
		if pair, ok := cursor.Node().(*ast.KeyValueExpr); ok && predecessor &&
			formattedNativeReadNode(pair.Key) == "store" && formattedNativeReadNode(pair.Value) == "store" {
			pair.Value = mutationSeedExpression(t, "fixture.Persistence.store")
		}
		call, ok := cursor.Node().(*ast.CallExpr)
		if !ok {
			return true
		}
		handlerNativeRootCall(t, call, predecessor)
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func handlerNativeSetupStatement(t *testing.T, cursor *astutil.Cursor, predecessor bool) bool {
	node := cursor.Node()
	if _, ok := node.(ast.Stmt); !ok {
		return false
	}
	source := formattedNativeReadNode(node)
	remove := source == "var ctx context.Context" ||
		source == "configureWorkflowLifecycleForTest(t, pc)" || source == "configurePipelineTestDeliveryOwner(t, pc)"
	if conditional, ok := node.(*ast.IfStmt); ok && formattedNativeReadNode(conditional.Cond) == `backend == "sqlite"` {
		expected := projectionShapeStatement(t, `if backend=="sqlite"{ctx=sqliteExactOnceRunContext(t,db)}else{ctx=testPipelineRunContext(t,db)}`)
		if source != formattedNativeReadNode(expected) {
			t.Fatal("unexpected original backend/context branch")
		}
		remove = true
	}
	if !predecessor {
		remove = remove || source == "fixture := open(t, source)" ||
			source == "ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)" ||
			source == "address.FlowInstance = testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)"
	} else if source == "db, store := openHandlerEntityRequirementStore(t, backend)" {
		remove = true
	}
	if remove {
		cursor.Delete()
		return true
	}
	return false
}

func handlerNativeRootCall(t *testing.T, call *ast.CallExpr, predecessor bool) {
	if !predecessor {
		return
	}
	switch formattedNativeReadNode(call.Fun) {
	case "newDurablePipelineCoordinatorForTest":
		call.Fun = mutationSeedExpression(t, "fixture.NewCoordinator")
		options := call.Args[2].(*ast.CompositeLit)
		var fields []ast.Expr
		for _, field := range options.Elts {
			pair := field.(*ast.KeyValueExpr)
			if formattedNativeReadNode(pair.Key) == "Module" {
				fields = append(fields, field)
			}
		}
		if len(fields) != 1 {
			t.Fatal("original handler coordinator lost its exact source module")
		}
		options.Elts = fields
		call.Args = []ast.Expr{options}
	case "executeExistingOwnerBehavior":
		call.Args = append([]ast.Expr{call.Args[0], ast.NewIdent("fixture")}, call.Args[1:]...)
	case "store.Load":
		call.Fun = mutationSeedExpression(t, "fixture.Persistence.LoadWorkflowInstance")
	}
}

func handlerNativeHelperWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if !predecessor {
		return formattedNativeReadNode(body)
	}
	var output []ast.Stmt
	for _, stmt := range body.List {
		text := formattedNativeReadNode(stmt)
		if text == "seedExactOnceEvent(t, pc.workflowStore, ctx, sourceEvent)" {
			continue
		}
		if strings.HasPrefix(text, "deliveryCtx := withClaimedWorkflowNodePublicationForTest(") {
			assign := stmt.(*ast.AssignStmt)
			call := assign.Rhs[0].(*ast.CallExpr)
			route := projectionShapeStatement(t, "route := routeValue").(*ast.AssignStmt)
			route.Rhs[0] = call.Args[4]
			output = append(output, route,
				projectionShapeStatement(t, "fixture.Publish(ctx, sourceEvent, route)"),
				projectionShapeStatement(t, "deliveryCtx, stopHeartbeat := claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, evt, route)"),
				projectionShapeStatement(t, `defer func(){if err:=stopHeartbeat();err!=nil{t.Errorf("native handler heartbeat cleanup: %v",err)}}()`))
			continue
		}
		ast.Inspect(stmt, func(node ast.Node) bool {
			if pair, ok := node.(*ast.KeyValueExpr); ok && formattedNativeReadNode(pair.Key) == "WorkflowVersion" {
				if formattedNativeReadNode(pair.Value) != `"1"` {
					t.Fatal("original handler version changed")
				}
				pair.Value = mutationSeedExpression(t, "pc.SemanticSource().WorkflowVersion()")
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch formattedNativeReadNode(call.Fun) {
			case "pc.workflowStore.upsert":
				call.Fun = mutationSeedExpression(t, "fixture.Construct")
			case "pc.workflowStore.Load":
				call.Fun = mutationSeedExpression(t, "fixture.Persistence.LoadWorkflowInstance")
			case "handlerTestRootIngress":
				if len(call.Args) != 10 || formattedNativeReadNode(call.Args[7]) != `""` {
					t.Fatal("original handler event identity changed")
				}
				call.Fun = mutationSeedExpression(t, "eventtest.ExistingRunRootIngressWithRoutingSource")
				call.Args = append(append([]ast.Expr{}, call.Args[:7]...), call.Args[8], mutationSeedExpression(t, `testWorkflowRoutingSource(".", flowInstance, entityID)`), call.Args[9])
			}
			return true
		})
		output = append(output, stmt)
	}
	return formattedNativeReadNode(&ast.BlockStmt{List: output})
}

func TestNativeHandlerOraclesRejectLostCasesAndPersistenceAssertions(t *testing.T) {
	for _, row := range handlerNativeRecipes(t) {
		if row.Function == "executeExistingOwnerBehavior" {
			continue
		}
		for _, condition := range []string{"!result.handled", "len(instance.Fields) != 0", "len(result.emissions) != 1", "after.Revision != before.Revision", "!reflect.DeepEqual(instance.Fields, tc.wantFields)"} {
			if !strings.Contains(row.After, condition) {
				continue
			}
			changed := strings.Replace(row.After, condition, "false", 1)
			if handlerNativeRootWorkload(t, row.Before, true) == handlerNativeRootWorkload(t, changed, false) {
				t.Fatalf("weakened handler assertion accepted: %s", condition)
			}
		}
	}
}
