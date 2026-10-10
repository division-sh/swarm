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

func schedulerTimerRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-scheduler-timer" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 1 {
		t.Fatal("scheduler timer cohort is not exact")
	}
	return selected[0]
}

func schedulerTimerWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if !predecessor {
		return formattedNativeReadNode(&ast.BlockStmt{List: body.List[2:]})
	}
	body.List = body.List[3:]
	var out []ast.Stmt
	for _, stmt := range body.List {
		if assign, ok := stmt.(*ast.AssignStmt); ok && formattedNativeReadNode(assign) == "ctx := testWorkflowStoreRunContext(t, store)" {
			continue
		}
		if expression, ok := stmt.(*ast.ExprStmt); ok {
			call, ok := expression.X.(*ast.CallExpr)
			if ok && formattedNativeReadNode(call.Fun) == "insertGenericSchedulePersistenceFixture" {
				admit := projectionShapeStatement(t, "committed, err := fixture.AdmitSchedule(ctx, command)").(*ast.AssignStmt)
				admit.Rhs[0].(*ast.CallExpr).Args[1] = call.Args[4]
				out = append(out, admit, projectionShapeStatement(t, `if err != nil || !committed.Acknowledged || committed.Result.Outcome != runtimegenericschedule.AdmissionCreated { t.Fatalf("admit exact native scheduler timer: %+v err=%v", committed, err) }`))
				continue
			}
		}
		if _, ok := stmt.(*ast.DeclStmt); ok {
			out = append(out, projectionShapeStatement(t, "schedulerRows, err := fixture.CountSchedulerTimers(ctx, entityID, storageRef)"), projectionShapeStatement(t, `if err != nil { t.Fatalf("count scheduler-owned timers: %v", err) }`))
			continue
		}
		if guard, ok := stmt.(*ast.IfStmt); ok && guard.Init != nil {
			if strings.Contains(formattedNativeReadNode(guard.Init), "db.QueryRowContext") {
				continue
			}
		}
		ast.Inspect(stmt, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch formattedNativeReadNode(call.Fun) {
			case "store.upsert":
				call.Fun = mutationSeedExpression(t, "fixture.Construct")
				call.Args[0] = ast.NewIdent("ctx")
			case "store.mutate":
				call.Fun = mutationSeedExpression(t, "fixture.Persistence.store.mutate")
				call.Args[0] = ast.NewIdent("ctx")
			case "store.Load":
				call.Fun = mutationSeedExpression(t, "fixture.Persistence.LoadWorkflowInstance")
				call.Args[0] = ast.NewIdent("ctx")
			}
			return true
		})
		out = append(out, stmt)
	}
	return formattedNativeReadNode(&ast.BlockStmt{List: out})
}

func TestNativeSchedulerTimerRecipePreservesCommandIsolationCallbackAndAssertions(t *testing.T) {
	row := schedulerTimerRecipe(t)
	if schedulerTimerWorkload(t, row.Before, true) != schedulerTimerWorkload(t, row.After, false) {
		t.Fatal("scheduler isolation workload or assertions changed")
	}
	for _, assertion := range []string{"instance.CurrentState = \"active\"", "if !ok", "schedulerRows != 1", "!committed.Acknowledged", "Due:           runtimegenericschedule.AbsoluteDue(now.Add(2 * time.Hour))"} {
		if !strings.Contains(row.After, assertion) {
			t.Fatalf("missing scheduler workload %s", assertion)
		}
		replacement := "false"
		switch assertion {
		case "instance.CurrentState = \"active\"":
			replacement = "instance.CurrentState = \"queued\""
		case "if !ok":
			replacement = "if false"
		case "schedulerRows != 1":
			replacement = "schedulerRows != 0"
		case "Due:           runtimegenericschedule.AbsoluteDue(now.Add(2 * time.Hour))":
			replacement = "Due: runtimegenericschedule.AbsoluteDue(now.Add(3 * time.Hour))"
		}
		changed := strings.Replace(row.After, assertion, replacement, 1)
		if schedulerTimerWorkload(t, row.Before, true) == schedulerTimerWorkload(t, changed, false) {
			t.Fatalf("weakened scheduler proof accepted: %s", assertion)
		}
	}
}

func TestNativeSchedulerTimerObservationPreservesWholePhysicalPredicate(t *testing.T) {
	row := schedulerTimerRecipe(t)
	var original string
	ast.Inspect(projectionShapeFunction(t, row.Before), func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || formattedNativeReadNode(call.Fun) != "db.QueryRowContext" {
			return true
		}
		if len(call.Args) != 5 || formattedNativeReadNode(call.Args[2]) != "entityID" || formattedNativeReadNode(call.Args[3]) != "storageRef" || formattedNativeReadNode(call.Args[4]) != "runtimeWorkflowID" {
			t.Fatal("original timer coordinates changed")
		}
		original, _ = strconv.Unquote(call.Args[1].(*ast.BasicLit).Value)
		return true
	})
	original = strings.Join(strings.Fields(original), " ")
	if original != "SELECT COUNT(*) FROM timers WHERE entity_id = $1::uuid AND flow_instance = $2 AND owner_agent = $3" {
		t.Fatal("original physical timer predicate changed")
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/store/internal/backend/genericschedule/test_workflow_scheduler_observation.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "observation.go", data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	owner, err := uniqueFunction(file, "CountWorkflowSchedulerTimersForTest")
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(owner, func(node ast.Node) bool {
		if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			value, _ := strconv.Unquote(lit.Value)
			if strings.HasPrefix(value, "SELECT") {
				queries = append(queries, strings.Join(strings.Fields(value), " "))
			}
		}
		return true
	})
	sqlite := "SELECT COUNT(*) FROM timers WHERE entity_id = ? AND flow_instance = ? AND owner_agent = ?"
	if len(queries) != 2 || queries[0] != sqlite || queries[1] != original {
		t.Fatal("native timer count changed scope, status/history or dialect")
	}
	if !strings.Contains(string(data), `query, entity, path, "workflow-runtime"`) {
		t.Fatal("native count changed fixed scheduler owner or bind order")
	}
}
