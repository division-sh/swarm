package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func mutationSeedRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var out []recipe
	for _, row := range rows {
		if row.Family == "native-mutation-seed" {
			out = append(out, row)
		}
	}
	if len(out) != 5 {
		t.Fatalf("mutation cohort=%d, want seed and all four callers", len(out))
	}
	return out
}
func TestNativeMutationSeedRecipesPreserveEveryCallbackFenceAndAssertion(t *testing.T) {
	for _, row := range mutationSeedRecipes(t) {
		if mutationSeedWorkload(t, row.Before, row.Function, true) != mutationSeedWorkload(t, row.After, row.Function, false) {
			t.Fatalf("mutation workload changed: %s", row.Function)
		}
		if strings.Contains(row.After, "releaseFirst := make(chan struct{})") {
			requireNativeMutationWorkerJoin(t, row.After)
		}
	}
}
func mutationSeedWorkload(t *testing.T, source, root string, predecessor bool) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	body := fn.Body
	seed := root == "seedWorkflowInstanceForMutationTest"
	if !seed {
		setup := 1
		if predecessor {
			setup = 3
		}
		body.List = body.List[setup:]
	}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		call, ok := cursor.Node().(*ast.CallExpr)
		if !ok {
			return true
		}
		fun := formattedNativeReadNode(call.Fun)
		switch fun {
		case "testWorkflowStoreRunContext":
			cursor.Replace(mutationSeedExpression(t, "fixture.Context"))
			return false
		case "store.Load":
			call.Fun = mutationSeedExpression(t, "fixture.Persistence.LoadWorkflowInstance")
		case "store.mutate":
			call.Fun = mutationSeedExpression(t, "fixture.Persistence.store.mutate")
		case "store.mutateE":
			call.Fun = mutationSeedExpression(t, "fixture.Persistence.store.mutateE")
		case "store.upsert":
			call.Fun = mutationSeedExpression(t, "fixture.Construct")
		case "seedWorkflowInstanceForMutationTest":
			if predecessor {
				call.Args[1] = ast.NewIdent("fixture")
			}
		case "pc.persistWorkflowStateForTest":
			call.Fun = ast.NewIdent("persistClaimedWorkflowStateForTest")
			call.Args = append(append([]ast.Expr{call.Args[0], ast.NewIdent("pc")}, call.Args[1:]...), ast.NewIdent("claimed"))
		case "close":
			if !predecessor && formattedNativeReadNode(call) == "close(releaseFirst)" {
				return true
			}
			if predecessor && formattedNativeReadNode(call) == "close(releaseFirst)" {
				call.Fun = ast.NewIdent("release")
				call.Args = nil
			}
		}
		return true
	}, nil)
	var normalized []ast.Stmt
	for _, statement := range body.List {
		text := formattedNativeReadNode(statement)
		if !predecessor && (text == "var workers sync.WaitGroup" || text == "var releaseOnce sync.Once" || strings.HasPrefix(text, "release := func()") || text == "workers.Add(1)" || strings.HasPrefix(text, "t.Cleanup(func()")) {
			continue
		}
		if predecessor && strings.HasPrefix(text, "pc := &PipelineCoordinator{") {
			literal := statement.(*ast.AssignStmt).Rhs[0].(*ast.UnaryExpr).X.(*ast.CompositeLit)
			if len(literal.Elts) != 3 {
				t.Fatal("original coordinator setup changed")
			}
			var module ast.Expr
			for _, entry := range literal.Elts {
				pair := entry.(*ast.KeyValueExpr)
				if formattedNativeReadNode(pair.Key) == "module" {
					module = pair.Value
				}
			}
			statement = projectionShapeStatement(t, "pc := fixture.NewCoordinator(&recordingPipelineBus{}, PipelineCoordinatorOptions{Module:"+formattedNativeReadNode(module)+"})")
		}
		if predecessor && strings.HasPrefix(text, "transitionCtx := testPersistedWorkflowStateTransitionContext(") {
			if text != `transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, testWorkflowInstanceRoute("mutation-flow"), entityID, "workflow.completed")` {
				t.Fatal("original transition identity changed")
			}
			statement = projectionShapeStatement(t, `transitionCtx, claimed := prepareClaimedWorkflowTransitionForTest(t, fixture, pc, ctx, testWorkflowInstanceRoute("mutation-flow"), entityID, "queued", "done", "workflow.completed")`)
		}
		if goStmt, ok := statement.(*ast.GoStmt); ok && !predecessor {
			callback := goStmt.Call.Fun.(*ast.FuncLit)
			if formattedNativeReadNode(callback.Body.List[0]) != "defer workers.Done()" {
				t.Fatal("native worker is not joined")
			}
			callback.Body.List = callback.Body.List[1:]
		}
		if conditional, ok := statement.(*ast.IfStmt); ok && predecessor && formattedNativeReadNode(conditional.Cond) == "!runtimefailures.IsStateContention(err)" {
			conditional.Cond = mutationSeedExpression(t, "!nativeWorkflowRevisionConflictForTest(err)")
		}
		normalized = append(normalized, statement)
	}
	body.List = normalized
	return formattedNativeReadNode(body)
}
func mutationSeedExpression(t *testing.T, source string) ast.Expr {
	t.Helper()
	expr, err := parser.ParseExpr(source)
	if err != nil {
		t.Fatal(err)
	}
	return expr
}
func requireNativeMutationWorkerJoin(t *testing.T, source string) {
	t.Helper()
	for _, fragment := range []string{"var workers sync.WaitGroup", "var releaseOnce sync.Once", "releaseOnce.Do(func() { close(releaseFirst) })", "t.Cleanup(func() { release(); workers.Wait() })"} {
		if !strings.Contains(source, fragment) {
			t.Fatalf("mutation worker cleanup missing: %s", fragment)
		}
	}
	if strings.Count(source, "workers.Add(1)") != 2 || strings.Count(source, "defer workers.Done()") != 2 {
		t.Fatal("both native contenders must be joined")
	}
}

func TestNativeMutationSeedOracleRejectsChangedOrderingStateGatesAndHistory(t *testing.T) {
	for _, row := range mutationSeedRecipes(t) {
		for _, condition := range []string{`loaded.CurrentState == "must_not_commit"`, `got != "done"`, `got != "processing"`, `gates["g_first"] || !gates["g_second"]`, `gates["g_ready"]`, `len(instance.TransitionHistory) == 0`, `len(evidence) != 1`} {
			if !strings.Contains(row.After, condition) {
				continue
			}
			changed := strings.Replace(row.After, condition, "false", 1)
			if mutationSeedWorkload(t, row.Before, row.Function, true) == mutationSeedWorkload(t, changed, row.Function, false) {
				t.Fatalf("weakened mutation assertion accepted: %s", condition)
			}
		}
		if strings.Contains(row.After, "<-firstEntered") {
			changed := strings.Replace(row.After, "<-firstEntered", "<-secondEntered", 1)
			if mutationSeedWorkload(t, row.Before, row.Function, true) == mutationSeedWorkload(t, changed, row.Function, false) {
				t.Fatal("changed contention cut accepted")
			}
		}
	}
}

func TestNativeMutationTransitionPrefixConsumesOneReviewedPreparationOwner(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original recipe
	for _, row := range rows {
		if row.Family == "native-mutation-owner-wrapper" {
			original = row
		}
	}
	if original.Function == "" {
		t.Fatal("missing transition owner recipe")
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/runtime/pipeline/workflow_lifecycle_test_helpers_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "lifecycle.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var shared *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "persistWorkflowStateWithAdmissionForTest" {
			shared = fn
		}
	}
	if shared == nil {
		t.Fatal("missing shared preparation owner")
	}
	ast.Inspect(shared.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "admit" {
			call.Fun = ast.NewIdent("admitTestLifecycleDeliveryOccurrence")
		}
		return true
	})
	if formattedNativeReadNode(shared.Body) != formattedNativeReadNode(projectionShapeFunction(t, original.Before).Body) {
		t.Fatal("transition preparation semantics changed during native admission split")
	}
	wrapper := projectionShapeFunction(t, original.After)
	want := projectionShapeFunction(t, `func wrapper(){return pc.persistWorkflowStateWithAdmissionForTest(ctx, route, entityID, nextState, sourceEvent, admitTestLifecycleDeliveryOccurrence)}`).Body
	if formattedNativeReadNode(wrapper.Body) != formattedNativeReadNode(want) {
		t.Fatal("unmigrated consumers no longer delegate their original exact admission")
	}
}
