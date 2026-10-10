package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeEngineReadRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-engine-state-read" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 7 {
		t.Fatal("engine read cohort must cover both roots and every changed fixture consumer")
	}
	return selected
}

func TestNativeEngineReadRecipesPreserveSourceStateAndAssertions(t *testing.T) {
	for _, row := range nativeEngineReadRecipes(t) {
		row = historicalMechanicalRecipe(t, row)
		if nativeEngineReadWorkload(t, row, row.Before, true) != nativeEngineReadWorkload(t, row, row.After, false) {
			t.Fatalf("engine read source/state/assertion changed: %s", row.Function)
		}
	}
}

func nativeEngineReadWorkload(t *testing.T, row recipe, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if !predecessor && row.Function == "workflowHandlerNativeFixture" && nativeEngineReadAddedPort(t, cursor) {
			return false
		}
		if nativeEngineReadSetup(t, cursor, row.Function, predecessor) {
			return false
		}
		if predecessor {
			if call, ok := cursor.Node().(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "testWorkflowStoreRunContext" {
				cursor.Replace(ast.NewIdent("ctx"))
				return false
			}
			nativeEngineReadBindings(t, cursor.Node())
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeEngineReadAddedPort(t *testing.T, cursor *astutil.Cursor) bool {
	t.Helper()
	pair, ok := cursor.Node().(*ast.KeyValueExpr)
	if !ok {
		return false
	}
	var expected string
	switch formattedNativeReadNode(pair.Key) {
	case "Runs":
		expected = `selected.(interface {runtimerunlifecycle.OperationOwner; runtimerunlifecycle.CandidateStore; LoadRunOrigin(context.Context,string)(runtimerunlifecycle.RunOrigin,error)})`
	case "PublishDirect":
		expected = `func(ctx context.Context,event events.Event){storetest.CommitSemanticEvent(t,ctx,selected,event)}`
	case "PublishedEvent":
		expected = `func(ctx context.Context,id string)(events.Event,error){value,found,err:=selected.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx,id);if err!=nil{return events.Event{},err};if !found{return events.Event{},fmt.Errorf("missing committed handler emission %s",id)};if err:=value.Validate();err!=nil{return events.Event{},err};return value.Event.Event(),nil}`
	case "ConflictingEntityType":
		expected = `func(ctx context.Context,run,entity string)(int64,error){return storetest.SetWorkflowProjectionConflictingEntityType(ctx,selected,run,entity)}`
	case "MissingHeader":
		expected = `func(ctx context.Context,run,path string)(int64,error){return storetest.RemoveWorkflowProjectionHeader(ctx,selected,run,path)}`
	case "MissingFields":
		expected = `func(ctx context.Context,run,entity string)(int64,error){return storetest.RemoveWorkflowProjectionFields(ctx,selected,run,entity)}`
	case "Draining":
		expected = `func(ctx context.Context,run,path string)(int64,error){return storetest.SetWorkflowProjectionDraining(ctx,selected,run,path)}`
	case "Terminated":
		expected = `func(ctx context.Context,run,path string,at time.Time)(int64,error){return storetest.SetWorkflowProjectionTerminated(ctx,selected,run,path,at)}`
	case "ApplicationStorage":
		expected = `func(ctx context.Context)(json.RawMessage,error){value,err:=storetest.ReadSelectedForkApplicationStorageSnapshot(ctx,selected);if err!=nil{return nil,err};return json.Marshal(value)}`
	case "PhysicalCounts":
		expected = `func(ctx context.Context)(pipeline.WorkflowEnginePhysicalCountsForTest,error){counts,err:=storetest.ReadWorkflowEnginePhysicalCounts(ctx,selected);return pipeline.WorkflowEnginePhysicalCountsForTest{EntityStates:counts.EntityStates,ConstructedHeaders:counts.ConstructedHeaders,MutationJournal:counts.MutationJournal},err}`
	default:
		return false
	}
	if formattedNativeReadNode(pair.Value) != formattedNativeReadNode(mutationSeedExpression(t, expected)) {
		t.Fatal("native companion stopped consuming its exact original selected owner")
	}
	cursor.Delete()
	return true
}

func nativeEngineReadSetup(t *testing.T, cursor *astutil.Cursor, name string, predecessor bool) bool {
	if _, ok := cursor.Node().(ast.Stmt); !ok {
		return false
	}
	source := formattedNativeReadNode(cursor.Node())
	if predecessor && name == "workflowHandlerNativeFixture" && strings.HasPrefix(source, "if counts.ByOperation[") {
		cursor.Replace(projectionShapeStatement(t, `if counts.ByOperation[storetest.TransactionWorkflowMutation].WriteCommits != mutations || counts.ByOperation[storetest.TransactionDeliveryClaim].WriteCommits != claims || counts.Active != 0 {t.Errorf("handler cohort escaped selected mutation/claim ownership: %+v, want mutations=%d claims=%d", counts, mutations, claims)}`))
		return true
	}
	if predecessor && name == "TestPipelineEngineStateRepoLoadStateMissingEntityDoesNotMaterializeDefaults" {
		if assign, ok := cursor.Node().(*ast.AssignStmt); ok && strings.HasPrefix(source, "loaded, ok, err := repo.LoadState(") {
			call := assign.Rhs[0].(*ast.CallExpr)
			cursor.InsertBefore(projectionShapeStatement(t, "address := "+formattedNativeReadNode(call.Args[1])))
			call.Args[1] = ast.NewIdent("address")
		}
	}
	remove := source == "_, db, cleanup := testutil.StartPostgres(t)" || source == "t.Cleanup(cleanup)" ||
		source == "ctx := testWorkflowStoreRunContext(t, repo.coordinator.workflowStore)" ||
		source == "ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)" ||
		source == formattedNativeReadNode(projectionShapeStatement(t, "if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {t.Fatal(err)}")) ||
		source == "store := newPostgresWorkflowInstanceStoreForTest(db)" || source == "store := fixture.Persistence.store" ||
		source == "fixture := open(t, source)" || source == "ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)" ||
		strings.HasPrefix(source, "address.FlowInstance = testRunScopedWorkflowInstanceFromContext(")
	if predecessor && name == "TestPipelineEngineMutationOwnerRoundTripsTypedCarrier" {
		switch source {
		case `source := testRootEntityContractSource("root", "test_entity")`:
			cursor.Replace(projectionShapeStatement(t, `source := loadWorkflowTempSource(t, map[string]string{"schema.yaml":"name: root\n","entities.yaml":"test_entity:\n  score: integer\n  subject_id: text\n"})`))
			return true
		case "bundle, _ := semanticview.Bundle(source)",
			`bundle.RootEntities["test_entity"].Fields["score"] = runtimecontracts.EntityFieldDecl{Type: "integer"}`,
			`bundle.RootEntities["test_entity"].Fields["subject_id"] = runtimecontracts.EntityFieldDecl{Type: "text"}`:
			remove = true
		}
	}
	if remove {
		cursor.Delete()
		return true
	}
	return false
}

func nativeEngineReadBindings(t *testing.T, node ast.Node) {
	t.Helper()
	if pair, ok := node.(*ast.KeyValueExpr); ok {
		nativeEngineReadCoordinator(t, pair)
		if formattedNativeReadNode(pair.Key) == "WorkflowVersion" && formattedNativeReadNode(pair.Value) == `"1.0.0"` {
			pair.Value = mutationSeedExpression(t, "source.WorkflowVersion()")
		}
	}
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return
	}
	switch formattedNativeReadNode(call.Fun) {
	case "store.upsert":
		call.Fun = mutationSeedExpression(t, "fixture.Construct")
	case "workflowHandlerNativeFixture":
		call.Args = append(call.Args, call.Args[len(call.Args)-1])
	}
}

func nativeEngineReadCoordinator(t *testing.T, pair *ast.KeyValueExpr) {
	t.Helper()
	if formattedNativeReadNode(pair.Key) != "coordinator" {
		return
	}
	literal, ok := pair.Value.(*ast.UnaryExpr)
	if !ok {
		return
	}
	options := literal.X.(*ast.CompositeLit)
	if formattedNativeReadNode(options.Type) != "PipelineCoordinator" {
		return
	}
	options.Type = ast.NewIdent("PipelineCoordinatorOptions")
	var fields []ast.Expr
	for _, field := range options.Elts {
		entry := field.(*ast.KeyValueExpr)
		if formattedNativeReadNode(entry.Key) == "module" {
			entry.Key = ast.NewIdent("Module")
			fields = append(fields, entry)
		}
	}
	if len(fields) != 1 {
		t.Fatal("original engine read source module missing")
	}
	options.Elts = fields
	pair.Value = &ast.CallExpr{Fun: mutationSeedExpression(t, "fixture.NewCoordinator"), Args: []ast.Expr{options}}
}

func TestNativeEngineReadOracleRejectsLostAbsenceAndTypedStateAssertions(t *testing.T) {
	for _, row := range nativeEngineReadRecipes(t) {
		for _, condition := range []string{"if ok {", "if !ok {", `got != int64(91)`, `!loaded.Gates["ready"]`, `got != int64(2)`} {
			if !strings.Contains(row.After, condition) {
				continue
			}
			changed := strings.Replace(row.After, condition, "if false {", 1)
			if strings.HasPrefix(condition, "got ") || strings.HasPrefix(condition, "!") {
				changed = strings.Replace(row.After, condition, "false", 1)
			}
			if nativeEngineReadWorkload(t, row, row.Before, true) == nativeEngineReadWorkload(t, row, changed, false) {
				t.Fatalf("weakened engine read assertion accepted: %s", condition)
			}
		}
	}
}
