package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

var nativeActivitySetupRoots = map[string]bool{
	"TestPipelineActivityRequestExecutesNonIdempotentHTTPToolOnceWithStaticCredentials":     true,
	"TestGeneratedSyntheticConnectorUsesCanonicalActivityJournalOnReplay":                   true,
	"TestPipelineActivityRequestNonIdempotentFailureDoesNotRetry":                           true,
	"TestPipelineActivityRequestNonIdempotentTransportErrorMarksUncertain":                  true,
	"TestPipelineActivityRequestStartedJournalBlocksProviderRedispatchWithoutTerminalizing": true,
	"TestPipelineActivityRequestConcurrentDuplicatePreservesOriginalTerminalResult":         true,
	"TestPipelineActivityRequestMissingCredentialFailsAfterClaimBeforeDispatch":             true,
	"TestPipelineActivityRequestTelegramConnectorMissingTokenFailsAfterClaimBeforeDispatch": true,
}

func TestNativeActivitySetupRecipesPreserveEveryWorkloadStatement(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-activity-setup" {
			continue
		}
		if row.File != "internal/runtime/pipeline/activity_engine_test.go" || !nativeActivitySetupRoots[row.Function] || seen[row.Function] {
			t.Fatalf("unknown or repeated native activity root: %s/%s", row.File, row.Function)
		}
		seen[row.Function] = true
		expected, err := nativeActivitySetupAfter(row.Before)
		actual, parseErr := canonicalFunction(row.After)
		if err != nil || parseErr != nil || expected != actual {
			t.Fatalf("activity workload/temporal proof changed: %s: %v / %v", row.Function, err, parseErr)
		}
	}
	if len(seen) != len(nativeActivitySetupRoots) {
		t.Fatalf("native activity family count=%d, want %d", len(seen), len(nativeActivitySetupRoots))
	}
}

func nativeActivitySetupAfter(source string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "activity.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return "", err
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	if !nativeActivitySetupRoots[fn.Name.Name] || fn.Recv != nil || fn.Type.Params.NumFields() != 1 {
		return "", fmt.Errorf("unreviewed activity root or signature")
	}
	if err := replaceNativeActivitySetup(fn); err != nil {
		return "", err
	}
	var rewriteErr error
	coordinators := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && formattedNativeReadNode(call.Fun) == "newDurablePipelineCoordinatorForTest" {
			coordinators++
			if err := rewriteNativeActivityCoordinator(call); err != nil {
				rewriteErr = err
			}
		}
		if id, ok := node.(*ast.Ident); ok && id.Name == "db" {
			rewriteErr = fmt.Errorf("unreviewed activity pool use survives")
		}
		return true
	})
	if rewriteErr != nil || coordinators != 1 {
		return "", fmt.Errorf("native activity coordinator count=%d: %v", coordinators, rewriteErr)
	}
	fn.Name.Name = "Verify" + strings.TrimPrefix(fn.Name.Name, "Test") + "ForTest"
	openType, _ := parser.ParseExpr("func(*testing.T) WorkflowActivityNativeFixtureForTest")
	fn.Type.Params.List = append(fn.Type.Params.List, &ast.Field{Names: []*ast.Ident{ast.NewIdent("open")}, Type: openType})
	return formattedNativeReadNode(fn), nil
}

func replaceNativeActivitySetup(fn *ast.FuncDecl) error {
	replacements := map[string]string{
		"ctx := testAuthorActivityContext(t, context.Background())": "ctx := fixture.Context",
		"db, store := newSQLiteActivityJournalStore(t, ctx)":        "store := fixture.Persistence.store",
		"seedActivityRun(t, db, true, runID)":                       "if err := fixture.RequireRun(ctx, runID); err != nil { t.Fatal(err) }",
	}
	seen := map[string]bool{}
	for i, statement := range fn.Body.List {
		old := formattedNativeReadNode(statement)
		newSource, found := replacements[old]
		if !found {
			continue
		}
		if seen[old] {
			return fmt.Errorf("duplicate native activity setup")
		}
		seen[old] = true
		parsed, err := parser.ParseFile(token.NewFileSet(), "setup.go", "package probe\nfunc setup(){"+newSource+"}", parser.AllErrors)
		if err != nil {
			return err
		}
		fn.Body.List[i] = parsed.Decls[0].(*ast.FuncDecl).Body.List[0]
	}
	if len(seen) != len(replacements) {
		return fmt.Errorf("native activity setup count=%d, want three", len(seen))
	}
	parsed, _ := parser.ParseFile(token.NewFileSet(), "open.go", "package probe\nfunc setup(){fixture := open(t)}", 0)
	fn.Body.List = append(parsed.Decls[0].(*ast.FuncDecl).Body.List, fn.Body.List...)
	return nil
}

func rewriteNativeActivityCoordinator(call *ast.CallExpr) error {
	if len(call.Args) != 3 || formattedNativeReadNode(call.Args[0]) != "bus" || formattedNativeReadNode(call.Args[1]) != "db" {
		return fmt.Errorf("unreviewed activity coordinator arguments")
	}
	options, ok := call.Args[2].(*ast.CompositeLit)
	if !ok || formattedNativeReadNode(options.Type) != "PipelineCoordinatorOptions" {
		return fmt.Errorf("unreviewed activity coordinator options")
	}
	var retained []ast.Expr
	removed := 0
	for _, element := range options.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			return fmt.Errorf("unkeyed activity options")
		}
		if formattedNativeReadNode(field.Key) == "PipelineObligations" {
			if formattedNativeReadNode(field.Value) != "unavailablePipelineTestObligationOwner{}" {
				return fmt.Errorf("activity has a different obligation owner")
			}
			removed++
			continue
		}
		retained = append(retained, element)
	}
	if removed != 1 {
		return fmt.Errorf("activity obligation owner count=%d, want one", removed)
	}
	options.Elts = retained
	call.Fun, _ = parser.ParseExpr("fixture.NewCoordinator")
	call.Args = []ast.Expr{call.Args[0], options}
	return nil
}

func TestNativeActivitySetupRefusesChangedPoolContextAndOwners(t *testing.T) {
	base := `func TestPipelineActivityRequestConcurrentDuplicatePreservesOriginalTerminalResult(t *testing.T) {
ctx := testAuthorActivityContext(t, context.Background())
db, store := newSQLiteActivityJournalStore(t, ctx)
seedActivityRun(t, db, true, runID)
pc := newDurablePipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{Module: module, Persistence: workflowPersistenceForTest(store), PipelineObligations: unavailablePipelineTestObligationOwner{}})
<-providerEntered
release()
if err := <-firstDone; err != nil { t.Fatal(err) }
if calls != 1 { t.Fatalf("no redispatch: %d", calls) }
}`
	for _, change := range [][2]string{
		{"<-providerEntered", "db.Exec(query); <-providerEntered"},
		{"newSQLiteActivityJournalStore(t, ctx)", "newSQLiteActivityJournalStore(other, ctx)"},
		{"testAuthorActivityContext(t, context.Background())", "context.WithoutCancel(parent)"},
		{"newDurablePipelineCoordinatorForTest(bus, db,", "newDurablePipelineCoordinatorForTest(bus, other,"},
		{"unavailablePipelineTestObligationOwner{}", "differentOwner{}"},
	} {
		if _, err := nativeActivitySetupAfter(strings.Replace(base, change[0], change[1], 1)); err == nil {
			t.Fatalf("unreviewed authority matched: %v", change)
		}
	}
	out, err := nativeActivitySetupAfter(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"<-providerEntered", "release()", "<-firstDone", "no redispatch: %d"} {
		if !strings.Contains(out, required) {
			t.Fatalf("lost temporal/assertion statement %q", required)
		}
	}
}
