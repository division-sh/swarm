package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

var nativeJournalSetupRoots = map[string]bool{
	"TestActivityJournalFixtureTerminalNoopBothStores":             true,
	"TestActivityAttemptJournalSQLiteAndPostgres":                  true,
	"TestActivityAttemptJournalPreservesReplyContextAcrossRestart": true,
}

func TestNativeJournalRecipesPreserveMutationsAndCompleteResultAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-activity-journal" {
			continue
		}
		if row.File != "internal/runtime/pipeline/activity_journal_test.go" || !nativeJournalSetupRoots[row.Function] || seen[row.Function] {
			t.Fatalf("unknown or duplicated journal consumer: %s/%s", row.File, row.Function)
		}
		seen[row.Function] = true
		before, after := journalMutationAndAssertionCalls(t, row.Before), journalMutationAndAssertionCalls(t, row.After)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("journal mutation/result assertions changed: %s\nbefore=%v\nafter=%v", row.Function, before, after)
		}
		if !strings.Contains(row.After, "fixture.RequireRun(ctx, runID)") {
			t.Fatalf("native journal lost canonical run construction: %s", row.Function)
		}
	}
	if len(seen) != len(nativeJournalSetupRoots) {
		t.Fatalf("native journal cohort=%d, want three", len(seen))
	}
}

func journalMutationAndAssertionCalls(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "journal.go", "package probe\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	ast.Inspect(file, func(node ast.Node) bool {
		if condition, ok := node.(*ast.IfStmt); ok && journalAssertionHasLiteralFailure(condition.Body) {
			calls = append(calls, "assert: "+normalizeJournalProof(formattedNativeReadNode(condition.Cond)))
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		name := selector.Sel.Name
		owner := formattedNativeReadNode(selector.X)
		if journalProofCall(owner, name, call) {
			calls = append(calls, normalizeJournalProof(formattedNativeReadNode(call)))
		}
		return true
	})
	return calls
}

func normalizeJournalProof(value string) string {
	return strings.NewReplacer(
		"observed.RunForkRevisions", "revisions",
		"before.StoryCount", "beforeCount", "after.StoryCount", "afterCount",
		"before.StoryHead", "beforeHead", "after.StoryHead", "afterHead",
	).Replace(value)
}

func journalAssertionHasLiteralFailure(block *ast.BlockStmt) bool {
	for _, statement := range block.List {
		expression, ok := statement.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expression.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && journalProofCall(formattedNativeReadNode(selector.X), selector.Sel.Name, call) {
			return true
		}
	}
	return false
}

func TestNativeJournalProofControlRejectsWeakenedTerminalEquality(t *testing.T) {
	before := `func proof(){ if err != nil || !reflect.DeepEqual(again, terminal) { t.Fatal("terminal changed") } }`
	after := strings.Replace(before, "err != nil || !reflect.DeepEqual(again, terminal)", "false", 1)
	if reflect.DeepEqual(journalMutationAndAssertionCalls(t, before), journalMutationAndAssertionCalls(t, after)) {
		t.Fatal("unchanged error text concealed a weakened complete-result assertion")
	}
}

func journalProofCall(owner, name string, call *ast.CallExpr) bool {
	if owner == "store" || owner == "journal" || owner == "restarted" {
		switch name {
		case "StartActivityAttempt", "CompleteActivityAttempt", "MarkActivityAttemptUncertain", "LoadActivityAttempt":
			return true
		}
	}
	if owner != "t" || len(call.Args) == 0 {
		return false
	}
	_, literal := call.Args[0].(*ast.BasicLit)
	return literal && (name == "Fatal" || name == "Fatalf")
}
