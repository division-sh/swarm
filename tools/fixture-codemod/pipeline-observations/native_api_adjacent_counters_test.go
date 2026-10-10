package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func nativeAPIAdjacentCounterRecipes(t *testing.T) []recipe {
	t.Helper()
	wanted := map[string]bool{
		"countOperatorReplayEvents": true, "countDirectiveEvents": true,
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":      true,
		"TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity": true,
		"TestOperatorAgentSendDirectivePersistsDirectiveEventOnceOnReplay": true,
	}
	var all, found []recipe
	if err := json.Unmarshal(recipeBytes, &all); err != nil {
		t.Fatal(err)
	}
	for _, row := range all {
		if wanted[row.Function] {
			found = append(found, row)
			delete(wanted, row.Function)
		}
	}
	if len(wanted) != 0 || len(found) != 5 {
		t.Fatalf("adjacent counter recipes missing/duplicate: %v/%d", wanted, len(found))
	}
	return found
}

func TestNativeAPIAdjacentCountersRetainWorkloadsContextsAndPredicates(t *testing.T) {
	for _, row := range nativeAPIAdjacentCounterRecipes(t) {
		if strings.HasPrefix(row.Function, "Test") {
			if nativeAPIPhysicalCardinalityWorkload(t, row.Before) != nativeAPIPhysicalCardinalityWorkload(t, row.After) {
				t.Fatalf("replay/directive work, context or assertion changed: %s", row.Function)
			}
			continue
		}
		want := `func countOperatorReplayEvents(t *testing.T, ctx context.Context, selected any) int {
			t.Helper()
			count, err := storetest.CountPhysicalEvents(ctx, selected)
			if err != nil { t.Fatalf("count operator replay events: %v", err) }
			return count
		}`
		if row.Function == "countDirectiveEvents" {
			want = `func countDirectiveEvents(t *testing.T, selected any) int {
				t.Helper()
				count, err := storetest.CountEventNameStorage(context.Background(), selected, "platform.agent_directive")
				if err != nil { t.Fatalf("count directive events: %v", err) }
				return count
			}`
		}
		if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
			t.Fatalf("counter predicate/context/owner/refusal changed: %s", row.Function)
		}
	}
}

func TestNativeAPIAdjacentCountersRejectChangedOwnersAndAssertions(t *testing.T) {
	probes := 0
	for _, row := range nativeAPIAdjacentCounterRecipes(t) {
		if !strings.HasPrefix(row.Function, "Test") {
			continue
		}
		for _, change := range [][2]string{
			{"countDirectiveEvents(t, pg)", "countDirectiveEvents(t, wrongOwner)"},
			{"countOperatorReplayEvents(t, ctx, pg)", "countOperatorReplayEvents(t, wrongContext, pg)"},
			{"countOperatorReplayEvents(t, ctx, f.store)", "countOperatorReplayEvents(t, ctx, wrongOwner)"},
			{"count != 1", "count < 1"}, {"got != beforeEvents", "got < beforeEvents"}, {"got != 2", "got < 2"},
		} {
			broken := strings.Replace(row.After, change[0], change[1], 1)
			if broken == row.After {
				continue
			}
			probes++
			if nativeAPIPhysicalCardinalityWorkload(t, row.After) == nativeAPIPhysicalCardinalityWorkload(t, broken) {
				t.Fatalf("weakened adjacent counter proof admitted: %s/%s", row.Function, change[0])
			}
		}
	}
	if probes != 6 {
		t.Fatalf("adjacent counter adversaries missing: %d", probes)
	}
}

func TestNativeAPIAdjacentCounterCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "apiv1", "*_test.go"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("enumerate API callers: %v/%d", err, len(paths))
	}
	actual := map[string][]string{}
	for _, path := range paths {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok && (formattedNativeReadNode(call.Fun) == "countDirectiveEvents" || formattedNativeReadNode(call.Fun) == "countOperatorReplayEvents") {
					actual[fn.Name.Name] = append(actual[fn.Name.Name], formattedNativeReadNode(call))
				}
				return true
			})
		}
	}
	expected := map[string][]string{
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":      {"countOperatorReplayEvents(t, ctx, pg)", "countOperatorReplayEvents(t, ctx, pg)"},
		"TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity": {"countOperatorReplayEvents(t, ctx, f.store)"},
		"TestOperatorAgentSendDirectivePersistsDirectiveEventOnceOnReplay": {"countDirectiveEvents(t, pg)", "countDirectiveEvents(t, pg)"},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("adjacent counter callers differ: got=%v want=%v", actual, expected)
	}
}
