package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"
)

func nativeAPISQLiteCounterRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == "native-api-sqlite-shared-counter-consumer" {
			found = append(found, row)
		}
	}
	if len(found) != 5 {
		t.Fatalf("SQLite counter consumers=%d, want5", len(found))
	}
	return found
}

func nativeAPISQLiteCounterWorkload(t *testing.T, source string) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	mapping := map[string]string{
		"countSQLiteEventsByName":       "countEventsByName",
		"countSQLiteAllRunRows":         "countAllRunRows",
		"countSQLiteAllEventRows":       "countAllEventRows",
		"countSQLiteAPIIdempotencyRows": "countAPIIdempotencyRows",
		"countSQLiteEventRowsByRunID":   "countEventRowsByRunID",
	}
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := formattedNativeReadNode(call.Fun)
		if next, ok := mapping[name]; ok {
			if len(call.Args) < 2 {
				t.Fatal("missing exact original selected owner")
			}
			getter, ok := call.Args[1].(*ast.CallExpr)
			if !ok || len(getter.Args) != 1 || formattedNativeReadNode(getter.Fun) != "storetest.DatabaseForTest" {
				t.Fatal("unknown SQLite counter authority")
			}
			call.Fun = ast.NewIdent(next)
			call.Args[1] = getter.Args[0]
		}
		return true
	})
	return formattedNativeReadNode(fn)
}

func TestNativeAPISQLiteCounterRetirementPreservesCompleteWorkloadsAndKeys(t *testing.T) {
	for _, row := range nativeAPISQLiteCounterRecipes(t) {
		if nativeAPISQLiteCounterWorkload(t, row.Before) != nativeAPISQLiteCounterWorkload(t, row.After) {
			t.Fatalf("SQLite source/request/refusal/replay/input/assertion changed: %s", row.Function)
		}
	}
}

func TestNativeAPISQLiteCounterRetirementRejectsChangedCountKeyOwnerOrAssertions(t *testing.T) {
	probes := 0
	for _, row := range nativeAPISQLiteCounterRecipes(t) {
		for _, change := range [][2]string{
			{"(t, sqliteStore", "(t, anotherOwner"},
			{"(t, selected", "(t, anotherOwner"},
			{"count != 1", "count < 1"},
			{"got != 0", "got > 0"},
			{`"scan.requested"`, `"unowned.requested"`},
		} {
			broken := strings.Replace(row.After, change[0], change[1], 1)
			if broken == row.After {
				continue
			}
			probes++
			if nativeAPISQLiteCounterWorkload(t, row.Before) == nativeAPISQLiteCounterWorkload(t, broken) {
				t.Fatalf("changed SQLite proof admitted: %s/%s", row.Function, change[0])
			}
		}
	}
	if probes < 5 {
		t.Fatalf("hostile probes missing: %d", probes)
	}
}
