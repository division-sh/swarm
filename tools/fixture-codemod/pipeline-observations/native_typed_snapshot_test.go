package main

import (
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeTypedSnapshotReadWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		astutil.Apply(body, func(cursor *astutil.Cursor) bool {
			call, ok := cursor.Node().(*ast.CallExpr)
			if ok && formattedNativeReadNode(call.Fun) == "encodeSelectedForkSnapshotRow" {
				if formattedNativeReadNode(call.Args[1]) != "len(columns)" {
					t.Fatal("physical predecessor column binding changed")
				}
				call.Args[1] = ast.NewIdent("columns")
			}
			return true
		}, nil)
	}
	return formattedNativeReadNode(body)
}

func TestNativeTypedSnapshotRecipePreservesPhysicalReadCleanupAndSort(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-typed-snapshot-read")
	if nativeTypedSnapshotReadWorkload(t, row.Before, true) != nativeTypedSnapshotReadWorkload(t, row.After, false) {
		t.Fatal("closed physical query, complete rows, late errors, close or sorting changed")
	}
	for _, required := range []string{"rows.Err()", "rows.Close()", "sort.Strings(evidence.Rows)", "append(evidence.Rows, encoded)"} {
		changed := strings.ReplaceAll(row.After, required, "omittedSnapshotProof()")
		if nativeTypedSnapshotReadWorkload(t, row.Before, true) == nativeTypedSnapshotReadWorkload(t, changed, false) {
			t.Fatalf("lost physical snapshot completeness admitted: %s", required)
		}
	}
}

func TestNativeTypedSnapshotRecipePreservesScanAndCompleteColumnBindings(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-typed-snapshot-encoding")
	before := projectionShapeFunction(t, row.Before).Body
	after := projectionShapeFunction(t, row.After).Body
	if len(before.List) != 6 || len(after.List) != 4 {
		t.Fatal("unexpected finite physical encoding recipe")
	}
	before.List = before.List[:3]
	after.List = after.List[:3]
	astutil.Apply(before, func(cursor *astutil.Cursor) bool {
		call, ok := cursor.Node().(*ast.CallExpr)
		if ok && formattedNativeReadNode(call.Fun) == "make" {
			call.Args[1] = mutationSeedExpression(t, "len(columns)")
		}
		return true
	}, nil)
	if formattedNativeReadNode(before) != formattedNativeReadNode(after) || !strings.Contains(row.After, "return encodeSelectedForkSnapshotValues(columns, values)") {
		t.Fatal("physical pointer bindings or fail-closed scan changed")
	}
}
