package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"strings"
	"testing"
)

func nativeReopenCloseRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == "native-runtime-reopen-close-owner" {
			found = append(found, row)
		}
	}
	if len(found) != 2 || found[0].Function != "StartPostgresRuntimeStoreWithReopen" || found[1].Function != "StartSQLiteRuntimeStoreWithReopen" {
		t.Fatal("reopen close ownership requires exactly both existing native fixtures")
	}
	return found
}

func nativeReopenCloseAfter(t *testing.T, row recipe) (string, error) {
	t.Helper()
	typeName, backend := "PostgresStore", "postgres"
	if row.Function == "StartSQLiteRuntimeStoreWithReopen" {
		typeName, backend = "SQLiteRuntimeStore", "sqlite"
	}
	fn := projectionShapeFunction(t, row.Before)
	old := projectionShapeStatement(t, fmt.Sprintf(`t.Cleanup(func() {
if err := selected.Close(); err != nil { t.Errorf("close %s runtime store: %%v", err) }
})`, backend))
	closeOwner := projectionShapeStatement(t, fmt.Sprintf(`t.Cleanup(func() {
ownersMu.Lock()
defer ownersMu.Unlock()
for i := len(owners)-1; i >= 0; i-- {
if err := owners[i].Close(); err != nil { t.Errorf("close %s runtime store: %%v", err) }
}
})`, backend))
	var body []ast.Stmt
	found := 0
	for _, statement := range fn.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if ok && len(assignment.Lhs) == 1 && formattedNativeReadNode(assignment.Lhs[0]) == "open" && len(assignment.Rhs) == 1 {
			open, ok := assignment.Rhs[0].(*ast.FuncLit)
			if !ok {
				return "", fmt.Errorf("native opener is not a function")
			}
			var inner []ast.Stmt
			for _, child := range open.Body.List {
				if formattedNativeReadNode(child) == formattedNativeReadNode(old) {
					found++
					inner = append(inner, projectionShapeStatement(t, "ownersMu.Lock()"),
						projectionShapeStatement(t, "owners = append(owners, selected)"), projectionShapeStatement(t, "ownersMu.Unlock()"))
				} else {
					inner = append(inner, child)
				}
			}
			open.Body.List = inner
			body = append(body, projectionShapeStatement(t, "var ownersMu sync.Mutex"),
				projectionShapeStatement(t, "var owners []*store."+typeName), closeOwner)
		}
		body = append(body, statement)
	}
	if found != 1 {
		return "", fmt.Errorf("native reopen cleanup count=%d, want one", found)
	}
	fn.Body.List = body
	return canonicalFunction(formattedNativeReadNode(fn))
}

func TestNativeRuntimeReopenRecipePreservesLocationBootstrapAndAdmission(t *testing.T) {
	for _, row := range nativeReopenCloseRecipes(t) {
		expected, err := nativeReopenCloseAfter(t, row)
		actual, parseErr := canonicalFunction(row.After)
		if err != nil || parseErr != nil || expected != actual {
			t.Fatalf("%s changed native location, bootstrap or admission: %v / %v", row.Function, err, parseErr)
		}
		for _, pair := range [][2]string{
			{"bindTestPayloadAdmitter(selected)", ""},
			{"owners = append(owners, selected)", "owners = append(owners, foreignStore)"},
			{"owners[i].Close()", "selected.Close()"},
			{"return open(), open", "return open(), foreignReopen"},
		} {
			changed := strings.Replace(row.After, pair[0], pair[1], 1)
			if changed == row.After {
				t.Fatalf("negative control did not change source: %v", pair)
			}
			actual, err := canonicalFunction(changed)
			if err == nil && actual == expected {
				t.Fatalf("native admission, exact owner or reopener lost: %v", pair)
			}
		}
	}
}
