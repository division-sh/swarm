package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func nativeToolPostgresConstructionShape(t *testing.T, source string) bool {
	t.Helper()
	want := `func newPostgresHumanTaskToolStoreForTest(t *testing.T) *store.PostgresStore {
		t.Helper()
		selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)
		return selected
	}`
	return formattedNativeReadNode(projectionShapeFunction(t, source)) == formattedNativeReadNode(projectionShapeFunction(t, want))
}

func TestNativeToolPostgresConstructionRetainsCanonicalAdmissionAndCleanup(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-tool-postgres-construction")
	before := `func newPostgresHumanTaskToolStoreForTest(t *testing.T) *store.PostgresStore {
		t.Helper()
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		return storetest.AdmitPostgresRuntimeStore(t, db)
	}`
	if formattedNativeReadNode(projectionShapeFunction(t, row.Before)) != formattedNativeReadNode(projectionShapeFunction(t, before)) || !nativeToolPostgresConstructionShape(t, row.After) {
		t.Fatal("constructor admission, owner or cleanup changed outside the finite migration")
	}
	path := filepath.Join("..", "..", "..", "internal", "store", "storetest", "runtime_reopen.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	ownerRecipe := nativeReopenCloseRecipes(t)[0]
	owner, err := uniqueFunction(file, ownerRecipe.Function)
	if err != nil {
		t.Fatal(err)
	}
	if formattedNativeReadNode(owner) != formattedNativeReadNode(projectionShapeFunction(t, ownerRecipe.After)) {
		t.Fatal("native factory admission or shared close ownership differs from its exact recipe")
	}
}

func TestNativeToolPostgresConstructionRejectsLostOrSubstitutedAuthority(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-tool-postgres-construction")
	for _, probe := range []struct{ from, to string }{
		{"storetest.StartPostgresRuntimeStoreWithReopen(t)", "storetest.AdmitPostgresRuntimeStore(t, db)"},
		{"return selected", "return reconstructed"},
		{"t.Helper()", "t.Helper(); selected.Close()"},
		{"selected, _ :=", "selected, reopen :="},
	} {
		changed := strings.Replace(row.After, probe.from, probe.to, 1)
		if changed == row.After || nativeToolPostgresConstructionShape(t, changed) {
			t.Fatalf("changed ownership or cleanup admitted: %s", probe.from)
		}
	}
}

func TestNativeToolPostgresConstructionCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "runtime", "tools", "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
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
				if call, ok := node.(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "newPostgresHumanTaskToolStoreForTest" {
					counts[fn.Name.Name]++
				}
				return true
			})
		}
	}
	for _, name := range []string{
		"TestEntityTools_ReadImportedCanonicalEntityContractOnBothStores",
		"TestAskHumanCreatesTypedCardAndContinuationForImportedAgentOnBothStores",
		"TestEntitySparseGeneratedToolMutation",
		"TestSaveEntityFieldAcknowledgedErrorReturnsCommittedToolResponseOnBothStores",
		"TestRetiredCreateEntityCannotMutateEitherStore",
		"TestAskHumanAcknowledgedPostCommitErrorKeepsCardWithoutDuplicateBothStores",
	} {
		if counts[name] != 1 {
			t.Fatalf("constructor caller %s=%d, want1", name, counts[name])
		}
		delete(counts, name)
	}
	if len(counts) != 0 {
		t.Fatalf("unclassified constructor consumers: %v", counts)
	}
}
