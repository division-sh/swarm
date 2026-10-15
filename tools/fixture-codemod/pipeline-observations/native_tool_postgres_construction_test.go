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
	native := map[string]int{}
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
			old, current := nativeToolPostgresCallerCounts(fn)
			counts[fn.Name.Name] += old
			native[fn.Name.Name] += current
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
		if counts[name] != 0 || native[name] != 1 {
			t.Fatalf("constructor caller %s: retired=%d native=%d, want0/1", name, counts[name], native[name])
		}
		delete(counts, name)
	}
	for name, count := range counts {
		if count != 0 {
			t.Fatalf("unclassified retired constructor consumer: %s/%d", name, count)
		}
	}
}

func nativeToolPostgresCallerCounts(fn *ast.FuncDecl) (retired, native int) {
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if formattedNativeReadNode(call.Fun) == "newPostgresHumanTaskToolStoreForTest" {
			retired++
		}
		if formattedNativeReadNode(call) == "storetest.StartPostgresRuntimeStore(t)" {
			native++
		}
		return true
	})
	return retired, native
}

func TestNativeToolPostgresCallerRetirementRefusesLostDuplicateAndForeignConstruction(t *testing.T) {
	valid := "func proof(t *testing.T) { selected := storetest.StartPostgresRuntimeStore(t); _ = selected }"
	for _, sample := range []struct {
		body         string
		old, current int
	}{
		{valid, 0, 1},
		{strings.Replace(valid, "storetest.StartPostgresRuntimeStore(t)", "newPostgresHumanTaskToolStoreForTest(t)", 1), 1, 0},
		{strings.Replace(valid, "storetest.StartPostgresRuntimeStore(t)", "storetest.AdmitPostgresRuntimeStore(t, db)", 1), 0, 0},
		{strings.Replace(valid, "_ = selected", "_ = selected; storetest.StartPostgresRuntimeStore(t)", 1), 0, 2},
		{strings.Replace(valid, "storetest.StartPostgresRuntimeStore(t)", "storetest.StartPostgresRuntimeStore(other)", 1), 0, 0},
	} {
		old, current := nativeToolPostgresCallerCounts(projectionShapeFunction(t, sample.body))
		if old != sample.old || current != sample.current {
			t.Fatalf("constructor inventory admitted changed call: %s -> %d/%d", sample.body, old, current)
		}
	}
}
