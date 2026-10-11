package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReviewedSnapshotsAreFiniteAndIdempotent(t *testing.T) {
	var recipes []recipe
	if err := json.Unmarshal(recipeBytes, &recipes); err != nil {
		t.Fatal(err)
	}
	if len(recipes) != 893 {
		t.Fatalf("recipe count=%d", len(recipes))
	}
	for _, row := range recipes {
		t.Run(row.File+"/"+row.Function, func(t *testing.T) {
			source := []byte("package proof\n" + row.Before)
			if row.Removed && row.Before == row.After {
				if _, err := checkpointRecipes(source, []recipe{row}); err == nil {
					t.Fatal("pure retirement accepted its surviving original capability")
				}
				files, changes, err := prepareFiles("../../..", []recipe{row})
				if err != nil || len(files) != 0 || len(changes) != 0 {
					t.Fatalf("pure retirement is not absent and inert: %v", err)
				}
				return
			}
			updated, changed, err := rewriteFunction(row.File, source, row)
			if err != nil || !changed {
				t.Fatalf("rewrite: changed=%t err=%v", changed, err)
			}
			again, changed, err := rewriteFunction(row.File, updated, row)
			if err != nil || changed || !bytes.Equal(again, updated) {
				t.Fatalf("idempotence: changed=%t err=%v", changed, err)
			}
		})
	}
}

func TestSnapshotRewritePreservesExplicitStoretestImportWithoutDuplication(t *testing.T) {
	row := recipe{Function: "observe", Before: "func observe() { storetest.StartSQLiteRuntimeStore(oldTest) }", After: "func observe() { storetest.StartSQLiteRuntimeStore(newTest) }"}
	for _, alias := range []string{"", "storetest "} {
		source := []byte("package proof\nimport " + alias + "\"github.com/division-sh/swarm/internal/store/storetest\"\n" + row.Before)
		result, changes, err := rewriteSource("proof.go", source, []recipe{row})
		if err != nil || len(changes) != 1 {
			t.Fatalf("aliased import rewrite: %d changes, %v", len(changes), err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), "proof.go", result, parser.ImportsOnly)
		if err != nil || len(file.Imports) != 1 || file.Imports[0].Path.Value != `"github.com/division-sh/swarm/internal/store/storetest"` {
			t.Fatalf("duplicate or altered native import: %s, %v", result, err)
		}
	}
}

func TestReviewedSnapshotCandidateOverlayTypeChecks(t *testing.T) {
	// This reviewed candidate includes the explicitly tagged #2413 fixture.
	// The production preflight still refuses files outside its active build view.
	t.Setenv("GOFLAGS", "-tags=issue2413")
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	grouped := map[string][]recipe{}
	for _, row := range rows {
		grouped[row.File] = append(grouped[row.File], row)
	}
	var files []pendingFile
	applied := 0
	expected := 0
	for path, recipes := range grouped {
		absolute := filepath.Join(root, path)
		source, err := os.ReadFile(absolute)
		if err != nil {
			t.Fatal(err)
		}
		recipes, err = checkpointRecipes(source, recipes)
		if err != nil {
			t.Fatal(err)
		}
		expected += len(recipes)
		for _, row := range recipes {
			inverse := recipe{Function: replacementFunctionName(t, row), Before: row.After, After: row.Before}
			var changed bool
			source, changed, err = rewriteFunction(path, source, inverse)
			if err != nil || !changed {
				t.Fatalf("prepare actual predecessor %s/%s: changed=%t err=%v", path, row.Function, changed, err)
			}
		}
		candidate, changed, err := rewriteSource(path, source, recipes)
		if err != nil || len(changed) != len(recipes) {
			t.Fatalf("complete candidate overlay %s: changes=%d err=%v", path, len(changed), err)
		}
		applied += len(changed)
		files = append(files, pendingFile{path: absolute, data: candidate})
	}
	if applied != expected {
		t.Fatalf("candidate applied %d current recipes, want %d", applied, expected)
	}
	if err := checkTypes(root, files); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerRetirementSnapshotsRejectSurvivingAndAmbiguousCapabilities(t *testing.T) {
	row := recipe{Function: "original", Before: "func original() { work() }", After: "func native() { work() }", Removed: true}
	for _, source := range []string{"func original() { work() }", "func native() { work() }", "func original() {}; func native() {}", "func"} {
		if _, err := checkpointRecipes([]byte("package probe\n"+source), []recipe{row}); err == nil {
			t.Fatalf("retired capability survived: %s", source)
		}
	}
	if current, err := checkpointRecipes([]byte("package probe\nfunc different() {}"), []recipe{row}); err != nil || len(current) != 0 {
		t.Fatalf("retired capability absent: %v/%v", current, err)
	}
	root := t.TempDir()
	row.File = "retired.go"
	if _, changes, err := prepareFiles(root, []recipe{row}); err != nil || len(changes) != 0 {
		t.Fatalf("whole file retirement: %v/%v", changes, err)
	}
	if _, _, err := prepareFiles(root, []recipe{row, {File: row.File, Function: "required"}}); err == nil {
		t.Fatal("retirement hid a required current consumer")
	}
	for _, broken := range []recipe{
		{File: row.File, Function: row.Function, Before: "broken", After: row.After, Removed: true},
		{File: row.File, Function: row.Function, Before: row.Before, After: row.After, Successor: row.After, Removed: true},
	} {
		if _, _, err := prepareFiles(root, []recipe{broken}); err == nil {
			t.Fatal("missing retired file hid invalid snapshot metadata")
		}
	}
}

func TestOwnerSuccessorSnapshotsArePinnedAndKeepHistoricalTransformation(t *testing.T) {
	row := recipe{Function: "original", Before: "func original() { raw() }", After: "func native() { local() }", Successor: "func native() { selected() }"}
	current, err := checkpointRecipes([]byte("package probe\n"+row.Successor), []recipe{row})
	if err != nil || len(current) != 1 || current[0].Before != row.After || current[0].After != row.Successor {
		t.Fatalf("successor cut changed: %v/%v", current, err)
	}
	for _, source := range []string{row.Before, "func native() { raw() }", "func native() { selected(); extra() }"} {
		if _, changed, err := rewriteFunction("probe.go", []byte("package probe\n"+source), current[0]); err == nil || changed {
			t.Fatalf("unreviewed successor admitted: %s", source)
		}
	}
	if _, changed, err := rewriteFunction("probe.go", []byte("package probe\n"+row.Before), row); err != nil || !changed {
		t.Fatalf("historical transformation lost: %t/%v", changed, err)
	}
}

func replacementFunctionName(t *testing.T, row recipe) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "after.go", "package proof\n"+row.After, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	return file.Decls[0].(*ast.FuncDecl).Name.Name
}

func TestRenamedSnapshotRejectsAmbiguousAndAlteredReplacements(t *testing.T) {
	row := recipe{Function: "original", Before: "func original() { proof() }", After: "func native() { proof() }"}
	for _, source := range []string{
		"func original() { proof() }; func native() { proof() }",
		"func native() { other() }",
		"func native() { proof() }; func native() { proof() }",
	} {
		if _, changed, err := rewriteFunction("proof.go", []byte("package proof\n"+source), row); err == nil || changed {
			t.Fatalf("ambiguous or altered renamed proof admitted: %s", source)
		}
	}
}

func TestSnapshotRewriteRejectsChangedBindingsAndExtraWork(t *testing.T) {
	row := recipe{Function: "observe", Before: "func observe(owner any) { consume(owner) }", After: "func observe(owner any) { read(owner) }"}
	for _, body := range []string{
		"func observe(other any) { consume(other) }",
		"func observe(owner any) { owner = replacement(); consume(owner) }",
		"func observe(owner any) { extra(); consume(owner) }",
		"func sibling(owner any) { consume(owner) }",
		"func observe(owner any) { consume(owner) }; func (x X) observe(owner any) { consume(owner) }",
		"func observe(owner any) {",
	} {
		if result, changed, err := rewriteFunction("proof.go", []byte("package proof\n"+body), row); err == nil || changed || result != nil {
			t.Fatalf("unknown/ambiguous source was admitted: %s changed=%t err=%v", body, changed, err)
		}
	}
}

func TestSnapshotRewriteAllowsOnlyInertLayoutChanges(t *testing.T) {
	row := recipe{Function: "observe", Before: "func observe(owner any) { consume(owner) }", After: "func observe(owner any) { read(owner) }"}
	source := []byte("package proof\n\n// shifted header\nfunc observe(owner any) {\n// inert line\nconsume(owner)\n}")
	if _, changed, err := rewriteFunction("proof.go", source, row); err != nil || !changed {
		t.Fatalf("layout: changed=%t err=%v", changed, err)
	}
}

func TestCompletePreflightDoesNotWriteAnEarlierValidFile(t *testing.T) {
	root := t.TempDir()
	before := []byte("package proof\nfunc observe(owner any) { consume(owner) }")
	for _, path := range []string{"a.go", "z.go"} {
		if err := os.WriteFile(filepath.Join(root, path), before, 0644); err != nil {
			t.Fatal(err)
		}
	}
	rows := []recipe{
		{File: "a.go", Function: "observe", Before: "func observe(owner any) { consume(owner) }", After: "func observe(owner any) { read(owner) }"},
		{File: "z.go", Function: "observe", Before: "func observe(owner any) { different(owner) }", After: "func observe(owner any) { read(owner) }"},
	}
	if files, changes, err := prepareFiles(root, rows); err == nil || files != nil || changes != nil {
		t.Fatalf("partial preflight: files=%v changes=%v err=%v", files, changes, err)
	}
	actual, err := os.ReadFile(filepath.Join(root, "a.go"))
	if err != nil || !bytes.Equal(actual, before) {
		t.Fatalf("earlier file changed: %s err=%v", actual, err)
	}
}

func TestRecipeSourceAndSyntaxErrorsFailClosed(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{"../foreign.go", "./foreign.go", "missing.go"} {
		if _, _, err := prepareFiles(root, []recipe{{File: path}}); err == nil {
			t.Fatalf("accepted unavailable/noncanonical path %s", path)
		}
	}
	for _, source := range []string{"broken", "var different int", "func one() {}; func two() {}"} {
		if _, err := canonicalFunction(source); err == nil {
			t.Fatalf("accepted invalid recipe: %s", source)
		}
	}
	if _, err := canonicalFunction("func one() { text := \"retained\"; _ = text }"); err != nil {
		t.Fatal(err)
	}
	if _, err := uniqueFunction(&ast.File{}, "absent"); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing function error=%v", err)
	}
}
