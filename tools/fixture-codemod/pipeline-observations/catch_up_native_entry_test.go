package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func nativeEntryConformanceRecipeInputs(t *testing.T) ([]byte, []recipe) {
	t.Helper()
	current, err := os.ReadFile(filepath.Join("../../..", selectedreadrole.NativeEntry2542File))
	if err != nil {
		t.Fatal(err)
	}
	var all []recipe
	if err := json.Unmarshal(recipeBytes, &all); err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	for _, row := range all {
		if row.File == selectedreadrole.NativeEntry2542File {
			rows = append(rows, row)
		}
	}
	if len(rows) != 15 {
		t.Fatalf("conformance recipe inventory=%d want15", len(rows))
	}
	return current, rows
}

func TestCatchUp2542NativeEntryPreservesExactHistoryAndCurrentSource(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, path := range []string{
		"tools/fixture-codemod/pipeline-observations/recipes.json",
		"tools/fixture-codemod/pipeline-observations/family145-transitions.json",
		"tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/catch_up_2542.json",
		"tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/catch_up_2542_before_2672.json",
	} {
		prior, err := exec.Command("git", "-C", root, "show", selectedreadrole.NativeEntry2542Prior+":"+path).Output()
		if err != nil {
			t.Fatal(err)
		}
		current, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || !bytes.Equal(prior, current) {
			t.Errorf("immutable historical corpus changed: %s: %v", path, err)
		}
	}
	current, rows := nativeEntryConformanceRecipeInputs(t)
	unchangedRows := append([]recipe(nil), rows...)
	prior, err := selectedreadrole.NativeEntry2542HistoricalSource(root, selectedreadrole.NativeEntry2542File, current)
	if err != nil || bytes.Equal(prior, current) {
		t.Fatal("exact current composition lacks its predecessor:", err)
	}
	expected, err := exec.Command("git", "-C", root, "show", selectedreadrole.NativeEntry2542Prior+":"+selectedreadrole.NativeEntry2542File).Output()
	if err != nil || !bytes.Equal(prior, expected) {
		t.Fatal("complete signed predecessor changed:", err)
	}
	adapted, err := nativeEntryCurrentRecipes(root, selectedreadrole.NativeEntry2542File, current, rows)
	if err != nil {
		t.Fatal(err)
	}
	out, changed, err := rewriteSource(selectedreadrole.NativeEntry2542File, current, adapted)
	if err != nil || len(changed) != 0 || !bytes.Equal(out, current) {
		t.Fatal("current native setup was rewritten or downgraded:", err)
	}
	if !reflect.DeepEqual(rows, unchangedRows) {
		t.Fatal("current-source comparison mutated immutable recipe rows")
	}
	if _, changed, err := rewriteSource(selectedreadrole.NativeEntry2542File, prior, rows); err != nil || len(changed) != 0 {
		t.Fatal("unchanged prior recipes lost idempotence:", err)
	}
}

func TestCatchUp2542NativeEntryRefusesChangedSourceAndRecipePairs(t *testing.T) {
	current, rows := nativeEntryConformanceRecipeInputs(t)
	for _, mutation := range []string{"whitespace", "assertion", "raw-handle", "foreign-path"} {
		bad, path := append([]byte(nil), current...), selectedreadrole.NativeEntry2542File
		switch mutation {
		case "whitespace":
			bad = append(bad, '\n')
		case "assertion":
			bad = bytes.Replace(bad, []byte("exact duplicate changed lifecycle"), []byte("foreign assertion"), 1)
		case "raw-handle":
			bad = append(bad, []byte("\nvar rawAuthority *sql.DB\n")...)
		case "foreign-path":
			path += ".foreign"
		}
		if mutation != "foreign-path" && bytes.Equal(bad, current) {
			t.Fatal("hostile source mutation did not apply:", mutation)
		}
		prior, err := selectedreadrole.NativeEntry2542HistoricalSource("../../..", path, bad)
		if err == nil && !bytes.Equal(prior, bad) {
			t.Error("changed/foreign source received a historical certificate:", mutation)
		}
	}
	for _, mutation := range []string{"before", "after", "family", "function"} {
		bad := append([]recipe(nil), rows...)
		switch mutation {
		case "before":
			bad[0].Before += "\n"
		case "after":
			bad[0].After = strings.Replace(bad[0].After, "{", "{ foreignAuthority();", 1)
		case "family":
			bad[0].Family += "-foreign"
		case "function":
			bad[0].Function += "Foreign"
		}
		if _, err := nativeEntryCurrentRecipes("../../..", selectedreadrole.NativeEntry2542File, current, bad); err == nil {
			t.Error("changed recipe admitted into exact current view:", mutation)
		}
	}
}
