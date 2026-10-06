package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
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
	if len(recipes) != 53 {
		t.Fatalf("recipe count=%d", len(recipes))
	}
	for _, row := range recipes {
		t.Run(row.File+"/"+row.Function, func(t *testing.T) {
			source := []byte("package proof\n" + row.Before)
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
