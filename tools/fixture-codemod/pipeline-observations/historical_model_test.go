package main

import (
	"crypto/sha256"

	"fmt"
	"os"
	"path/filepath"

	"strings"
	"testing"
)

// Check today's owner first, then give unchanged mechanical oracles their
// actual historical target. The historical target is never a source rewrite.
func historicalMechanicalRecipe(t *testing.T, row recipe) recipe {
	t.Helper()
	files, changes, err := prepareFiles("../../..", []recipe{row})
	if err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("current semantic successor diverged: %s/%v", row.Function, err)
	}
	if row.Mechanical != nil {
		row.After = row.Mechanical.After
	}
	return row
}

func TestHistoricalMechanicalSnapshotsRejectInvalidAndCrossedCuts(t *testing.T) {
	row := recipe{File: "fixture.go", Function: "original", Before: "func original() { raw() }", After: "func native() { acknowledged() }", Successor: "func native() { current() }"}
	row.Mechanical = &mechanicalSnapshot{SourceCommit: strings.Repeat("1", 40), BeforeSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))), After: "func native() { mechanical() }"}
	for _, source := range []string{row.Before, row.After, row.Mechanical.After, "func native() { foreign() }", "func native() { current() }; func native() {}", "func original() {}; func native() { current() }", "func"} {
		t.Run(source, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, row.File), []byte("package probe\n"+source), 0600); err != nil {
				t.Fatal(err)
			}
			files, changes, err := prepareFiles(root, []recipe{row})
			if err == nil && len(files) == 0 && len(changes) == 0 {
				t.Fatal("missing/ambiguous/old/foreign current semantic owner accepted")
			}
		})
	}
	if _, _, err := prepareFiles(t.TempDir(), []recipe{row}); err == nil {
		t.Fatal("missing current source accepted")
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, row.File), []byte("package probe\n"+row.Successor), 0600); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"commit", "before", "after"} {
		t.Run(change, func(t *testing.T) {
			broken := row
			mechanical := *row.Mechanical
			broken.Mechanical = &mechanical
			switch change {
			case "commit":
				mechanical.SourceCommit = "unknown"
			case "before":
				mechanical.BeforeSHA256 = strings.Repeat("0", 64)
			case "after":
				mechanical.After = "func"
			}
			if _, _, err := prepareFiles(root, []recipe{broken}); err == nil {
				t.Fatal("invalid historical proof metadata accepted")
			}
		})
	}
	if files, changes, err := prepareFiles(root, []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("exact current successor not inert: %v", err)
	}
	row.Removed, row.Successor = true, ""
	if _, _, err := prepareFiles(root, []recipe{row}); err == nil {
		t.Fatal("historical receipt permitted a surviving retired owner")
	}
}

func historicalCurrentSourcePinned(source, digest string) bool {
	value, err := canonicalFunction(source)
	return err == nil && fmt.Sprintf("%x", sha256.Sum256([]byte(value))) == digest
}
