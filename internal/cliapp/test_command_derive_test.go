package cliapp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverScenarioTestFilesIncludesExplicitDerivedDeclaration(t *testing.T) {
	root := t.TempDir()
	testsDir := filepath.Join(root, "tests")
	if err := os.MkdirAll(testsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	derived := filepath.Join(testsDir, "derived.yaml")
	if err := os.WriteFile(derived, []byte("name: explicit derive\nderive:\n  flow: work\n  input: request\n  payload:\n    generate: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(testsDir, "not-a-scenario.yaml"), []byte("name: ignored contract document\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	files, err := discoverScenarioTestFiles(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != derived {
		t.Fatalf("discovered scenarios = %#v, want only %s", files, derived)
	}
}
