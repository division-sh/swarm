package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
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
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	files, err := discoverScenarioTestFiles(&runtimecontracts.WorkflowContractBundle{SourceArtifact: artifact}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "tests/derived.yaml" {
		t.Fatalf("discovered scenarios = %#v, want only tests/derived.yaml", files)
	}
}

func TestDiscoverScenarioTestFilesUsesCanonicalTestsResourceBranches(t *testing.T) {
	root := t.TempDir()
	for _, label := range []string{
		"tests/root.yaml",
		"orders/tests/child.yaml",
		"data/tests/root-fixture.yaml",
		"mocks/tests/root-case.yaml",
		"orders/data/tests/child-fixture.yaml",
	} {
		target := filepath.Join(root, filepath.FromSlash(label))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte("version: 1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{SourceArtifact: artifact}
	files, err := discoverScenarioTestFiles(bundle, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Path != "orders/tests/child.yaml" || files[0].FlowID != "orders" || files[1].Path != "tests/root.yaml" || files[1].FlowID != "." {
		t.Fatalf("discovered scenarios = %#v, want only canonical root and child tests branches", files)
	}
	if _, err := discoverScenarioTestFiles(bundle, []string{"data/tests/root-fixture.yaml"}); err == nil || !strings.Contains(err.Error(), "not an admitted YAML member of a tests/ resource branch") {
		t.Fatalf("explicit nested resource scenario error = %v", err)
	}
}
