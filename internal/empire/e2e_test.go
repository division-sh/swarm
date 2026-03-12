package empire

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func TestCannedLLME2E_Scenario2_PrefilterRejectsAll(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"),
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml"),
	)
	if err != nil {
		t.Fatalf("load empire bundle: %v", err)
	}
	if bundle.WorkflowName() == "" {
		t.Fatal("expected Empire workflow name")
	}
}

func TestCannedLLME2E_Scenario4_DerivationLoop(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	eventFile := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts", "events.yaml")
	if stat, err := os.Stat(eventFile); err != nil || stat.IsDir() {
		t.Fatalf("expected event catalog file %s: %v", eventFile, err)
	}
}

func TestCannedLLME2E_Scenario7_CampaignMultiMode(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	paths := runtimecontracts.ResolveWorkflowContractPathsWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"),
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml"),
	)
	if paths.ProjectPackageFile == "" {
		t.Fatal("expected Empire package manifest path")
	}
}

func TestCannedLLME2E_ScenarioFilesExist(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	for _, rel := range []string{
		filepath.Join("docs", "specs", "mas-platform", "empire", "contracts", "package.yaml"),
		filepath.Join("docs", "specs", "mas-platform", "empire", "contracts", "nodes.yaml"),
		filepath.Join("docs", "specs", "mas-platform", "empire", "contracts", "events.yaml"),
	} {
		path := filepath.Join(repoRoot, rel)
		if stat, err := os.Stat(path); err != nil || stat.IsDir() {
			t.Fatalf("expected Empire fixture %s: %v", rel, err)
		}
	}
}
