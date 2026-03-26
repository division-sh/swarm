package contracts

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestPromptSchemaGuard_EmitFieldListsMatchEventSchemas(t *testing.T) {
	repoRoot := repoRoot(t)
	if err := ValidatePromptSchemaGuardsForBundle(loadPromptTestBundle(t, repoRoot)); err != nil {
		t.Fatal(err)
	}
}

func TestPromptSchemaGuard_EmitFieldListsMatchEventSchemasForBundle(t *testing.T) {
	bundle := loadPromptTestBundle(t, repoRoot(t))
	if err := ValidatePromptSchemaGuardsForBundle(bundle); err != nil {
		t.Fatal(err)
	}
}

func loadPromptTestBundle(t *testing.T, repoRoot string) *WorkflowContractBundle {
	t.Helper()
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"), DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
