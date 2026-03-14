package contracts

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestPromptSchemaGuard_EmitFieldListsMatchEventSchemas(t *testing.T) {
	if err := ValidatePromptSchemaGuards(repoRoot(t)); err != nil {
		t.Fatal(err)
	}
}

func TestPromptSchemaGuard_EmitFieldListsMatchEventSchemasForBundle(t *testing.T) {
	bundle, err := LoadWorkflowContractBundle(repoRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle: %v", err)
	}
	if err := ValidatePromptSchemaGuardsForBundle(bundle); err != nil {
		t.Fatal(err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
