package contracts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveWorkflowContractPaths_PrefersWorkflowScopedLayout(t *testing.T) {
	repoRoot := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "workflow-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "hooks-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "nodes-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "events-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "agents-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "tools-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "policy-empire.yaml"))
	if err := os.MkdirAll(filepath.Join(repoRoot, "contracts", "empire", "prompts"), 0o755); err != nil {
		t.Fatalf("mkdir prompts dir: %v", err)
	}

	paths := ResolveWorkflowContractPaths(repoRoot)
	if want := filepath.Join(repoRoot, "contracts", "empire", "workflow-empire.yaml"); paths.WorkflowSchemaFile != want {
		t.Fatalf("workflow schema path = %s, want %s", paths.WorkflowSchemaFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "empire", "hooks-empire.yaml"); paths.GuardRegistryFile != want {
		t.Fatalf("guard registry path = %s, want %s", paths.GuardRegistryFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "empire", "prompts"); paths.PromptsDir != want {
		t.Fatalf("prompts dir = %s, want %s", paths.PromptsDir, want)
	}
}

func TestResolveWorkflowContractPaths_FallsBackToLegacyLayout(t *testing.T) {
	repoRoot := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	if err := os.MkdirAll(filepath.Join(repoRoot, "contracts", "prompts"), 0o755); err != nil {
		t.Fatalf("mkdir legacy prompts dir: %v", err)
	}

	paths := ResolveWorkflowContractPaths(repoRoot)
	if want := filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"); paths.WorkflowSchemaFile != want {
		t.Fatalf("workflow schema path = %s, want %s", paths.WorkflowSchemaFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"); paths.GuardRegistryFile != want {
		t.Fatalf("guard registry path = %s, want %s", paths.GuardRegistryFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "prompts"); paths.PromptsDir != want {
		t.Fatalf("prompts dir = %s, want %s", paths.PromptsDir, want)
	}
}

func mustWriteTestFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte("test: true\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
