package contracts

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	bundleRoot := writePromptTestBundle(t, repoRoot)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, bundleRoot, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func writePromptTestBundle(t *testing.T, repoRoot string) string {
	t.Helper()
	srcRoot := filepath.Join(repoRoot, "internal", "runtime", "testdata", "generic-swarm-bundle")
	dstRoot := filepath.Join(t.TempDir(), "prompt-test-bundle")
	copyTree(t, srcRoot, dstRoot)

	agentsPath := filepath.Join(dstRoot, "agents.yaml")
	agentsRaw, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatalf("read %s: %v", agentsPath, err)
	}
	agentsRaw = append(agentsRaw, []byte(strings.TrimLeft(`
ops-lead:
  id: ops-lead
  role: ops_lead
  manager_fallback: control-plane
  emit_events:
    - item.created
`, "\n"))...)
	if err := os.WriteFile(agentsPath, agentsRaw, 0o644); err != nil {
		t.Fatalf("write %s: %v", agentsPath, err)
	}

	promptsDir := filepath.Join(dstRoot, "prompts")
	if err := os.MkdirAll(promptsDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", promptsDir, err)
	}
	prompt := strings.TrimSpace(`
You are the Operations Lead for {{team_name}}.

When you call emit_item_created, include item_id.
`)
	if err := os.WriteFile(filepath.Join(promptsDir, "ops-lead.md"), []byte(prompt+"\n"), 0o644); err != nil {
		t.Fatalf("write prompt fixture: %v", err)
	}

	return dstRoot
}

func copyTree(t *testing.T, srcRoot, dstRoot string) {
	t.Helper()
	if err := filepath.Walk(srcRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcRoot, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dstRoot, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	}); err != nil {
		t.Fatalf("copy %s -> %s: %v", srcRoot, dstRoot, err)
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
