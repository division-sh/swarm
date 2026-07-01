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

func TestDerivePromptSchemaGuards_UsesCanonicalPromptResolution(t *testing.T) {
	repoRoot := repoRoot(t)
	root := writePromptTestBundle(t, repoRoot)

	agentsPath := filepath.Join(root, "agents.yaml")
	agentsRaw, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatalf("read %s: %v", agentsPath, err)
	}
	agentsRaw = append(agentsRaw, []byte(strings.TrimLeft(`
schema-ref-agent:
  role: schema_ref
  mode: task
  prompt_ref: shared-schema-prompt
  emit_events:
    - schema.prompt.created
`, "\n"))...)
	if err := os.WriteFile(agentsPath, agentsRaw, 0o644); err != nil {
		t.Fatalf("write %s: %v", agentsPath, err)
	}
	if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte(strings.TrimLeft(`
schema.prompt.created:
  item_id: string
  required: [item_id]
`, "\n")), 0o644); err != nil {
		t.Fatalf("write events.yaml: %v", err)
	}
	prompt := strings.TrimSpace(`
When you call emit_schema_prompt_created with:
- item_id: the created item id
`)
	if err := os.WriteFile(filepath.Join(root, "prompts", "shared-schema-prompt.md"), []byte(prompt+"\n"), 0o644); err != nil {
		t.Fatalf("write shared schema prompt: %v", err)
	}

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	cases := DerivePromptSchemaGuards(bundle)
	found := false
	for _, tc := range cases {
		if filepath.Base(tc.PromptFile) == "shared-schema-prompt.md" && tc.EmitTool == "emit_schema_prompt_created" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected prompt schema guard case for shared-schema-prompt.md, got %#v", cases)
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
  mode: task
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
