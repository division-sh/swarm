package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAgentEntityWriteRule_UnmarshalYAML(t *testing.T) {
	t.Run("all", func(t *testing.T) {
		var rule AgentEntityWriteRule
		if err := yaml.Unmarshal([]byte("all\n"), &rule); err != nil {
			t.Fatalf("yaml.Unmarshal: %v", err)
		}
		if !rule.All {
			t.Fatal("expected All to be true")
		}
		if len(rule.Fields) != 0 {
			t.Fatalf("Fields = %#v, want nil", rule.Fields)
		}
	})

	t.Run("explicit list", func(t *testing.T) {
		var rule AgentEntityWriteRule
		if err := yaml.Unmarshal([]byte("- one\n- two\n"), &rule); err != nil {
			t.Fatalf("yaml.Unmarshal: %v", err)
		}
		if rule.All {
			t.Fatal("expected All to be false")
		}
		if !reflect.DeepEqual(rule.Fields, []string{"one", "two"}) {
			t.Fatalf("Fields = %#v", rule.Fields)
		}
	})
}

func TestDerivePromptEntityWriteEvidence(t *testing.T) {
	repo := repoRoot(t)
	root := writePromptTestBundle(t, repo)

	agentsPath := filepath.Join(root, "agents.yaml")
	agentsRaw, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatalf("read %s: %v", agentsPath, err)
	}
	agentsRaw = append(agentsRaw, []byte(`
writer:
  id: writer
  role: writer
  workspace_class: factory
  manager_fallback: ops-lead
`)...)
	if err := os.WriteFile(agentsPath, agentsRaw, 0o644); err != nil {
		t.Fatalf("write %s: %v", agentsPath, err)
	}

	prompt := "Call create_entity using the delivered schema.\nThen call save_entity_field for `business_brief`.\n"
	if err := os.WriteFile(filepath.Join(root, "prompts", "writer.md"), []byte(prompt), 0o644); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	got, err := DerivePromptEntityWriteEvidence(bundle)
	if err != nil {
		t.Fatalf("DerivePromptEntityWriteEvidence: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	if got[0].AgentID != "writer" || !got[0].CreateEntity || !got[0].SaveEntity {
		t.Fatalf("evidence = %#v", got[0])
	}
	if !reflect.DeepEqual(got[0].SaveFields, []string{"business_brief"}) {
		t.Fatalf("SaveFields = %#v", got[0].SaveFields)
	}
}
