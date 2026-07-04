package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
  mode: task
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

func TestExtractPromptEntityWriteEvidence_SkipsToolToken(t *testing.T) {
	_, saveEntity, saveFields := extractPromptEntityWriteEvidence("Use `save_entity_field` for `business_brief`.\n")
	if !saveEntity {
		t.Fatal("expected save_entity_field evidence")
	}
	if !reflect.DeepEqual(saveFields, []string{"business_brief"}) {
		t.Fatalf("SaveFields = %#v", saveFields)
	}
}

func TestExtractPromptEntityWriteEvidence_IncludesDottedFields(t *testing.T) {
	_, saveEntity, saveFields := extractPromptEntityWriteEvidence("Use `save_entity_field` for `metadata.region` and `business_brief`.\n")
	if !saveEntity {
		t.Fatal("expected save_entity_field evidence")
	}
	if !reflect.DeepEqual(saveFields, []string{"metadata.region", "business_brief"}) {
		t.Fatalf("SaveFields = %#v", saveFields)
	}
}

func TestExtractPromptEntityWriteEvidence_IncludesMultilineDottedFields(t *testing.T) {
	_, saveEntity, saveFields := extractPromptEntityWriteEvidence("Use `save_entity_field`.\n- `metadata.region`\n- `business_brief`\n")
	if !saveEntity {
		t.Fatal("expected save_entity_field evidence")
	}
	if !reflect.DeepEqual(saveFields, []string{"metadata.region", "business_brief"}) {
		t.Fatalf("SaveFields = %#v", saveFields)
	}
}

func TestDerivePromptEntityWriteEvidence_IncludesScopedDuplicateAgentIDs(t *testing.T) {
	repo := repoRoot(t)
	root := t.TempDir()

	writePromptWriterFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: prompt-entity-writes-duplicate
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: alpha
    flow: alpha
    mode: static
  - id: beta
    flow: beta
    mode: static
`)
	writePromptWriterFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: prompt-entity-writes-duplicate\n")
	writePromptWriterFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePromptWriterFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePromptWriterFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePromptWriterFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writePromptWriterFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	for _, flowID := range []string{"alpha", "beta"} {
		writePromptWriterFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), "name: "+flowID+"\n")
		writePromptWriterFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writePromptWriterFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), "{}\n")
		writePromptWriterFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
		writePromptWriterFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), `
case:
  business_brief:
    type: text
    _unused_reason: scoped duplicate prompt proof
`)
		writePromptWriterFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), `
writer:
  id: writer
  role: writer
  mode: task
  prompt_ref: writer
  workspace_class: factory
  manager_fallback: ops
`)
		writePromptWriterFixtureFile(t, filepath.Join(root, "flows", flowID, "prompts", "writer.md"), "Use save_entity_field for `business_brief` in "+flowID+".\n")
	}

	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	got, err := DerivePromptEntityWriteEvidence(bundle)
	if err != nil {
		t.Fatalf("DerivePromptEntityWriteEvidence: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].Source.FlowID == got[1].Source.FlowID {
		t.Fatalf("scoped evidence flow ids = %#v, want distinct flows", []string{got[0].Source.FlowID, got[1].Source.FlowID})
	}
	if strings.TrimSpace(got[0].AgentID) != "writer" || strings.TrimSpace(got[1].AgentID) != "writer" {
		t.Fatalf("AgentIDs = %#v, want duplicate logical ids", []string{got[0].AgentID, got[1].AgentID})
	}
}

func writePromptWriterFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
