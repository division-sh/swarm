package templateops

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLoadGlobalAgentsFromYAML_RequiresRosterAndPrompt(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadGlobalAgentsFromYAML(dir); err == nil {
		t.Fatal("expected missing roster to fail")
	}

	roster := []byte("agents:\n  a:\n    config_path: ./a.yaml\n")
	if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), roster, 0o644); err != nil {
		t.Fatalf("write roster: %v", err)
	}
	agentNoPrompt := []byte(strings.Join([]string{
		"id: a",
		"role: a",
		"mode: holding",
		"model_tier: sonnet",
		"subscriptions: [system.started]",
		"tools: [agent_message]",
	}, "\n"))
	if err := os.WriteFile(filepath.Join(dir, "a.yaml"), agentNoPrompt, 0o644); err != nil {
		t.Fatalf("write agent: %v", err)
	}

	if _, err := LoadGlobalAgentsFromYAML(dir); err == nil {
		t.Fatal("expected missing system_prompt to fail")
	}
}

func TestLoadGlobalAgentsFromYAML_UsesContractPromptFile(t *testing.T) {
	dir := t.TempDir()
	promptsDir := t.TempDir()
	t.Setenv("EMPIREAI_PROMPTS_DIR", promptsDir)

	if err := os.WriteFile(filepath.Join(promptsDir, "a.md"), []byte("contract prompt"), 0o644); err != nil {
		t.Fatalf("write contract prompt: %v", err)
	}

	roster := []byte("agents:\n  a:\n    config_path: ./a.yaml\n")
	if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), roster, 0o644); err != nil {
		t.Fatalf("write roster: %v", err)
	}
	agentNoPrompt := []byte(strings.Join([]string{
		"id: a",
		"role: a",
		"mode: holding",
		"model_tier: sonnet",
		"subscriptions: [system.started]",
		"tools: [agent_message]",
	}, "\n"))
	if err := os.WriteFile(filepath.Join(dir, "a.yaml"), agentNoPrompt, 0o644); err != nil {
		t.Fatalf("write agent: %v", err)
	}

	got, err := LoadGlobalAgentsFromYAML(dir)
	if err != nil {
		t.Fatalf("load global agents: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one agent, got %d", len(got))
	}
	if !strings.Contains(string(got[0].Config), "contract prompt") {
		t.Fatalf("expected contract prompt in config, got %s", string(got[0].Config))
	}
}

func TestLoadGlobalAgentsFromYAML_LoadsRosterFiles(t *testing.T) {
	dir := t.TempDir()
	roster := []byte(strings.Join([]string{
		"agents:",
		"  a:",
		"    config_path: ./a.yaml",
		"  b:",
		"    config_path: ./b.yaml",
	}, "\n"))
	if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), roster, 0o644); err != nil {
		t.Fatalf("write roster: %v", err)
	}
	a := []byte(strings.Join([]string{
		"id: a",
		"role: a",
		"mode: holding",
		"model_tier: sonnet",
		"system_prompt: |",
		"  You are a.",
	}, "\n"))
	b := []byte(strings.Join([]string{
		"id: b",
		"role: b",
		"mode: factory",
		"model_tier: haiku",
		"system_prompt: |",
		"  You are b.",
	}, "\n"))
	if err := os.WriteFile(filepath.Join(dir, "a.yaml"), a, 0o644); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.yaml"), b, 0o644); err != nil {
		t.Fatalf("write b: %v", err)
	}

	got, err := LoadGlobalAgentsFromYAML(dir)
	if err != nil {
		t.Fatalf("load global agents: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 agents, got %d", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("unexpected ids: %+v", []string{got[0].ID, got[1].ID})
	}
}

func TestLoadGlobalAgentsFromYAML_RejectsInvalidRosterPaths(t *testing.T) {
	t.Run("invalid extension", func(t *testing.T) {
		dir := t.TempDir()
		roster := []byte("agents:\n  a:\n    config_path: ./a.txt\n")
		if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), roster, 0o644); err != nil {
			t.Fatalf("write roster: %v", err)
		}
		if _, err := LoadGlobalAgentsFromYAML(dir); err == nil {
			t.Fatal("expected invalid extension failure")
		}
	})

	t.Run("duplicate path", func(t *testing.T) {
		dir := t.TempDir()
		roster := []byte(strings.Join([]string{
			"agents:",
			"  a:",
			"    config_path: ./a.yaml",
			"  b:",
			"    config_path: ./a.yaml",
		}, "\n"))
		if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), roster, 0o644); err != nil {
			t.Fatalf("write roster: %v", err)
		}
		if _, err := LoadGlobalAgentsFromYAML(dir); err == nil {
			t.Fatal("expected duplicate config_path failure")
		}
	})

	t.Run("missing referenced file", func(t *testing.T) {
		dir := t.TempDir()
		roster := []byte("agents:\n  a:\n    config_path: ./missing.yaml\n")
		if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), roster, 0o644); err != nil {
			t.Fatalf("write roster: %v", err)
		}
		if _, err := LoadGlobalAgentsFromYAML(dir); err == nil {
			t.Fatal("expected missing file failure")
		}
	})

	t.Run("path escapes dir", func(t *testing.T) {
		dir := t.TempDir()
		outside := filepath.Join(filepath.Dir(dir), "outside.yaml")
		if err := os.WriteFile(outside, []byte("id: o\nrole: o\nsystem_prompt: |\n  x\n"), 0o644); err != nil {
			t.Fatalf("write outside file: %v", err)
		}
		roster := []byte("agents:\n  a:\n    config_path: ../outside.yaml\n")
		if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), roster, 0o644); err != nil {
			t.Fatalf("write roster: %v", err)
		}
		if _, err := LoadGlobalAgentsFromYAML(dir); err == nil {
			t.Fatal("expected path escape failure")
		}
	})
}

func TestLoadGlobalAgentsFromYAML_RepoRosterContract(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller unavailable")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	agentsDir := filepath.Join(repoRoot, "configs", "agents")
	got, err := LoadGlobalAgentsFromYAML(agentsDir)
	if err != nil {
		t.Fatalf("load repo global roster: %v", err)
	}
	if len(got) != 15 {
		t.Fatalf("expected 15 global/factory agents, got %d", len(got))
	}
}

func TestInferGlobalAgentMode(t *testing.T) {
	cases := []struct {
		id   string
		role string
		want string
	}{
		{id: "factory-cto", role: "", want: "factory"},
		{id: "any", role: "validation-coordinator", want: "factory"},
		{id: "any", role: "scanner-agent", want: "factory"},
		{id: "any", role: "analysis-agent", want: "factory"},
		{id: "any", role: "pre-brand-agent", want: "factory"},
		{id: "any", role: "business-research-agent", want: "factory"},
		{id: "any", role: "lightweight-spec-agent", want: "factory"},
		{id: "any", role: "spec-reviewer", want: "factory"},
		{id: "any", role: "market-research-agent", want: "factory"},
		{id: "any", role: "trend-research-agent", want: "factory"},
		{id: "empire-coordinator", role: "", want: "holding"},
		{id: "operations-analyst", role: "", want: "holding"},
	}
	for _, tc := range cases {
		got := inferGlobalAgentMode(tc.id, tc.role)
		if got != tc.want {
			t.Fatalf("inferGlobalAgentMode(%q,%q): got %q want %q", tc.id, tc.role, got, tc.want)
		}
	}
}
