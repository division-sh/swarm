package llm

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
)

func TestShouldUseMCPBridge_DefaultsOn(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "")
	if !shouldUseMCPBridge() {
		t.Fatal("expected MCP bridge to default on")
	}
}

func TestShouldUseMCPBridge_CanDisable(t *testing.T) {
	for _, raw := range []string{"0", "false", "no"} {
		t.Setenv("SWARM_CLAUDE_USE_MCP", raw)
		if shouldUseMCPBridge() {
			t.Fatalf("expected %q to disable MCP bridge", raw)
		}
	}
}

func TestClaudeDisallowedBuiltinToolsArgForActor_DefaultsToAllKnownBuiltins(t *testing.T) {
	got := claudeDisallowedBuiltinToolsArgForActor(models.AgentConfig{})
	if got == "" {
		t.Fatal("expected builtin tools to be blocked by default")
	}
	gotNames := strings.Split(got, ",")
	for _, name := range []string{"Bash", "Read", "WebSearch", "ToolSearch", "AskUserQuestion"} {
		if !slices.Contains(gotNames, name) {
			t.Fatalf("expected %q in disallowed tools %q", name, got)
		}
	}
}

func TestClaudeDisallowedBuiltinToolsArgForActor_MapsNativeCapabilities(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"native_tools": map[string]any{
			"bash":       true,
			"web_search": true,
			"file_io":    true,
		},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	got := claudeDisallowedBuiltinToolsArgForActor(models.AgentConfig{Config: raw})
	gotNames := strings.Split(got, ",")
	for _, name := range []string{"Bash", "Read", "Write", "Edit", "WebSearch"} {
		if slices.Contains(gotNames, name) {
			t.Fatalf("did not expect allowed tool %q in disallowed list %q", name, got)
		}
	}
	for _, name := range []string{"ToolSearch", "AskUserQuestion", "Glob"} {
		if !slices.Contains(gotNames, name) {
			t.Fatalf("expected undeclared tool %q in disallowed list %q", name, got)
		}
	}
}
