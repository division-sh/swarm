package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"empireai/internal/models"
)

func TestIsUniversalRuntimeTool(t *testing.T) {
	if !IsUniversalRuntimeTool("agent_message") {
		t.Fatal("expected agent_message to be universal")
	}
	if IsUniversalRuntimeTool("sql_execute") {
		t.Fatal("expected sql_execute to be non-universal")
	}
}

func TestFilterToolsIncludesUniversalEvenWhenConstrained(t *testing.T) {
	in := []ToolDefinition{
		{Name: "agent_message"},
		{Name: "sql_execute"},
	}
	allowed := map[string]struct{}{
		"sql_execute": {},
	}
	filtered := filterTools(in, allowed, true)
	got := map[string]struct{}{}
	for _, d := range filtered {
		got[d.Name] = struct{}{}
	}
	if _, ok := got["agent_message"]; !ok {
		t.Fatalf("expected filtered tools to include universal agent_message, got %#v", filtered)
	}
	if _, ok := got["sql_execute"]; !ok {
		t.Fatalf("expected filtered tools to include explicitly allowed sql_execute, got %#v", filtered)
	}
}

func TestAuthorizeToolUsageAllowsUniversalTool(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{
		"tools": []string{"sql_execute"},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	actor := models.AgentConfig{
		ID:     "a1",
		Role:   "analysis-agent",
		Config: cfg,
	}
	exec := &RuntimeToolExecutor{}
	if err := exec.authorizeToolUsage(context.Background(), actor, "agent_message"); err != nil {
		t.Fatalf("expected universal tool allowed, got %v", err)
	}
}
