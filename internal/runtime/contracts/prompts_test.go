package contracts

import (
	"strings"
	"testing"

	models "empireai/internal/runtime/core/actors"
)

func TestLoadPromptForAgent_UsesPromptRefAndWorkspaceRoleFallback(t *testing.T) {
	prompt, found, err := LoadPromptForAgent(models.AgentConfig{
		ID:   "cos-entity-1",
		Role: "chief_of_staff",
	}, "")
	if err != nil {
		t.Fatalf("LoadPromptForAgent: %v", err)
	}
	if !found {
		t.Fatal("expected prompt to be found")
	}
	if !strings.Contains(prompt, "{{vertical_name}}") {
		t.Fatalf("expected operating CoS prompt template, got %q", prompt)
	}
}
