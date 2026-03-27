package contracts

import (
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
)

func TestLoadPromptForAgent_UsesPromptRefAndWorkspaceRoleFallback(t *testing.T) {
	SetActivePromptBundle(loadPromptTestBundle(t, repoRoot(t)))
	prompt, found, err := LoadPromptForAgent(models.AgentConfig{
		ID:   "cos-entity-1",
		Role: "ops_lead",
	}, "")
	if err != nil {
		t.Fatalf("LoadPromptForAgent: %v", err)
	}
	if !found {
		t.Fatal("expected prompt to be found")
	}
	if !strings.Contains(prompt, "{{team_name}}") {
		t.Fatalf("expected generic operations prompt template, got %q", prompt)
	}
}
