package runtime

import (
	"testing"

	"empireai/internal/models"
)

func TestOrderAgentsByParent_OrdersParentBeforeChild(t *testing.T) {
	parent := PersistedAgent{
		Config: modelsAgentConfigForTest("parent"),
	}
	child := PersistedAgent{
		Config:        modelsAgentConfigForTest("child"),
		ParentAgentID: "parent",
	}

	ordered, err := orderAgentsByParent([]PersistedAgent{child, parent})
	if err != nil {
		t.Fatalf("orderAgentsByParent: %v", err)
	}
	if len(ordered) != 2 {
		t.Fatalf("expected 2 agents, got %d", len(ordered))
	}
	if ordered[0].Config.ID != "parent" || ordered[1].Config.ID != "child" {
		t.Fatalf("unexpected order: %q then %q", ordered[0].Config.ID, ordered[1].Config.ID)
	}
}

func TestOrderAgentsByParent_ErrorsOnMissingParent(t *testing.T) {
	a := PersistedAgent{
		Config:        modelsAgentConfigForTest("child"),
		ParentAgentID: "missing",
	}
	if _, err := orderAgentsByParent([]PersistedAgent{a}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestOrderAgentsByParent_ErrorsOnCycle(t *testing.T) {
	a := PersistedAgent{Config: modelsAgentConfigForTest("a"), ParentAgentID: "b"}
	b := PersistedAgent{Config: modelsAgentConfigForTest("b"), ParentAgentID: "a"}
	if _, err := orderAgentsByParent([]PersistedAgent{a, b}); err == nil {
		t.Fatalf("expected error")
	}
}

func modelsAgentConfigForTest(id string) models.AgentConfig {
	return models.AgentConfig{ID: id, Role: id, Mode: "operating"}
}
