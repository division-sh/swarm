package empire

import (
	"encoding/json"
	"testing"

	runtimemanager "empireai/internal/runtime/manager"
)

func TestManager_SpawnOpCo_UsesTemplateAndExpandsPrompts(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"vertical_name": "Clinic Ops",
		"geography":     "mx",
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	got := runtimemanager.ExpandConfigPromptTemplate("Launch {{vertical_name}} in {{geography}}", raw)
	if got != "Launch Clinic Ops in mx" {
		t.Fatalf("ExpandConfigPromptTemplate() = %q", got)
	}
}
