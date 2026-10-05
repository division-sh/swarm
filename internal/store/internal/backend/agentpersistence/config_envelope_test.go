package agentpersistence

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPersistedAgentNamespacesNormalizeDoubleTokensBeforeStorage(t *testing.T) {
	cfg := persistedIntentTestAgent(t)
	cfg.Config = json.RawMessage(`{"number":7e0}`)
	row, err := ProjectPersistedAgentConfig(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	want := `{"number":7.0}`
	if string(row.ConfigJSON) != want {
		t.Fatalf("storage wire can lose double kind in JSONB: got %s want %s", row.ConfigJSON, want)
	}
}

func TestPersistedAgentOpaqueAuthorityRejectsInsteadOfStripping(t *testing.T) {
	cases := map[string]string{
		"system_prompt": `{"system_prompt":"override"}`,
		"nested_prompt": `{"nested":{"system_prompt":"override"}}`,
		"list_prompt":   `{"nested":[{"system_prompt":"override"}]}`,
	}
	for key := range runtimeConfigKeys {
		raw, err := json.Marshal(map[string]any{key: nil})
		if err != nil {
			t.Fatal(err)
		}
		cases[key] = string(raw)
	}
	for _, key := range []string{"mode", "conversation_mode", "session_scope", "session_scope_authority", "memory", "max_turns_per_task"} {
		raw, err := json.Marshal(map[string]any{"constraints": map[string]any{key: nil}})
		if err != nil {
			t.Fatal(err)
		}
		cases["constraints."+key] = string(raw)
	}
	base, err := ProjectPersistedAgentConfig(persistedIntentTestAgent(t), "")
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			want := "config contains runtime-owned keys: " + name
			if strings.Contains(name, "prompt") {
				want = "authored config"
			}
			cfg := persistedIntentTestAgent(t)
			cfg.Config = json.RawMessage(raw)
			if _, err := ProjectPersistedAgentConfig(cfg, ""); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("projection error=%v want %q", err, want)
			}
			row := base
			row.ConfigJSON = []byte(raw)
			if _, err := HydratePersistedAgentConfig(row); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("hydration error=%v want %q", err, want)
			}
		})
	}
}
