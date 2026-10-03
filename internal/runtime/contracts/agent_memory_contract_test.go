package contracts

import (
	"strings"
	"testing"
)

func TestEffectiveAgentMemoryPreservesEnablementOnly(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		enabled bool
	}{
		{name: "omitted", yaml: "intent: {inline: helper}\nrole: helper\n", enabled: false},
		{name: "explicit true", yaml: "intent: {inline: helper}\nrole: helper\nmemory: true\n", enabled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entry AgentRegistryEntry
			if err := decodeNodeTestYAML([]byte(tt.yaml), &entry); err != nil {
				t.Fatalf("yaml.Unmarshal: %v", err)
			}
			effective := EffectiveAgentRegistryEntry("helper", entry)
			if effective.MemoryPlan.Enabled != tt.enabled || effective.EffectiveSourceForField("memory") != "" {
				t.Fatalf("memory plan = %#v, want enabled=%v without a separate source", effective.MemoryPlan, tt.enabled)
			}
		})
	}
}

func TestAgentRegistryEntryRejectsRetiredMemoryFields(t *testing.T) {
	for _, field := range []string{"mode", "conversation_mode", "session_scope", "session_scope_authority"} {
		t.Run(field, func(t *testing.T) {
			var entry AgentRegistryEntry
			err := decodeNodeTestYAML([]byte("role: helper\n"+field+": task\n"), &entry)
			if err == nil || !strings.Contains(err.Error(), "is not supported") || !strings.Contains(err.Error(), "memory") {
				t.Fatalf("yaml.Unmarshal error = %v, want RETIRED guidance to memory", err)
			}
		})
	}
}
