package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"gopkg.in/yaml.v3"
)

func TestEffectiveAgentMemoryPreservesPresenceAndProvenance(t *testing.T) {
	tests := []struct {
		name       string
		yaml       string
		enabled    bool
		provenance agentmemory.Source
	}{
		{name: "omitted", yaml: "role: helper\n", enabled: false, provenance: agentmemory.SourcePlatformDefault},
		{name: "explicit false", yaml: "role: helper\nmemory: false\n", enabled: false, provenance: agentmemory.SourceAuthored},
		{name: "explicit true", yaml: "role: helper\nmemory: true\n", enabled: true, provenance: agentmemory.SourceAuthored},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entry AgentRegistryEntry
			if err := yaml.Unmarshal([]byte(tt.yaml), &entry); err != nil {
				t.Fatalf("yaml.Unmarshal: %v", err)
			}
			effective := EffectiveAgentRegistryEntry("helper", entry)
			if effective.MemoryPlan.Enabled != tt.enabled || effective.MemoryPlan.Source != tt.provenance {
				t.Fatalf("memory plan = %#v, want enabled=%v provenance=%q", effective.MemoryPlan, tt.enabled, tt.provenance)
			}
		})
	}
}

func TestAgentRegistryEntryRejectsRetiredMemoryFields(t *testing.T) {
	for _, field := range []string{"mode", "conversation_mode", "session_scope", "session_scope_authority"} {
		t.Run(field, func(t *testing.T) {
			var entry AgentRegistryEntry
			err := yaml.Unmarshal([]byte("role: helper\n"+field+": task\n"), &entry)
			if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "memory") {
				t.Fatalf("yaml.Unmarshal error = %v, want RETIRED guidance to memory", err)
			}
		})
	}
}
