package apiv1

import (
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func withAPITestIntent(t testing.TB, cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	t.Helper()
	content := "Test intent for " + cfg.ID + "."
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+cfg.ID+".intent",
		content,
	)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Intent = intent
	cfg.SystemPrompt = content
	return cfg
}

func apiTestCatalogAgentDefinition(t testing.TB, agentID, content string) map[string]any {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+agentID+".intent",
		content,
	)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"agent_id":            agentID,
		"role":                "worker",
		"type":                "managed",
		"model":               "regular",
		"memory":              false,
		"memory_source":       "platform_default",
		"intent_kind":         string(intent.Kind),
		"intent_source":       intent.Coordinate,
		"intent_provenance":   intent.Provenance,
		"intent_content_hash": intent.ContentHash,
		"intent_identity":     intent.Identity,
		"intent_content":      intent.Content,
	}
}
