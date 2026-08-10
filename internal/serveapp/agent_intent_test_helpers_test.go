package serveapp

import (
	"encoding/json"
	"strings"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func serveTestAgentConfig(cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	if strings.TrimSpace(cfg.ResolvedLLMBackend) == "" {
		cfg.ResolvedLLMBackend = strings.TrimSpace(cfg.LLMBackend)
		if cfg.ResolvedLLMBackend == "" {
			cfg.ResolvedLLMBackend = "anthropic"
		}
	}
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"test#agents."+strings.TrimSpace(cfg.ID)+".intent",
		"Perform the serve-runtime test agent's assigned work.",
	)
	if err != nil {
		panic(err)
	}
	cfg.Intent = intent
	cfg.SystemPrompt = intent.Content
	return cfg
}

func serveTestAgentRuntimeDescriptor(agentID, agentType string, fields map[string]any) string {
	cfg := serveTestAgentConfig(runtimeactors.AgentConfig{ID: agentID})
	descriptor := map[string]any{
		"type":                  strings.TrimSpace(agentType),
		"execution_mode":        "live",
		"intent":                cfg.Intent,
		"derived_system_prompt": cfg.SystemPrompt,
	}
	for key, value := range fields {
		descriptor[key] = value
	}
	raw, err := json.Marshal(descriptor)
	if err != nil {
		panic(err)
	}
	return string(raw)
}
