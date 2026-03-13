package manager

import (
	"context"
	"encoding/json"

	"empireai/internal/events"
	runtimeactors "empireai/internal/runtime/core/actors"
	workspace "empireai/internal/runtime/workspace"
)

type genericAgent struct {
	id            string
	agentType     string
	subscriptions []events.EventType
}

func newGenericAgent(cfg runtimeactors.AgentConfig) Agent {
	if cfg.Type == "" {
		cfg.Type = "generic"
	}
	merged := make([]string, 0, len(cfg.Subscriptions))
	merged = append(merged, cfg.Subscriptions...)
	if len(cfg.Config) > 0 {
		var aux struct {
			Subscriptions []string `json:"subscriptions"`
		}
		if err := json.Unmarshal(cfg.Config, &aux); err == nil {
			merged = append(merged, aux.Subscriptions...)
		}
	}

	uniq := make(map[string]struct{})
	subs := make([]events.EventType, 0, len(merged))
	for _, s := range merged {
		if s == "" {
			continue
		}
		if _, ok := uniq[s]; ok {
			continue
		}
		uniq[s] = struct{}{}
		subs = append(subs, events.EventType(s))
	}

	return &genericAgent{
		id:            cfg.ID,
		agentType:     cfg.Type,
		subscriptions: subs,
	}
}

func (a *genericAgent) ID() string                        { return a.id }
func (a *genericAgent) Type() string                      { return a.agentType }
func (a *genericAgent) Subscriptions() []events.EventType { return a.subscriptions }
func (a *genericAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func mergeAgentConfig(base, patch runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	return MergeAgentConfig(base, patch)
}

func extractSystemPromptFromConfig(raw json.RawMessage) string {
	return ExtractSystemPromptFromConfig(raw)
}

func withSystemPrompt(raw json.RawMessage, prompt string) json.RawMessage {
	return WithSystemPrompt(raw, prompt)
}

func (am *AgentManager) SetWorkspaceLifecycle(workspaces workspace.Lifecycle) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.workspaces = workspaces
}
