package actors

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

type NativeToolConfig struct {
	Bash      bool `json:"bash,omitempty"`
	WebSearch bool `json:"web_search,omitempty"`
	FileIO    bool `json:"file_io,omitempty"`
}

func (cfg NativeToolConfig) Enabled(name string) bool {
	switch strings.TrimSpace(name) {
	case "bash":
		return cfg.Bash
	case "web_search":
		return cfg.WebSearch
	case "file_io":
		return cfg.FileIO
	default:
		return false
	}
}

func (cfg NativeToolConfig) Any() bool {
	return cfg.Bash || cfg.WebSearch || cfg.FileIO
}

func (cfg NativeToolConfig) Names() []string {
	names := make([]string, 0, 3)
	if cfg.Bash {
		names = append(names, "bash")
	}
	if cfg.WebSearch {
		names = append(names, "web_search")
	}
	if cfg.FileIO {
		names = append(names, "file_io")
	}
	return names
}

// AgentConfig is the runtime-owned actor descriptor used by manager, tools,
// LLM, and semantic/runtime contract resolution. It is intentionally distinct
// from persistence-row ownership even when stored verbatim.
type AgentConfig struct {
	ID                   string                        `json:"id"`
	Identity             runtimeagentidentity.Identity `json:"identity"`
	Type                 string                        `json:"type"`
	Role                 string                        `json:"role"`
	FlowID               string                        `json:"flow_id,omitempty"`
	Model                string                        `json:"model,omitempty"`
	LLMBackend           string                        `json:"llm_backend,omitempty"`
	ResolvedLLMBackend   string                        `json:"resolved_llm_backend,omitempty"`
	ResolvedModel        string                        `json:"resolved_model,omitempty"`
	ResolvedLLMProvider  string                        `json:"resolved_llm_provider,omitempty"`
	ResolvedLLMTransport string                        `json:"resolved_llm_transport,omitempty"`
	ExecutionMode        runtimeeffects.ExecutionMode  `json:"execution_mode,omitempty"`
	Memory               agentmemory.Plan              `json:"memory"`
	Mock                 mockperformance.Performance   `json:"mock,omitempty"`
	Intent               runtimeagentintent.Resolved   `json:"intent"`
	SystemPrompt         string                        `json:"derived_system_prompt"`
	MaxTurnsPerTask      int                           `json:"max_turns_per_task,omitempty"`
	Subscriptions        []string                      `json:"subscriptions,omitempty"`
	EmitEvents           []string                      `json:"emit_events,omitempty"`
	Criteria             []string                      `json:"criteria,omitempty"`
	Tools                []string                      `json:"tools,omitempty"`
	Permissions          []string                      `json:"permissions,omitempty"`
	NativeTools          NativeToolConfig              `json:"native_tools,omitempty"`
	FlowDataAccess       []string                      `json:"flow_data_access,omitempty"`
	WorkspaceClass       string                        `json:"workspace_class,omitempty"`
	ManagerFallback      string                        `json:"manager_fallback,omitempty"`
	FlowPath             string                        `json:"flow_path,omitempty"`
	EntityID             string                        `json:"entity_id,omitempty"`
	ParentAgent          string                        `json:"parent_agent_id,omitempty"`
	Config               json.RawMessage               `json:"config,omitempty"`
	BudgetEnvelope       float64                       `json:"budget_envelope,omitempty"`
}

func (cfg AgentConfig) EffectiveEntityID() string { return strings.TrimSpace(cfg.EntityID) }

func (cfg AgentConfig) ValidateIntentCarrier() error {
	if err := cfg.Intent.Validate(); err != nil {
		return fmt.Errorf("resolved intent: %w", err)
	}
	if strings.TrimSpace(cfg.SystemPrompt) == "" {
		return fmt.Errorf("derived system prompt is required")
	}
	if !strings.HasPrefix(cfg.SystemPrompt, cfg.Intent.Content) {
		return fmt.Errorf("derived system prompt must begin with the exact resolved intent content")
	}
	return nil
}

func ValidateNoAuthoredSystemPrompt(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("agent config must be valid JSON: %w", err)
	}
	if path, ok := authoredSystemPromptPath(value, "config"); ok {
		return fmt.Errorf("RETIRED: authored %s is unsupported; declare intent: in agents.yaml", path)
	}
	return nil
}

func authoredSystemPromptPath(value any, path string) (string, bool) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			key = strings.TrimSpace(key)
			childPath := path + "." + key
			if key == "system_prompt" {
				return childPath, true
			}
			if found, ok := authoredSystemPromptPath(child, childPath); ok {
				return found, true
			}
		}
	case []any:
		for i, child := range typed {
			if found, ok := authoredSystemPromptPath(child, fmt.Sprintf("%s[%d]", path, i)); ok {
				return found, true
			}
		}
	}
	return "", false
}

func (cfg *AgentConfig) NormalizeEntityID() {
	if cfg == nil {
		return
	}
	entityID := cfg.EffectiveEntityID()
	if strings.TrimSpace(cfg.EntityID) == "" {
		cfg.EntityID = entityID
	}
}

func (cfg AgentConfig) CanonicalFlowPath() string {
	return strings.Trim(strings.TrimSpace(cfg.FlowPath), "/")
}

func (cfg AgentConfig) ConcreteIdentity() (runtimeagentidentity.Identity, error) {
	identity := cfg.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeagentidentity.Identity{}, err
	}
	if identity.AgentID() != strings.TrimSpace(cfg.ID) {
		return runtimeagentidentity.Identity{}, fmt.Errorf(
			"agent config id %q conflicts with concrete identity agent_id %q",
			strings.TrimSpace(cfg.ID),
			identity.AgentID(),
		)
	}
	if identity.FlowInstance() != cfg.CanonicalFlowPath() {
		return runtimeagentidentity.Identity{}, fmt.Errorf(
			"agent config flow_path %q conflicts with concrete identity flow_instance %q",
			cfg.CanonicalFlowPath(),
			identity.FlowInstance(),
		)
	}
	return identity, nil
}

func (cfg *AgentConfig) NormalizeRuntimeDescriptor() {
	if cfg == nil {
		return
	}
	cfg.ID = strings.TrimSpace(cfg.ID)
	cfg.Identity = cfg.Identity.Normalize()
	cfg.Type = strings.TrimSpace(cfg.Type)
	cfg.Role = strings.TrimSpace(cfg.Role)
	cfg.FlowID = strings.TrimSpace(cfg.FlowID)
	cfg.Model = strings.TrimSpace(cfg.Model)
	cfg.LLMBackend = strings.TrimSpace(cfg.LLMBackend)
	cfg.ResolvedLLMBackend = strings.TrimSpace(cfg.ResolvedLLMBackend)
	cfg.ResolvedModel = strings.TrimSpace(cfg.ResolvedModel)
	cfg.ResolvedLLMProvider = strings.TrimSpace(cfg.ResolvedLLMProvider)
	cfg.ResolvedLLMTransport = strings.TrimSpace(cfg.ResolvedLLMTransport)
	cfg.ExecutionMode = runtimeeffects.ExecutionMode(strings.TrimSpace(string(cfg.ExecutionMode)))
	cfg.Mock.Kind = strings.TrimSpace(cfg.Mock.Kind)
	cfg.Mock.Module = strings.TrimSpace(cfg.Mock.Module)
	cfg.Mock.Digest = strings.TrimSpace(cfg.Mock.Digest)
	cfg.Mock.SourcePath = strings.TrimSpace(cfg.Mock.SourcePath)
	cfg.Mock.Source = append([]byte(nil), cfg.Mock.Source...)
	if plan, err := cfg.Memory.Normalize(); err == nil {
		cfg.Memory = plan
	}
	cfg.WorkspaceClass = strings.TrimSpace(cfg.WorkspaceClass)
	cfg.ManagerFallback = strings.TrimSpace(cfg.ManagerFallback)
	cfg.FlowPath = cfg.CanonicalFlowPath()
	cfg.ParentAgent = strings.TrimSpace(cfg.ParentAgent)
	cfg.Subscriptions = normalizeStringList(cfg.Subscriptions)
	cfg.EmitEvents = normalizeStringList(cfg.EmitEvents)
	cfg.Criteria = normalizeStringList(cfg.Criteria)
	cfg.Tools = normalizeStringList(cfg.Tools)
	cfg.Permissions = normalizeStringList(cfg.Permissions)
	cfg.FlowDataAccess = normalizeStringList(cfg.FlowDataAccess)
}

func normalizeStringList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	slices.Sort(out)
	return out
}

type actorContextKey struct{}

func WithActor(ctx context.Context, actor AgentConfig) context.Context {
	return context.WithValue(ctx, actorContextKey{}, actor)
}

func ActorFromContext(ctx context.Context) (AgentConfig, bool) {
	v := ctx.Value(actorContextKey{})
	if v == nil {
		return AgentConfig{}, false
	}
	cfg, ok := v.(AgentConfig)
	return cfg, ok
}
