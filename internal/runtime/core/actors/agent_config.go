package actors

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
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

const SessionScopeAuthorityPlatformInternal = "platform_internal"

// AgentConfig is the runtime-owned actor descriptor used by manager, tools,
// LLM, and semantic/runtime contract resolution. It is intentionally distinct
// from persistence-row ownership even when stored verbatim.
type AgentConfig struct {
	ID                    string           `json:"id"`
	Type                  string           `json:"type"`
	Role                  string           `json:"role"`
	Mode                  string           `json:"mode"`
	Model                 string           `json:"model,omitempty"`
	LLMBackend            string           `json:"llm_backend,omitempty"`
	ResolvedModel         string           `json:"resolved_model,omitempty"`
	ResolvedLLMProvider   string           `json:"resolved_llm_provider,omitempty"`
	ResolvedLLMTransport  string           `json:"resolved_llm_transport,omitempty"`
	ConversationMode      string           `json:"conversation_mode,omitempty"`
	SessionScope          string           `json:"session_scope,omitempty"`
	SessionScopeAuthority string           `json:"-"`
	MaxTurnsPerTask       int              `json:"max_turns_per_task,omitempty"`
	Subscriptions         []string         `json:"subscriptions,omitempty"`
	EmitEvents            []string         `json:"emit_events,omitempty"`
	Tools                 []string         `json:"tools,omitempty"`
	Permissions           []string         `json:"permissions,omitempty"`
	NativeTools           NativeToolConfig `json:"native_tools,omitempty"`
	FlowDataAccess        []string         `json:"flow_data_access,omitempty"`
	WorkspaceClass        string           `json:"workspace_class,omitempty"`
	ManagerFallback       string           `json:"manager_fallback,omitempty"`
	FlowPath              string           `json:"flow_path,omitempty"`
	EntityID              string           `json:"entity_id,omitempty"`
	ParentAgent           string           `json:"parent_agent_id,omitempty"`
	Config                json.RawMessage  `json:"config,omitempty"`
	BudgetEnvelope        float64          `json:"budget_envelope,omitempty"`
}

func (cfg AgentConfig) EffectiveEntityID() string { return strings.TrimSpace(cfg.EntityID) }

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

func (cfg AgentConfig) HasPlatformInternalSessionScopeAuthority() bool {
	return strings.TrimSpace(cfg.SessionScopeAuthority) == SessionScopeAuthorityPlatformInternal
}

func (cfg *AgentConfig) NormalizeRuntimeDescriptor() {
	if cfg == nil {
		return
	}
	cfg.ID = strings.TrimSpace(cfg.ID)
	cfg.Type = strings.TrimSpace(cfg.Type)
	cfg.Role = strings.TrimSpace(cfg.Role)
	cfg.Mode = strings.TrimSpace(cfg.Mode)
	cfg.Model = strings.TrimSpace(cfg.Model)
	cfg.LLMBackend = strings.TrimSpace(cfg.LLMBackend)
	cfg.ResolvedModel = strings.TrimSpace(cfg.ResolvedModel)
	cfg.ResolvedLLMProvider = strings.TrimSpace(cfg.ResolvedLLMProvider)
	cfg.ResolvedLLMTransport = strings.TrimSpace(cfg.ResolvedLLMTransport)
	cfg.ConversationMode = strings.TrimSpace(cfg.ConversationMode)
	cfg.SessionScope = strings.TrimSpace(cfg.SessionScope)
	cfg.SessionScopeAuthority = strings.TrimSpace(cfg.SessionScopeAuthority)
	cfg.WorkspaceClass = strings.TrimSpace(cfg.WorkspaceClass)
	cfg.ManagerFallback = strings.TrimSpace(cfg.ManagerFallback)
	cfg.FlowPath = cfg.CanonicalFlowPath()
	cfg.ParentAgent = strings.TrimSpace(cfg.ParentAgent)
	cfg.Subscriptions = normalizeStringList(cfg.Subscriptions)
	cfg.EmitEvents = normalizeStringList(cfg.EmitEvents)
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
