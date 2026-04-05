package sessions

import (
	"context"
	"fmt"
	"strings"

	runtimeactors "swarm/internal/runtime/core/actors"
)

const (
	RuntimeModeTask             = "task"
	RuntimeModeSession          = "session"
	RuntimeModeSessionPerEntity = "session_per_entity"
)

type ScopeContext struct {
	ConversationMode string
	ScopeKey         string
}

type ResolvedScope struct {
	RuntimeMode  string
	ScopeKey     string
	Scope        string
	EntityID     string
	FlowInstance string
	Stateless    bool
}

type scopeContextKey struct{}

func WithScope(ctx context.Context, mode, scopeKey string) context.Context {
	payload := ScopeContext{
		ConversationMode: strings.TrimSpace(mode),
		ScopeKey:         strings.TrimSpace(scopeKey),
	}
	return context.WithValue(ctx, scopeContextKey{}, payload)
}

func ScopeFromContext(ctx context.Context) ScopeContext {
	if ctx == nil {
		return ScopeContext{}
	}
	v := ctx.Value(scopeContextKey{})
	payload, ok := v.(ScopeContext)
	if !ok {
		return ScopeContext{}
	}
	payload.ConversationMode = strings.TrimSpace(payload.ConversationMode)
	payload.ScopeKey = strings.TrimSpace(payload.ScopeKey)
	return payload
}

func canonicalConversationRuntimeMode(raw string) (string, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case RuntimeModeTask, "stateless":
		return RuntimeModeTask, true
	case RuntimeModeSession:
		return RuntimeModeSession, true
	case RuntimeModeSessionPerEntity:
		return RuntimeModeSessionPerEntity, true
	default:
		return "", false
	}
}

func NormalizeConversationRuntimeMode(raw string) string {
	mode, _ := canonicalConversationRuntimeMode(raw)
	return mode
}

func ParseConversationRuntimeMode(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("conversation mode is required")
	}
	mode, ok := canonicalConversationRuntimeMode(raw)
	if !ok {
		return "", fmt.Errorf("invalid conversation mode %q", raw)
	}
	return mode, nil
}

func IsStatelessRuntimeMode(raw string) bool {
	mode, ok := canonicalConversationRuntimeMode(raw)
	return ok && mode == RuntimeModeTask
}

func IsLiveSessionRuntimeMode(raw string) bool {
	mode, ok := canonicalConversationRuntimeMode(raw)
	return ok && mode != RuntimeModeTask
}

func ResolveScope(ctx context.Context, runtimeMode, scopeKey string) (ResolvedScope, error) {
	mode, err := ParseConversationRuntimeMode(runtimeMode)
	if err != nil {
		return ResolvedScope{}, err
	}
	scopeKey = strings.TrimSpace(scopeKey)
	actor, _ := runtimeactors.ActorFromContext(ctx)
	flowPath := actor.CanonicalFlowPath()

	out := ResolvedScope{
		RuntimeMode: mode,
		ScopeKey:    scopeKey,
	}

	switch mode {
	case RuntimeModeTask:
		out.Stateless = true
		return out, nil
	case RuntimeModeSessionPerEntity:
		if scopeKey == "" {
			return ResolvedScope{}, fmt.Errorf("session_per_entity requires entity scope key")
		}
		if flowPath == "" {
			return ResolvedScope{}, fmt.Errorf("session_per_entity requires actor flow path")
		}
		out.Scope = "entity"
		out.EntityID = scopeKey
		out.FlowInstance = flowPath
		return out, nil
	case RuntimeModeSession:
		switch {
		case scopeKey == "global":
			out.Scope = "global"
			out.ScopeKey = "global"
			return out, nil
		case scopeKey != "":
			out.Scope = "flow"
			out.FlowInstance = scopeKey
			return out, nil
		case flowPath != "":
			out.Scope = "flow"
			out.ScopeKey = flowPath
			out.FlowInstance = flowPath
			return out, nil
		case strings.TrimSpace(actor.ID) != "":
			out.Scope = "global"
			out.ScopeKey = "global"
			return out, nil
		default:
			return ResolvedScope{}, fmt.Errorf("session requires explicit scope key or actor declaration")
		}
	}

	return ResolvedScope{}, fmt.Errorf("unsupported conversation mode %q", runtimeMode)
}
