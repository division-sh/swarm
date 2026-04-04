package sessions

import (
	"context"
	"strings"
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
		ConversationMode: NormalizeConversationRuntimeMode(mode),
		ScopeKey:         strings.TrimSpace(scopeKey),
	}
	return context.WithValue(ctx, scopeContextKey{}, payload)
}

func ScopeFromContext(ctx context.Context) ScopeContext {
	if ctx == nil {
		return ScopeContext{ConversationMode: RuntimeModeTask}
	}
	v := ctx.Value(scopeContextKey{})
	payload, ok := v.(ScopeContext)
	if !ok {
		return ScopeContext{ConversationMode: RuntimeModeTask}
	}
	payload.ConversationMode = NormalizeConversationRuntimeMode(payload.ConversationMode)
	payload.ScopeKey = strings.TrimSpace(payload.ScopeKey)
	return payload
}

func NormalizeConversationRuntimeMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case RuntimeModeTask, "stateless":
		return RuntimeModeTask
	case RuntimeModeSession:
		return RuntimeModeSession
	case RuntimeModeSessionPerEntity:
		return RuntimeModeSessionPerEntity
	default:
		return RuntimeModeTask
	}
}

func IsStatelessRuntimeMode(raw string) bool {
	return NormalizeConversationRuntimeMode(raw) == RuntimeModeTask
}

func IsLiveSessionRuntimeMode(raw string) bool {
	return !IsStatelessRuntimeMode(raw)
}

func ResolveScope(runtimeMode, scopeKey string) ResolvedScope {
	mode := NormalizeConversationRuntimeMode(runtimeMode)
	scopeKey = strings.TrimSpace(scopeKey)

	out := ResolvedScope{
		RuntimeMode: mode,
		ScopeKey:    scopeKey,
	}

	switch mode {
	case RuntimeModeTask:
		out.Stateless = true
	case RuntimeModeSessionPerEntity:
		out.Scope = "entity"
		out.EntityID = scopeKey
	default:
		if scopeKey == "" {
			out.Scope = "global"
			out.ScopeKey = "global"
			break
		}
		out.Scope = "flow"
		out.FlowInstance = scopeKey
	}

	return out
}
