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
	SessionScopeGlobal          = "global"
	SessionScopeFlow            = "flow"
	SessionScopeEntity          = "entity"
)

type ScopeContext struct {
	ConversationMode string
	SessionScope     string
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

func WithScope(ctx context.Context, mode, sessionScope, scopeKey string) context.Context {
	payload := ScopeContext{
		ConversationMode: strings.TrimSpace(mode),
		SessionScope:     strings.TrimSpace(sessionScope),
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
	payload.SessionScope = strings.TrimSpace(payload.SessionScope)
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

func canonicalSessionScope(raw string) (string, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case SessionScopeGlobal:
		return SessionScopeGlobal, true
	case SessionScopeFlow:
		return SessionScopeFlow, true
	case SessionScopeEntity:
		return SessionScopeEntity, true
	default:
		return "", false
	}
}

func NormalizeSessionScope(raw string) string {
	scope, _ := canonicalSessionScope(raw)
	return scope
}

func ParseSessionScope(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("session scope is required")
	}
	scope, ok := canonicalSessionScope(raw)
	if !ok {
		return "", fmt.Errorf("invalid session scope %q", raw)
	}
	return scope, nil
}

func ValidateSessionScopeIntent(runtimeMode, sessionScope string) (string, error) {
	mode, err := ParseConversationRuntimeMode(runtimeMode)
	if err != nil {
		return "", err
	}
	sessionScope = strings.TrimSpace(sessionScope)
	switch mode {
	case RuntimeModeTask:
		if sessionScope != "" {
			return "", fmt.Errorf("task mode does not use sessions; session_scope must be absent")
		}
		return "", nil
	case RuntimeModeSession:
		scope, err := ParseSessionScope(sessionScope)
		if err != nil {
			return "", fmt.Errorf("session mode requires explicit session_scope (global or flow)")
		}
		switch scope {
		case SessionScopeGlobal, SessionScopeFlow:
			return scope, nil
		default:
			return "", fmt.Errorf("session mode does not support entity scope; use session_per_entity")
		}
	case RuntimeModeSessionPerEntity:
		scope, err := ParseSessionScope(sessionScope)
		if err != nil {
			return "", fmt.Errorf("session_per_entity requires explicit session_scope: entity")
		}
		if scope != SessionScopeEntity {
			switch scope {
			case SessionScopeGlobal:
				return "", fmt.Errorf("session_per_entity does not support global scope; use session with session_scope: global")
			case SessionScopeFlow:
				return "", fmt.Errorf("session_per_entity does not support flow scope; use session with session_scope: flow")
			default:
				return "", fmt.Errorf("session_per_entity requires explicit session_scope: entity")
			}
		}
		return scope, nil
	default:
		return "", fmt.Errorf("unsupported conversation mode %q", runtimeMode)
	}
}

func ResolveScope(ctx context.Context, runtimeMode, sessionScope, scopeKey string) (ResolvedScope, error) {
	mode, err := ParseConversationRuntimeMode(runtimeMode)
	if err != nil {
		return ResolvedScope{}, err
	}
	sessionScope, err = ValidateSessionScopeIntent(mode, sessionScope)
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
		if sessionScope != SessionScopeEntity {
			return ResolvedScope{}, fmt.Errorf("session_per_entity requires explicit session_scope: entity")
		}
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
		switch sessionScope {
		case SessionScopeGlobal:
			switch scopeKey {
			case "", SessionScopeGlobal:
				out.Scope = SessionScopeGlobal
				out.ScopeKey = SessionScopeGlobal
				return out, nil
			default:
				return ResolvedScope{}, fmt.Errorf("session_scope global requires scope_key global or empty")
			}
		case SessionScopeFlow:
			switch {
			case scopeKey == SessionScopeGlobal:
				return ResolvedScope{}, fmt.Errorf("session_scope flow does not allow global scope key")
			case scopeKey != "" && flowPath != "" && scopeKey != flowPath:
				return ResolvedScope{}, fmt.Errorf("session_scope flow scope key %q does not match actor flow path %q", scopeKey, flowPath)
			case scopeKey != "":
				out.Scope = SessionScopeFlow
				out.ScopeKey = scopeKey
				out.FlowInstance = scopeKey
				return out, nil
			case flowPath != "":
				out.Scope = SessionScopeFlow
				out.ScopeKey = flowPath
				out.FlowInstance = flowPath
				return out, nil
			default:
				return ResolvedScope{}, fmt.Errorf("session_scope flow requires actor flow path")
			}
		default:
			return ResolvedScope{}, fmt.Errorf("session mode requires explicit session_scope (global or flow)")
		}
	}

	return ResolvedScope{}, fmt.Errorf("unsupported conversation mode %q", runtimeMode)
}
