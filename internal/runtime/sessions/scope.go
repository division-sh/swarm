package sessions

import (
	"context"
	"fmt"
	"strings"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

type RuntimeMode string

const (
	RuntimeModeTask             RuntimeMode = "task"
	RuntimeModeSession          RuntimeMode = "session"
	RuntimeModeSessionPerEntity RuntimeMode = "session_per_entity"
)

func (m RuntimeMode) String() string { return string(m) }
func (m RuntimeMode) IsStateless() bool {
	return m == RuntimeModeTask
}
func (m RuntimeMode) IsLiveSession() bool {
	return m != "" && m != RuntimeModeTask
}

type SessionScope string

const (
	SessionScopeGlobal SessionScope = "global"
	SessionScopeFlow   SessionScope = "flow"
	SessionScopeEntity SessionScope = "entity"
)

func (s SessionScope) String() string { return string(s) }

const authoredGlobalSessionScopeError = "authored normal agents cannot declare session_scope global; platform-owned global sessions are internal runtime state"

type ScopeContext struct {
	ConversationMode string
	SessionScope     string
	ScopeKey         string
}

type ResolvedScope struct {
	RuntimeMode  RuntimeMode
	ScopeKey     string
	Scope        SessionScope
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

func canonicalConversationRuntimeMode(raw string) (RuntimeMode, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case RuntimeModeTask.String(), "stateless":
		return RuntimeModeTask, true
	case RuntimeModeSession.String():
		return RuntimeModeSession, true
	case RuntimeModeSessionPerEntity.String():
		return RuntimeModeSessionPerEntity, true
	default:
		return "", false
	}
}

func NormalizeConversationRuntimeMode(raw string) RuntimeMode {
	mode, _ := canonicalConversationRuntimeMode(raw)
	return mode
}

func ParseConversationRuntimeMode(raw string) (RuntimeMode, error) {
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

func ParseAuthoredAgentMode(raw string) (RuntimeMode, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("mode is required")
	}
	switch strings.ToLower(raw) {
	case RuntimeModeTask.String():
		return RuntimeModeTask, nil
	case RuntimeModeSession.String():
		return RuntimeModeSession, nil
	case RuntimeModeSessionPerEntity.String():
		return RuntimeModeSessionPerEntity, nil
	case "stateless":
		return "", fmt.Errorf("mode %q is retired; use %q", raw, RuntimeModeTask)
	case SessionScopeGlobal.String():
		return "", fmt.Errorf("mode %q is reserved for platform-owned internal sessions", raw)
	default:
		return "", fmt.Errorf("invalid mode %q", raw)
	}
}

func DeriveSessionScopeForAuthoredMode(runtimeMode RuntimeMode) (SessionScope, error) {
	switch runtimeMode {
	case RuntimeModeTask:
		return "", nil
	case RuntimeModeSession:
		return SessionScopeFlow, nil
	case RuntimeModeSessionPerEntity:
		return SessionScopeEntity, nil
	default:
		return "", fmt.Errorf("unsupported mode %q", runtimeMode)
	}
}

func ResolveAuthoredAgentMemoryMode(raw string) (RuntimeMode, SessionScope, error) {
	mode, err := ParseAuthoredAgentMode(raw)
	if err != nil {
		return "", "", err
	}
	scope, err := DeriveSessionScopeForAuthoredMode(mode)
	if err != nil {
		return "", "", err
	}
	return mode, scope, nil
}

func IsStatelessRuntimeMode(raw string) bool {
	mode, ok := canonicalConversationRuntimeMode(raw)
	return ok && mode.IsStateless()
}

func IsLiveSessionRuntimeMode(raw string) bool {
	mode, ok := canonicalConversationRuntimeMode(raw)
	return ok && mode.IsLiveSession()
}

func canonicalSessionScope(raw string) (SessionScope, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case SessionScopeGlobal.String():
		return SessionScopeGlobal, true
	case SessionScopeFlow.String():
		return SessionScopeFlow, true
	case SessionScopeEntity.String():
		return SessionScopeEntity, true
	default:
		return "", false
	}
}

func NormalizeSessionScope(raw string) SessionScope {
	scope, _ := canonicalSessionScope(raw)
	return scope
}

func ParseSessionScope(raw string) (SessionScope, error) {
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

func ValidateSessionScopeIntent(runtimeMode RuntimeMode, sessionScope string) (SessionScope, error) {
	mode := runtimeMode
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
				return "", fmt.Errorf("session_per_entity does not support global scope")
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

// ValidateAuthoredSessionScopeIntent applies the product-authored agent boundary
// on top of the lower-level runtime scope model. The low-level model still
// retains global sessions for internal/platform and historical persisted rows.
func ValidateAuthoredSessionScopeIntent(runtimeMode RuntimeMode, sessionScope string) (SessionScope, error) {
	sessionScope = strings.TrimSpace(sessionScope)
	if runtimeMode == RuntimeModeSession && sessionScope == "" {
		return "", fmt.Errorf("session mode requires explicit session_scope flow; session_scope global is reserved for platform-owned system services")
	}
	scope, err := ValidateSessionScopeIntent(runtimeMode, sessionScope)
	if err != nil {
		return "", err
	}
	if runtimeMode == RuntimeModeSession && scope == SessionScopeGlobal {
		return "", fmt.Errorf(authoredGlobalSessionScopeError)
	}
	return scope, nil
}

func ValidateAgentSessionScopeConfig(actor runtimeactors.AgentConfig) (SessionScope, error) {
	runtimeMode := RuntimeModeTask
	if rawMode := strings.TrimSpace(actor.ConversationMode); rawMode != "" {
		parsedMode, err := ParseConversationRuntimeMode(rawMode)
		if err != nil {
			return "", err
		}
		runtimeMode = parsedMode
	}
	validateIntent := ValidateAuthoredSessionScopeIntent
	if actor.HasPlatformInternalSessionScopeAuthority() {
		validateIntent = ValidateSessionScopeIntent
	}
	sessionScope, err := validateIntent(runtimeMode, actor.SessionScope)
	if err != nil {
		return "", err
	}
	switch sessionScope {
	case SessionScopeFlow:
		if actor.CanonicalFlowPath() == "" {
			return "", fmt.Errorf("session_scope flow requires flow path metadata")
		}
	case SessionScopeEntity:
		if actor.CanonicalFlowPath() == "" {
			return "", fmt.Errorf("session_scope entity requires flow path metadata")
		}
	}
	return sessionScope, nil
}

func DeclaredScopeKey(actor runtimeactors.AgentConfig) (string, error) {
	sessionScope, err := ValidateAgentSessionScopeConfig(actor)
	if err != nil {
		return "", err
	}
	switch sessionScope {
	case "":
		return "", nil
	case SessionScopeGlobal:
		return SessionScopeGlobal.String(), nil
	case SessionScopeFlow:
		flowPath := actor.CanonicalFlowPath()
		if flowPath == "" {
			return "", fmt.Errorf("session_scope flow requires flow path metadata")
		}
		return flowPath, nil
	case SessionScopeEntity:
		entityID := actor.EffectiveEntityID()
		if entityID == "" {
			return "", fmt.Errorf("session_scope entity requires entity_id metadata")
		}
		return entityID, nil
	default:
		return "", fmt.Errorf("unsupported session scope %q", actor.SessionScope)
	}
}

func ResolveScope(ctx context.Context, runtimeMode RuntimeMode, sessionScope SessionScope, scopeKey string) (ResolvedScope, error) {
	mode := runtimeMode
	actor, actorOK := runtimeactors.ActorFromContext(ctx)
	validateIntent := ValidateSessionScopeIntent
	if actorOK && !actor.HasPlatformInternalSessionScopeAuthority() {
		validateIntent = ValidateAuthoredSessionScopeIntent
	}
	resolvedScope, err := validateIntent(mode, sessionScope.String())
	if err != nil {
		return ResolvedScope{}, err
	}
	scopeKey = strings.TrimSpace(scopeKey)
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
		if resolvedScope != SessionScopeEntity {
			return ResolvedScope{}, fmt.Errorf("session_per_entity requires explicit session_scope: entity")
		}
		if scopeKey == "" {
			return ResolvedScope{}, fmt.Errorf("session_per_entity requires entity scope key")
		}
		if flowPath == "" {
			return ResolvedScope{}, fmt.Errorf("session_per_entity requires actor flow path")
		}
		out.Scope = SessionScopeEntity
		out.EntityID = scopeKey
		out.FlowInstance = flowPath
		return out, nil
	case RuntimeModeSession:
		switch resolvedScope {
		case SessionScopeGlobal:
			switch scopeKey {
			case "", SessionScopeGlobal.String():
				out.Scope = SessionScopeGlobal
				out.ScopeKey = SessionScopeGlobal.String()
				return out, nil
			default:
				return ResolvedScope{}, fmt.Errorf("session_scope global requires scope_key global or empty")
			}
		case SessionScopeFlow:
			switch {
			case scopeKey == SessionScopeGlobal.String():
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
			return ResolvedScope{}, fmt.Errorf("session mode requires explicit session_scope")
		}
	}

	return ResolvedScope{}, fmt.Errorf("unsupported conversation mode %q", runtimeMode.String())
}
