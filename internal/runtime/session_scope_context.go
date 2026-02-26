package runtime

import (
	"context"
	"strings"
)

type sessionScopeContextKey struct{}

type sessionScopeContext struct {
	ConversationMode string
	ScopeKey         string
}

func withSessionScope(ctx context.Context, mode ConversationMode, scopeKey string) context.Context {
	payload := sessionScopeContext{
		ConversationMode: ConversationModeString(mode),
		ScopeKey:         strings.TrimSpace(scopeKey),
	}
	return context.WithValue(ctx, sessionScopeContextKey{}, payload)
}

func sessionScopeFromContext(ctx context.Context) sessionScopeContext {
	if ctx == nil {
		return sessionScopeContext{ConversationMode: "session"}
	}
	v := ctx.Value(sessionScopeContextKey{})
	payload, ok := v.(sessionScopeContext)
	if !ok {
		return sessionScopeContext{ConversationMode: "session"}
	}
	payload.ConversationMode = strings.TrimSpace(strings.ToLower(payload.ConversationMode))
	if payload.ConversationMode == "" {
		payload.ConversationMode = "session"
	}
	payload.ScopeKey = strings.TrimSpace(payload.ScopeKey)
	return payload
}
