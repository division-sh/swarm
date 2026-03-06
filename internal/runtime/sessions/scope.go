package sessions

import (
	"context"
	"strings"
)

type ScopeContext struct {
	ConversationMode string
	ScopeKey         string
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
		return ScopeContext{ConversationMode: "session"}
	}
	v := ctx.Value(scopeContextKey{})
	payload, ok := v.(ScopeContext)
	if !ok {
		return ScopeContext{ConversationMode: "session"}
	}
	payload.ConversationMode = strings.TrimSpace(strings.ToLower(payload.ConversationMode))
	if payload.ConversationMode == "" {
		payload.ConversationMode = "session"
	}
	payload.ScopeKey = strings.TrimSpace(payload.ScopeKey)
	return payload
}
