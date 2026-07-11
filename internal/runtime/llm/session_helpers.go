package llm

import (
	"context"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func resolvedSessionScope(ctx context.Context, mode sessions.RuntimeMode, sessionScope sessions.SessionScope, scopeKey string) (sessions.ResolvedScope, error) {
	return sessions.ResolveScope(ctx, mode, sessionScope, scopeKey)
}

func ensurePlatformSessionID(id string) string {
	id = strings.TrimSpace(id)
	if id != "" {
		return id
	}
	return uuid.NewString()
}

func shouldPersistConversationMode(mode sessions.RuntimeMode) bool {
	return true
}
