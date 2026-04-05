package llm

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"swarm/internal/runtime/sessions"
)

func resolvedSessionScope(ctx context.Context, mode, scopeKey string) (sessions.ResolvedScope, error) {
	return sessions.ResolveScope(ctx, mode, scopeKey)
}

func ensurePlatformSessionID(id string) string {
	id = strings.TrimSpace(id)
	if id != "" {
		return id
	}
	return uuid.NewString()
}

func sessionToken(s *Session) string {
	if s == nil {
		return ""
	}
	if sid := strings.TrimSpace(s.ProviderSessionID); sid != "" {
		return sid
	}
	return strings.TrimSpace(s.ID)
}

func shouldPersistConversationMode(mode string) bool {
	return true
}
