package llm

import (
	"strings"

	"github.com/google/uuid"
	"swarm/internal/runtime/sessions"
)

func resolvedSessionScope(mode, scopeKey string) sessions.ResolvedScope {
	return sessions.ResolveScope(mode, scopeKey)
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
