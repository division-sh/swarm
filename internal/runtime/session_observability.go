package runtime

import (
	"fmt"
	"log"
	"strings"

	llm "empireai/internal/runtime/llm"
)

func buildRotationCheckpoint(reason string, s *llm.Session) string {
	summary := buildSessionSummary(s)
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return summary
	}
	if strings.TrimSpace(summary) == "" {
		return fmt.Sprintf("rotation_reason=%s", reason)
	}
	return fmt.Sprintf("rotation_reason=%s\n%s", reason, summary)
}

func logSessionRotated(agentID, runtimeMode, oldSessionID, newSessionID, scopeKey, reason string, turnCount, parseFailures int) {
	agentID = strings.TrimSpace(agentID)
	runtimeMode = strings.TrimSpace(runtimeMode)
	scopeKey = strings.TrimSpace(scopeKey)
	reason = snippetForLog(reason, 180)
	log.Printf(
		"session.rotated agent=%q runtime=%q scope=%q reason=%q old=%q new=%q turn_count=%d parse_failures=%d",
		agentID,
		runtimeMode,
		scopeKey,
		reason,
		strings.TrimSpace(oldSessionID),
		strings.TrimSpace(newSessionID),
		turnCount,
		parseFailures,
	)
}

func logSessionAdopted(agentID, runtimeMode, oldSessionID, newSessionID, scopeKey string) {
	log.Printf(
		"session.adopted agent=%q runtime=%q scope=%q old=%q new=%q",
		strings.TrimSpace(agentID),
		strings.TrimSpace(runtimeMode),
		strings.TrimSpace(scopeKey),
		strings.TrimSpace(oldSessionID),
		strings.TrimSpace(newSessionID),
	)
}
