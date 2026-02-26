package runtime

import (
	"fmt"
	"strings"
)

func maybeRotateAfterTurn(s *Session, runtimeMode string, sessions SessionRegistry, lockOwner string, rotateAfter int) (*SessionLease, error) {
	if s == nil || sessions == nil || rotateAfter <= 0 {
		return nil, nil
	}
	if s.TurnCount < rotateAfter {
		return nil, nil
	}
	summary := buildSessionSummary(s)
	lease, err := sessions.Rotate(s.AgentID, runtimeMode, lockOwner, summary, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	s.ID = lease.SessionID
	s.TurnCount = 0
	s.ParseFailures = 0
	s.Messages = []Message{
		{Role: "system", Content: "Previous session summary:\n" + summary},
	}
	return lease, nil
}

func maybeRotateAfterParseFailures(s *Session, runtimeMode string, sessions SessionRegistry, lockOwner string, threshold int) (*SessionLease, error) {
	if s == nil || sessions == nil || threshold <= 0 {
		return nil, nil
	}
	if s.ParseFailures < threshold {
		return nil, nil
	}
	summary := buildSessionSummary(s)
	lease, err := sessions.Rotate(s.AgentID, runtimeMode, lockOwner, summary, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	s.ID = lease.SessionID
	s.TurnCount = 0
	s.ParseFailures = 0
	s.Messages = []Message{
		{Role: "system", Content: "Previous session summary:\n" + summary},
	}
	return lease, nil
}

func buildSessionSummary(s *Session) string {
	if s == nil || len(s.Messages) == 0 {
		return "No prior turns."
	}
	parts := make([]string, 0, 6)
	start := len(s.Messages) - 6
	if start < 0 {
		start = 0
	}
	for _, m := range s.Messages[start:] {
		role := strings.TrimSpace(m.Role)
		if role == "" {
			role = "unknown"
		}
		content := strings.TrimSpace(m.Content)
		if len(content) > 180 {
			content = content[:180] + "..."
		}
		parts = append(parts, fmt.Sprintf("%s: %s", role, content))
	}
	return strings.Join(parts, "\n")
}
