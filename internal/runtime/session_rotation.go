package runtime

import (
	"fmt"
	"strings"

	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/sessions"
)

func maybeRotateAfterTurn(s *llm.Session, runtimeMode string, registry sessions.Registry, lockOwner string, rotateAfter int) (*sessions.Lease, error) {
	if s == nil || registry == nil || rotateAfter <= 0 {
		return nil, nil
	}
	if s.TurnCount < rotateAfter {
		return nil, nil
	}
	oldSessionID := s.ID
	oldTurnCount := s.TurnCount
	oldParseFailures := s.ParseFailures
	summary := buildRotationCheckpoint(fmt.Sprintf("turn_limit_reached:%d", rotateAfter), s)
	lease, err := registry.Rotate(s.AgentID, runtimeMode, lockOwner, summary, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	s.ID = lease.SessionID
	s.TurnCount = 0
	s.ParseFailures = 0
	s.Messages = []llm.Message{
		{Role: "system", Content: "Previous session summary:\n" + summary},
	}
	logSessionRotated(
		s.AgentID,
		runtimeMode,
		oldSessionID,
		lease.SessionID,
		strings.TrimSpace(s.ScopeKey),
		fmt.Sprintf("turn_limit_reached:%d", rotateAfter),
		oldTurnCount,
		oldParseFailures,
	)
	return lease, nil
}

func maybeRotateAfterParseFailures(s *llm.Session, runtimeMode string, registry sessions.Registry, lockOwner string, threshold int) (*sessions.Lease, error) {
	if s == nil || registry == nil || threshold <= 0 {
		return nil, nil
	}
	if s.ParseFailures < threshold {
		return nil, nil
	}
	oldSessionID := s.ID
	oldTurnCount := s.TurnCount
	oldParseFailures := s.ParseFailures
	summary := buildRotationCheckpoint(fmt.Sprintf("parse_failures_threshold:%d", threshold), s)
	lease, err := registry.Rotate(s.AgentID, runtimeMode, lockOwner, summary, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	s.ID = lease.SessionID
	s.TurnCount = 0
	s.ParseFailures = 0
	s.Messages = []llm.Message{
		{Role: "system", Content: "Previous session summary:\n" + summary},
	}
	logSessionRotated(
		s.AgentID,
		runtimeMode,
		oldSessionID,
		lease.SessionID,
		strings.TrimSpace(s.ScopeKey),
		fmt.Sprintf("parse_failures_threshold:%d", threshold),
		oldTurnCount,
		oldParseFailures,
	)
	return lease, nil
}

func buildSessionSummary(s *llm.Session) string {
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
