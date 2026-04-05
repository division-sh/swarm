package llm

import (
	"context"
	"fmt"
	"strings"

	"swarm/internal/runtime/diaglog"
	"swarm/internal/runtime/sessions"
)

func MaybeRotateAfterTurn(ctx context.Context, s *Session, runtimeMode string, registry sessions.Registry, lockOwner string, rotateAfter int, sink any) (*sessions.Lease, error) {
	if s == nil || registry == nil || rotateAfter <= 0 {
		return nil, nil
	}
	if s.TurnCount < rotateAfter {
		return nil, nil
	}
	oldSessionID := s.ID
	oldTurnCount := s.TurnCount
	oldParseFailures := s.ParseFailures
	summary := BuildRotationCheckpoint(fmt.Sprintf("turn_limit_reached:%d", rotateAfter), s)
	lease, err := registry.Rotate(ctx, s.AgentID, runtimeMode, strings.TrimSpace(s.SessionScope), lockOwner, summary, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	s.ID = lease.SessionID
	s.TurnCount = 0
	s.ParseFailures = 0
	s.Messages = []Message{
		{Role: "system", Content: "Previous session summary:\n" + summary},
	}
	if sink != nil {
		LogSessionRotatedForRun(ctx, sink, s.AgentID, runtimeMode, oldSessionID, lease.SessionID, strings.TrimSpace(s.ScopeKey), fmt.Sprintf("turn_limit_reached:%d", rotateAfter), oldTurnCount, oldParseFailures)
	} else {
		LogSessionRotated(
			s.AgentID,
			runtimeMode,
			oldSessionID,
			lease.SessionID,
			strings.TrimSpace(s.ScopeKey),
			fmt.Sprintf("turn_limit_reached:%d", rotateAfter),
			oldTurnCount,
			oldParseFailures,
		)
	}
	return lease, nil
}

func MaybeRotateAfterParseFailures(ctx context.Context, s *Session, runtimeMode string, registry sessions.Registry, lockOwner string, threshold int, sink any) (*sessions.Lease, error) {
	if s == nil || registry == nil || threshold <= 0 {
		return nil, nil
	}
	if s.ParseFailures < threshold {
		return nil, nil
	}
	oldSessionID := s.ID
	oldTurnCount := s.TurnCount
	oldParseFailures := s.ParseFailures
	summary := BuildRotationCheckpoint(fmt.Sprintf("parse_failures_threshold:%d", threshold), s)
	lease, err := registry.Rotate(ctx, s.AgentID, runtimeMode, strings.TrimSpace(s.SessionScope), lockOwner, summary, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	s.ID = lease.SessionID
	s.TurnCount = 0
	s.ParseFailures = 0
	s.Messages = []Message{
		{Role: "system", Content: "Previous session summary:\n" + summary},
	}
	if sink != nil {
		LogSessionRotatedForRun(ctx, sink, s.AgentID, runtimeMode, oldSessionID, lease.SessionID, strings.TrimSpace(s.ScopeKey), fmt.Sprintf("parse_failures_threshold:%d", threshold), oldTurnCount, oldParseFailures)
	} else {
		LogSessionRotated(
			s.AgentID,
			runtimeMode,
			oldSessionID,
			lease.SessionID,
			strings.TrimSpace(s.ScopeKey),
			fmt.Sprintf("parse_failures_threshold:%d", threshold),
			oldTurnCount,
			oldParseFailures,
		)
	}
	return lease, nil
}

func BuildSessionSummary(s *Session) string {
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

func BuildRotationCheckpoint(reason string, s *Session) string {
	summary := BuildSessionSummary(s)
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return summary
	}
	if strings.TrimSpace(summary) == "" {
		return fmt.Sprintf("rotation_reason=%s", reason)
	}
	return fmt.Sprintf("rotation_reason=%s\n%s", reason, summary)
}

func LogSessionRotated(agentID, runtimeMode, oldSessionID, newSessionID, scopeKey, reason string, turnCount, parseFailures int) {
	agentID = strings.TrimSpace(agentID)
	runtimeMode = strings.TrimSpace(runtimeMode)
	scopeKey = strings.TrimSpace(scopeKey)
	reason = snippetForLog(reason, 180)
	diaglog.ProcessLog("info", "llm-runtime", "session rotated",
		"agent_id", agentID,
		"runtime_mode", runtimeMode,
		"scope_key", scopeKey,
		"reason", reason,
		"old_session_id", strings.TrimSpace(oldSessionID),
		"new_session_id", strings.TrimSpace(newSessionID),
		"turn_count", turnCount,
		"parse_failures", parseFailures,
	)
}

func LogSessionAdopted(agentID, runtimeMode, oldSessionID, newSessionID, scopeKey string) {
	diaglog.ProcessLog("info", "llm-runtime", "session adopted",
		"agent_id", strings.TrimSpace(agentID),
		"runtime_mode", strings.TrimSpace(runtimeMode),
		"scope_key", strings.TrimSpace(scopeKey),
		"old_session_id", strings.TrimSpace(oldSessionID),
		"new_session_id", strings.TrimSpace(newSessionID),
	)
}

func snippetForLog(text string, max int) string {
	text = strings.TrimSpace(text)
	if max <= 0 || len(text) <= max {
		return text
	}
	return strings.TrimSpace(text[:max]) + "..."
}
