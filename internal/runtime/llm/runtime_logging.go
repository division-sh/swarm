package llm

import (
	"context"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	"swarm/internal/runtime/diaglog"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

type RuntimeLogSink interface {
	LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry)
}

func logRunRuntime(ctx context.Context, logger RuntimeLogSink, level, action, message, agentID, sessionID, entityID string, detail any, err error) {
	if logger == nil {
		return
	}
	inbound, _ := runtimebus.InboundEventFromContext(ctx)
	errText := ""
	if err != nil {
		errText = strings.TrimSpace(err.Error())
	}
	diaglog.RunLog(ctx, logger, runtimepipeline.RuntimeLogEntry{
		Level:     strings.TrimSpace(level),
		Message:   strings.TrimSpace(message),
		Component: "llm-runtime",
		Action:    strings.TrimSpace(action),
		EventID:   strings.TrimSpace(inbound.ID),
		EventType: strings.TrimSpace(string(inbound.Type)),
		AgentID:   strings.TrimSpace(agentID),
		EntityID:  strings.TrimSpace(entityID),
		SessionID: strings.TrimSpace(sessionID),
		Detail:    detail,
		Error:     errText,
	})
}

func logPublisherRuntime(ctx context.Context, publisher EventPublisher, level, action, message, agentID, sessionID, entityID string, detail any, err error) {
	if publisher == nil {
		return
	}
	logger, ok := publisher.(RuntimeLogSink)
	if !ok || logger == nil {
		return
	}
	logRunRuntime(ctx, logger, level, action, message, agentID, sessionID, entityID, detail, err)
}

func logSessionRuntime(ctx context.Context, sink any, action, message, agentID, sessionID string, detail any) {
	if logger, ok := sink.(RuntimeLogSink); ok && logger != nil {
		logRunRuntime(ctx, logger, "info", action, message, agentID, sessionID, "", detail, nil)
		return
	}
	if publisher, ok := sink.(EventPublisher); ok && publisher != nil {
		logPublisherRuntime(ctx, publisher, "info", action, message, agentID, sessionID, "", detail, nil)
	}
}

func LogSessionRotatedForRun(ctx context.Context, sink any, agentID, runtimeMode, oldSessionID, newSessionID, scopeKey, reason string, turnCount, parseFailures int) {
	logSessionRuntime(ctx, sink, "session_rotated", "LLM session was rotated", agentID, newSessionID, map[string]any{
		"runtime_mode":   strings.TrimSpace(runtimeMode),
		"old_session_id": strings.TrimSpace(oldSessionID),
		"new_session_id": strings.TrimSpace(newSessionID),
		"scope_key":      strings.TrimSpace(scopeKey),
		"reason":         strings.TrimSpace(reason),
		"turn_count":     turnCount,
		"parse_failures": parseFailures,
	})
}

func LogSessionAdoptedForRun(ctx context.Context, sink any, agentID, runtimeMode, oldSessionID, newSessionID, scopeKey string) {
	logSessionRuntime(ctx, sink, "session_adopted", "LLM session was adopted", agentID, newSessionID, map[string]any{
		"runtime_mode":   strings.TrimSpace(runtimeMode),
		"old_session_id": strings.TrimSpace(oldSessionID),
		"new_session_id": strings.TrimSpace(newSessionID),
		"scope_key":      strings.TrimSpace(scopeKey),
	})
}
