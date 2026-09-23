package llm

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

// The provider response and conversation already exist when this mutation runs.
func incrementCompletedSessionTurn(ctx context.Context, registry sessions.Registry, identity agentmemory.Identity, sessionID, agentID string, sink any) error {
	result, err := registry.IncrementTurnOutcome(ctx, identity, sessionID)
	if !result.Acknowledged {
		if err == nil {
			return errors.New("session turn increment was not acknowledged")
		}
		return err
	}
	if err != nil {
		const message = "LLM session turn advanced but postcommit cleanup failed"
		if logger, ok := sink.(RuntimeLogSink); ok && logger != nil {
			logRunRuntime(ctx, logger, "warn", "session_turn_increment_postcommit_failed", message, agentID, sessionID, "", map[string]any{"postcommit_error": err.Error()}, err)
		} else {
			diaglog.ProcessLog("warn", "llm-runtime", message, "agent_id", agentID, "session_id", sessionID, "error", err)
		}
	}
	return nil
}
