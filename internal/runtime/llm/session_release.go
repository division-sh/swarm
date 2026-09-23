package llm

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

// releasePreProviderSessionLease never lets a postcommit release error turn
// acquired pre-provider work into a failed provider operation.
func releasePreProviderSessionLease(ctx context.Context, registry sessions.Registry, lease *sessions.Lease, agentID string, sink any, priorErr error) error {
	if registry == nil || lease == nil {
		return errors.Join(priorErr, errors.New("acquired session lease release requires a registry and lease"))
	}
	releaseCtx := context.WithoutCancel(ctx)
	result, releaseErr := registry.ReleaseOutcome(releaseCtx, lease)
	if !result.Acknowledged {
		if releaseErr == nil {
			releaseErr = errors.New("session release was not acknowledged")
		}
		return errors.Join(priorErr, ctx.Err(), releaseErr)
	}
	if priorErr != nil || ctx.Err() != nil {
		return errors.Join(priorErr, ctx.Err(), releaseErr)
	}
	if releaseErr != nil {
		const message = "LLM session lease was released but postcommit cleanup failed"
		if logger, ok := sink.(RuntimeLogSink); ok && logger != nil {
			logRunRuntime(releaseCtx, logger, "warn", "session_release_postcommit_failed", message, agentID, lease.SessionID, "", map[string]any{"postcommit_error": releaseErr.Error()}, releaseErr)
		} else {
			diaglog.ProcessLog("warn", "llm-runtime", message, "agent_id", agentID, "session_id", lease.SessionID, "error", releaseErr)
		}
	}
	return nil
}

// A caller cancellation after provider completion must not erase its response.
func releaseCompletedSessionLease(ctx context.Context, registry sessions.Registry, lease *sessions.Lease, agentID string, sink any, priorErr error) error {
	return releasePreProviderSessionLease(context.WithoutCancel(ctx), registry, lease, agentID, sink, priorErr)
}
