package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func prepareManagedSessionForTurn(ctx context.Context, s *Session, registry sessions.Registry, acquirer LiveSessionAcquirer, lockOwner string, sink any) error {
	if s == nil {
		return errors.New("managed session is required")
	}
	if !s.Memory.Enabled {
		return nil
	}
	if registry == nil {
		return errors.New("managed session rotation requires a live session registry")
	}
	lease, resolved, err := acquireContinuedMemory(ctx, acquirer, s, lockOwner)
	if err != nil {
		if lease != nil {
			err = releasePreProviderSessionLease(ctx, registry, lease, s.AgentID, sink, err)
		}
		return err
	}
	if !resolved.Enabled() || lease == nil {
		return errors.New("managed session rotation requires an exact live lease")
	}
	if s.adoptedFromID != "" {
		LogSessionAdoptedForRun(ctx, sink, resolved.Identity, s.adoptedFromID, lease.SessionID)
		s.adoptedFromID = ""
		return releasePreProviderSessionLease(ctx, registry, lease, s.AgentID, sink, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_capability_turn_identity_mismatch", "llm-conversation", "prepare_session", map[string]any{"session_id": s.ID}))
	}
	return releasePreProviderSessionLease(ctx, registry, lease, s.AgentID, sink, nil)
}

func MaybeRotateAfterParseFailures(ctx context.Context, s *Session, registry sessions.Registry, predecessor *sessions.Lease, threshold int, sink any) (*sessions.Lease, error) {
	if s == nil || registry == nil || threshold <= 0 {
		return nil, nil
	}
	if !s.Memory.Enabled {
		return nil, nil
	}
	if s.ParseFailures < threshold {
		return nil, nil
	}
	oldSessionID := s.ID
	oldTurnCount := s.TurnCount
	oldParseFailures := s.ParseFailures
	lease, err := registry.Rotate(ctx, predecessor, sessions.RotationMetadata{
		TerminationReason: sessions.TerminationReasonFailed,
	})
	if lease == nil {
		return nil, err
	}
	s.ID = lease.SessionID
	s.ProviderSessionID = ""
	s.TurnCount = 0
	s.ParseFailures = 0
	s.Messages = nil
	s.ContextCheckpoints = nil
	if sink != nil {
		LogSessionRotatedForRun(ctx, sink, s.MemoryIdentity, oldSessionID, lease.SessionID, fmt.Sprintf("parse_failures_threshold:%d", threshold), oldTurnCount, oldParseFailures)
	} else {
		LogSessionRotated(
			s.MemoryIdentity,
			oldSessionID,
			lease.SessionID,
			fmt.Sprintf("parse_failures_threshold:%d", threshold),
			oldTurnCount,
			oldParseFailures,
		)
	}
	return lease, err
}

func LogSessionRotated(identity agentmemory.Identity, oldSessionID, newSessionID, reason string, turnCount, parseFailures int) {
	identity = identity.Normalize()
	reason = snippetForLog(reason, 180)
	diaglog.ProcessLog("info", "llm-runtime", "session rotated",
		"agent_id", identity.AgentID(),
		"memory_enabled", true,
		"run_id", identity.RunID,
		"flow_instance", identity.FlowInstance(),
		"reason", reason,
		"old_session_id", strings.TrimSpace(oldSessionID),
		"new_session_id", strings.TrimSpace(newSessionID),
		"turn_count", turnCount,
		"parse_failures", parseFailures,
	)
}

func snippetForLog(text string, max int) string {
	text = strings.TrimSpace(text)
	if max <= 0 || len(text) <= max {
		return text
	}
	return strings.TrimSpace(text[:max]) + "..."
}
