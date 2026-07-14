package llm

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func ensurePlatformSessionID(id string) string {
	id = strings.TrimSpace(id)
	if id != "" {
		return id
	}
	return uuid.NewString()
}

func memoryConversationRecord(session *Session) (ConversationRecord, bool, error) {
	if session == nil || !session.Memory.Enabled {
		return ConversationRecord{}, false, nil
	}
	identity := session.MemoryIdentity.Normalize()
	if err := identity.Validate(); err != nil {
		return ConversationRecord{}, false, err
	}
	return ConversationRecord{
		SessionID: session.ID, AgentID: session.AgentID, Identity: identity, Memory: session.Memory,
		Watchdog: session.Watchdog, Messages: session.Messages, Summary: BuildSessionSummary(session),
		TurnCount: session.TurnCount, Status: "active",
	}, true, nil
}

type resolvedMemoryExecution struct {
	Plan     agentmemory.Plan
	Identity agentmemory.Identity
}

func (r resolvedMemoryExecution) Enabled() bool { return r.Plan.Enabled }

func resolveMemoryExecution(ctx context.Context, agentID string) (resolvedMemoryExecution, error) {
	execution, ok := agentmemory.FromContext(ctx)
	if !ok {
		return resolvedMemoryExecution{Plan: agentmemory.PlatformDefault()}, nil
	}
	plan, err := execution.Plan.Normalize()
	if err != nil {
		return resolvedMemoryExecution{}, err
	}
	if !plan.Enabled {
		return resolvedMemoryExecution{Plan: plan}, nil
	}
	identity := execution.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return resolvedMemoryExecution{}, err
	}
	if strings.TrimSpace(agentID) != identity.AgentID {
		return resolvedMemoryExecution{}, fmt.Errorf("agent memory identity agent_id %q does not match executing agent %q", identity.AgentID, strings.TrimSpace(agentID))
	}
	return resolvedMemoryExecution{Plan: plan, Identity: identity}, nil
}

func startMemory(ctx context.Context, registry sessions.Registry, conversations ConversationPersistence, agentID, lockOwner string) (*sessions.Lease, ConversationRecord, resolvedMemoryExecution, error) {
	resolved, err := resolveMemoryExecution(ctx, agentID)
	if err != nil || !resolved.Enabled() {
		return nil, ConversationRecord{}, resolved, err
	}
	lease, hydrated, err := acquireLiveSessionAndConversation(ctx, registry, conversations, resolved.Identity, lockOwner)
	return lease, hydrated, resolved, err
}

func acquireContinuedMemory(ctx context.Context, registry sessions.Registry, session *Session, lockOwner string) (*sessions.Lease, resolvedMemoryExecution, error) {
	resolved, err := resolveMemoryExecution(ctx, session.AgentID)
	if err != nil {
		return nil, resolvedMemoryExecution{}, err
	}
	if session.Memory != resolved.Plan {
		return nil, resolvedMemoryExecution{}, fmt.Errorf("agent memory plan changed during provider session")
	}
	if !resolved.Enabled() {
		return nil, resolved, nil
	}
	if session.MemoryIdentity.Normalize() != resolved.Identity {
		return nil, resolvedMemoryExecution{}, fmt.Errorf("agent memory identity changed during provider session")
	}
	lease, err := registry.Acquire(ctx, resolved.Identity, lockOwner)
	return lease, resolved, err
}
