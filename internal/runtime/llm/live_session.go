package llm

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func acquireLiveSessionAndConversation(ctx context.Context, registry sessions.Registry, conversations ConversationPersistence, identity agentmemory.Identity, lockOwner string) (*sessions.Lease, ConversationRecord, error) {
	if registry == nil {
		return nil, ConversationRecord{}, fmt.Errorf("live session registry is required")
	}
	if acquirer, ok := registry.(LiveSessionAcquirer); ok {
		return acquirer.AcquireLiveSession(ctx, identity, lockOwner)
	}
	if conversations != nil {
		return nil, ConversationRecord{}, fmt.Errorf("selected live session store must own atomic acquire and hydration")
	}
	lease, err := registry.Acquire(ctx, identity, lockOwner)
	if err != nil {
		return nil, ConversationRecord{}, err
	}
	return lease, ConversationRecord{
		SessionID: lease.SessionID, AgentID: identity.AgentID, Identity: identity, Memory: agentmemory.Authored(true),
		RetryReason: lease.RetryReason, RetriesFromSessionID: lease.RetriesFromSessionID,
		Status: "active",
	}, nil
}
