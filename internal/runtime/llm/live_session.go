package llm

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func acquireLiveSessionAndConversation(ctx context.Context, acquirer LiveSessionAcquirer, identity agentmemory.Identity, lockOwner string) (*sessions.Lease, ConversationRecord, error) {
	if acquirer == nil {
		return nil, ConversationRecord{}, fmt.Errorf("live session acquirer is required")
	}
	return acquirer.AcquireLiveSession(ctx, identity, lockOwner)
}

type transientLiveSessionAcquirer struct {
	registry sessions.Registry
}

func newTransientLiveSessionAcquirer(registry sessions.Registry) LiveSessionAcquirer {
	return transientLiveSessionAcquirer{registry: registry}
}

func NewTransientLiveSessionAcquirer(registry sessions.Registry) LiveSessionAcquirer {
	return newTransientLiveSessionAcquirer(registry)
}

func (a transientLiveSessionAcquirer) AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*sessions.Lease, ConversationRecord, error) {
	if a.registry == nil {
		return nil, ConversationRecord{}, fmt.Errorf("transient live session registry is required")
	}
	lease, err := a.registry.Acquire(ctx, identity, lockOwner)
	if err != nil {
		return nil, ConversationRecord{}, err
	}
	return lease, ConversationRecord{
		SessionID: lease.SessionID, AgentID: identity.AgentID(), Identity: identity, Memory: agentmemory.Authored(true),
		RetryReason: lease.RetryReason, RetriesFromSessionID: lease.RetriesFromSessionID,
		Status: "active",
	}, nil
}
