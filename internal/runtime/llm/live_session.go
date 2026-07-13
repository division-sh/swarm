package llm

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func acquireLiveSessionAndConversation(ctx context.Context, registry sessions.Registry, conversations ConversationPersistence, agentID string, runtimeMode sessions.RuntimeMode, sessionScope sessions.SessionScope, lockOwner, scopeKey string) (*sessions.Lease, ConversationRecord, error) {
	if registry == nil {
		return nil, ConversationRecord{}, fmt.Errorf("live session registry is required")
	}
	if acquirer, ok := registry.(LiveSessionAcquirer); ok {
		return acquirer.AcquireLiveSession(ctx, agentID, runtimeMode, sessionScope, lockOwner, scopeKey)
	}
	if conversations != nil {
		return nil, ConversationRecord{}, fmt.Errorf("selected live session store must own atomic acquire and hydration")
	}
	lease, err := registry.Acquire(ctx, agentID, runtimeMode, sessionScope, lockOwner, scopeKey)
	if err != nil {
		return nil, ConversationRecord{}, err
	}
	return lease, ConversationRecord{
		SessionID: lease.SessionID, AgentID: agentID, SessionScope: sessionScope.String(), ScopeKey: lease.ScopeKey,
		RetryReason: lease.RetryReason, RetriesFromSessionID: lease.RetriesFromSessionID,
		Mode: runtimeMode.String(), Status: "active",
	}, nil
}
