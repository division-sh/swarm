package llm

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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

func requireManagedAcquiredBase(ctx context.Context, session *Session, previousTurnCount int, managed *managedProviderCall) error {
	if managed == nil || !session.Memory.Enabled {
		return nil
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok || surface.Authority.SessionID != session.ID || session.TurnCount != previousTurnCount || !managed.frame.MatchesSurface(surface) {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_completion_acquired_base_changed", "llm-completion-authority", "continue_session", map[string]any{"session_id": session.ID, "surface_session_id": surface.Authority.SessionID, "previous_turn_count": previousTurnCount, "acquired_turn_count": session.TurnCount, "frame_matches_surface": managed.frame.MatchesSurface(surface)})
	}
	return nil
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
		return resolvedMemoryExecution{Plan: agentmemory.Plan{}}, nil
	}
	plan := execution.Plan
	identity := execution.Identity.Normalize()
	if err := agentmemory.ValidateIdentity(identity, false); err != nil {
		return resolvedMemoryExecution{}, err
	}
	if strings.TrimSpace(agentID) != identity.AgentID() {
		return resolvedMemoryExecution{}, fmt.Errorf("agent memory identity agent_id %q does not match executing agent %q", identity.AgentID(), strings.TrimSpace(agentID))
	}
	if !plan.Enabled {
		return resolvedMemoryExecution{Plan: plan, Identity: identity}, nil
	}
	if err := agentmemory.ValidateIdentity(identity, true); err != nil {
		return resolvedMemoryExecution{}, err
	}
	return resolvedMemoryExecution{Plan: plan, Identity: identity}, nil
}

func startMemory(ctx context.Context, acquirer LiveSessionAcquirer, agentID, lockOwner string) (*sessions.Lease, ConversationRecord, resolvedMemoryExecution, error) {
	resolved, err := resolveMemoryExecution(ctx, agentID)
	if err != nil || !resolved.Enabled() {
		return nil, ConversationRecord{}, resolved, err
	}
	lease, hydrated, err := acquireLiveSessionAndConversation(ctx, acquirer, resolved.Identity, lockOwner)
	return lease, hydrated, resolved, err
}

func acquireContinuedMemory(ctx context.Context, acquirer LiveSessionAcquirer, session *Session, lockOwner string) (*sessions.Lease, resolvedMemoryExecution, error) {
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
	lease, hydrated, err := acquireLiveSessionAndConversation(ctx, acquirer, resolved.Identity, lockOwner)
	if err != nil {
		return lease, resolved, err
	}
	if lease == nil {
		return nil, resolved, fmt.Errorf("continued memory acquisition returned no exact grant")
	}
	if err := lease.ValidateFor(resolved.Identity, lease.SessionID); err != nil {
		return lease, resolved, err
	}
	// The transient acquirer has no durable conversation. All selected-store
	// acquirers must return the snapshot acquired with the exact grant.
	_, transient := acquirer.(transientLiveSessionAcquirer)
	if !transient {
		if hydrated.SessionID != lease.SessionID || hydrated.Identity.Normalize() != resolved.Identity || hydrated.Memory != resolved.Plan || hydrated.Status != "active" || hydrated.TurnCount < 0 {
			return lease, resolved, fmt.Errorf("continued memory snapshot does not match acquired grant")
		}
		session.adoptedFromID = ""
		if session.ID != lease.SessionID {
			session.adoptedFromID = session.ID
		}
		session.ID = lease.SessionID
		session.Messages = append([]Message(nil), hydrated.Messages...)
		session.TurnCount = hydrated.TurnCount
		session.Watchdog = hydrated.Watchdog
		session.RetryReason = hydrated.RetryReason
		session.RetriesFromSessionID = hydrated.RetriesFromSessionID
		session.ProviderSessionID = lease.ProviderSessionID
	} else {
		session.adoptedFromID = ""
		if session.ID != lease.SessionID {
			session.adoptedFromID = session.ID
			session.ID = lease.SessionID
		}
	}
	if session.pendingAsync != nil && !transient {
		session.Messages = append(session.Messages, *session.pendingAsync)
	}
	session.pendingAsync = nil
	return lease, resolved, nil
}
