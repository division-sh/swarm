package llm

import (
	"context"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func (c *Conversation) settleUnemittedMemory(ctx context.Context) error {
	if c.Session == nil || !c.Session.Memory.Enabled {
		return nil
	}
	recorder, found := runtimebus.EmittedEventsRecorderFromContext(ctx)
	if !found || recorder == nil || len(recorder.Snapshot()) != 0 {
		return nil
	}
	writer, ok := c.runtime.(interface {
		settleUnemittedMemory(context.Context, *Session) error
	})
	if !ok {
		return nil
	}
	if err := writer.settleUnemittedMemory(ctx, c.Session); err != nil {
		return err
	}
	c.Messages = append([]Message(nil), c.Session.Messages...)
	return nil
}

func settleUnemittedMemory(ctx context.Context, session *Session, acquirer LiveSessionAcquirer, registry sessions.Registry, store ConversationPersistence, owner string, sink any) (retErr error) {
	if store == nil {
		return nil
	}
	identity := session.MemoryIdentity.Normalize()
	lease, record, err := acquireLiveSessionAndConversation(ctx, acquirer, identity, owner)
	if lease != nil {
		defer func() {
			retErr = releaseCompletedSessionLease(ctx, registry, lease, session.AgentID, sink, retErr)
		}()
	}
	if err != nil {
		return err
	}
	if err := lease.ValidateFor(identity, session.ID); err != nil {
		return err
	}
	if record.SessionID != session.ID || record.Identity.Normalize() != identity || record.AgentID != session.AgentID ||
		record.Memory != session.Memory || record.Status != "active" || record.TurnCount != session.TurnCount {
		return fmt.Errorf("unemitted memory settlement snapshot changed")
	}
	// Work from the acquired, already-redacted snapshot, never overwrite it
	// with the provider's unredacted in-process projection.
	end := len(record.Messages)
	for end > 0 && (record.Messages[end-1].Role != "user" || record.Messages[end-1].RuntimeContext != "") {
		end--
	}
	if end == 0 {
		return fmt.Errorf("unemitted memory settlement has no customer input")
	}
	if end == len(record.Messages) {
		return nil
	}
	if err := validateMemorySettlementShape(record.Messages, session.Messages); err != nil {
		return err
	}
	record.Messages = append([]Message(nil), record.Messages[:end]...)
	if err := store.UpsertConversation(ctx, lease, record); err != nil {
		return err
	}
	session.Messages = append([]Message(nil), record.Messages...)
	return nil
}

func validateMemorySettlementShape(stored, expected []Message) error {
	if len(stored) != len(expected) {
		return fmt.Errorf("unemitted memory settlement transcript changed")
	}
	for i, message := range stored {
		if message.Role != expected[i].Role || message.RuntimeContext != expected[i].RuntimeContext || len(message.ToolCalls) != len(expected[i].ToolCalls) {
			return fmt.Errorf("unemitted memory settlement transcript changed")
		}
		for j, call := range message.ToolCalls {
			if call.ID != expected[i].ToolCalls[j].ID || call.Name != expected[i].ToolCalls[j].Name {
				return fmt.Errorf("unemitted memory settlement tool linkage changed")
			}
		}
	}
	return nil
}

func (r *AnthropicAPIRuntime) settleUnemittedMemory(ctx context.Context, session *Session) error {
	return settleUnemittedMemory(ctx, session, r.liveSessions, r.sessions, r.conversations, r.lockOwner, r.events)
}

func (r *OpenAICompatibleRuntime) settleUnemittedMemory(ctx context.Context, session *Session) error {
	return settleUnemittedMemory(ctx, session, r.liveSessions, r.sessions, r.conversations, r.lockOwner, r.events)
}

func (r *OpenAIResponsesRuntime) settleUnemittedMemory(ctx context.Context, session *Session) error {
	return settleUnemittedMemory(ctx, session, r.liveSessions, r.sessions, r.conversations, r.lockOwner, r.events)
}

func (r *ClaudeCLIRuntime) settleUnemittedMemory(ctx context.Context, session *Session) error {
	return settleUnemittedMemory(ctx, session, r.liveSessions, r.sessions, r.conversations, r.lockOwner, r.events)
}

func (r *MockRuntime) settleUnemittedMemory(ctx context.Context, session *Session) error {
	return settleUnemittedMemory(ctx, session, r.liveSessions, r.sessions, r.conversations, r.lockOwner, r.events)
}
