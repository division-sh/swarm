package llm

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

// Protocol tests deliberately fake backing. Only the opt-in Docker tests and
// live restart journey receive provider-artifact retention proof credit.
type claudeStateStub struct{}

type claudeSettledTestRegistry struct {
	sessions.Registry
	effects *effecttest.Harness
}

func (r claudeSettledTestRegistry) Acquire(ctx context.Context, identity agentmemory.Identity, owner string) (*sessions.Lease, error) {
	lease, err := r.Registry.Acquire(ctx, identity, owner)
	if err != nil {
		return nil, err
	}
	// Model the selected-store atomic head projection in protocol-only tests.
	for _, settlement := range r.effects.CompletionSettlementsForAdapter("claude_cli") {
		if head := settlement.ProviderHead; head != nil && head.SessionID == lease.SessionID {
			lease.ProviderSessionID = head.NewProviderHead
		}
	}
	return lease, nil
}

func (claudeStateStub) Directory() string                       { return workspace.ClaudeStateDirectory }
func (claudeStateStub) CheckHead(context.Context, string) error { return nil }
func (claudeStateStub) Release(context.Context) error           { return nil }

func (s workspaceResolverStub) ResolveClaudeWorkspace(ctx context.Context, actor actors.AgentConfig, _ workspace.ClaudeStateRequest, _ string) (*workspace.Target, error) {
	target, err := s.ResolveWorkspaceForCapabilityAdmission(ctx, actor)
	if err != nil || target == nil {
		return target, err
	}
	copy := *target
	copy.ClaudeState = claudeStateStub{}
	return &copy, nil
}

type failingClaudeState struct {
	claudeStateStub
	failure error
}

func (s failingClaudeState) Release(context.Context) error { return s.failure }

func TestClaudeInvocationReleasePreservesSessionReadbackAndError(t *testing.T) {
	cause := context.DeadlineExceeded
	runtime := &ClaudeCLIRuntime{}
	session := &Session{ID: "audit-session", claudeState: failingClaudeState{failure: cause}}
	conversation := &Conversation{runtime: runtime, Session: session}
	if err := conversation.releaseInvocationState(context.Background()); !errors.Is(err, cause) {
		t.Fatalf("cleanup error=%v", err)
	} else if engine.FailureDispositionFor(err) != engine.FailureDispositionTerminal {
		t.Fatalf("cleanup failure permits provider redispatch: %v", err)
	}
	if conversation.Session != session || session.claudeState == nil {
		t.Fatal("failed cleanup discarded the invocation")
	}
	session.claudeState = claudeStateStub{}
	if err := conversation.releaseInvocationState(context.Background()); err != nil {
		t.Fatal(err)
	}
	if conversation.Session != session || session.claudeState != nil {
		t.Fatal("cleanup erased completed session readback")
	}
}
