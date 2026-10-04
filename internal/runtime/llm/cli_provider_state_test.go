package llm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
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

type claudeForkWorkspaceCurrencyProbe struct {
	*effecttest.Harness
	authority effects.Authority
	observed  []effects.Authority
}

func (p *claudeForkWorkspaceCurrencyProbe) IsForkChatWorkspaceAuthorityCurrent(_ context.Context, authority effects.Authority) (bool, error) {
	p.observed = append(p.observed, authority)
	return reflect.DeepEqual(authority, p.authority), nil
}

type claudeForkWorkspaceProbe struct {
	workspaceResolverStub
	request  workspace.ClaudeStateRequest
	bindings int
}

func (p *claudeForkWorkspaceProbe) ResolveClaudeWorkspace(ctx context.Context, _ actors.AgentConfig, request workspace.ClaudeStateRequest, _ string) (*workspace.Target, error) {
	if request != p.request {
		return nil, fmt.Errorf("invalid provider backing request")
	}
	current, err := effects.ForkChatWorkspaceCurrent(ctx)
	if err != nil {
		return nil, err
	}
	if !current {
		return nil, fmt.Errorf("fork workspace authority is stale")
	}
	p.bindings++
	return &workspace.Target{Container: "isolated-fork", Workdir: "/workspace", ClaudeState: claudeStateStub{}}, nil
}

func TestClaudeForkWorkspaceReceivesExistingControllerBeforeBackingMutation(t *testing.T) {
	authority := testConversationForkAuthority()
	probe := &claudeForkWorkspaceCurrencyProbe{Harness: effecttest.New(), authority: authority}
	identity := testMemoryIdentity("fork-agent", "fork/one")
	identity.RunID = authority.ForkChat.SourceRunID
	actor := actors.AgentConfig{ID: "fork-agent", Identity: identity, ExecutionMode: effects.ExecutionModeLive}
	session := &Session{ID: "edd33d64-4c3b-40fe-9d8e-bc9c3f445b0d"}
	request, err := workspace.ClaudeForkState(identity, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &claudeForkWorkspaceProbe{request: request}
	runtime := &ClaudeCLIRuntime{workspaces: resolver, completionController: liveTestCompletionController(probe, probe, probe, probe)}
	ctx := effects.WithAuthority(context.Background(), authority)
	// The public fork executor supplies authority, not a completion controller.
	for _, head := range []string{"", "confirmed-private-head"} {
		session.ProviderSessionID = head
		target, err := runtime.resolveSessionClaudeState(ctx, actor, session)
		if err != nil || target == nil || target.Container != "isolated-fork" || session.claudeState == nil {
			t.Fatalf("private backing resolution: target=%+v err=%v", target, err)
		}
	}
	foreign := authority
	foreign.FenceGeneration++
	if target, err := runtime.resolveSessionClaudeState(effects.WithAuthority(ctx, foreign), actor, session); err == nil || target != nil {
		t.Fatalf("foreign fork authority admitted: target=%+v err=%v", target, err)
	}
	if resolver.bindings != 2 || !reflect.DeepEqual(probe.observed, []effects.Authority{authority, authority, foreign}) {
		t.Fatalf("workspace currency/side effects: bindings=%d authorities=%+v", resolver.bindings, probe.observed)
	}
}
