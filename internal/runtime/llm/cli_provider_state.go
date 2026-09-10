package llm

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func (r *ClaudeCLIRuntime) resolveClaudeState(ctx context.Context, actor actors.AgentConfig, request workspace.ClaudeStateRequest, head string) (*workspace.Target, error) {
	resolver, ok := r.workspaces.(workspace.ClaudeWorkspaceResolver)
	if !ok {
		return nil, failures.Wrap(failures.ClassLifecycleConflict, "claude_provider_state_owner_missing", "claude-cli-adapter", "resolve_provider_state", nil, fmt.Errorf("workspace does not own Claude provider state"))
	}
	target, err := resolver.ResolveClaudeWorkspace(ctx, actor, request, head)
	if err != nil {
		return nil, failures.Wrap(failures.ClassLifecycleConflict, "claude_provider_state_unavailable", "claude-cli-adapter", "resolve_provider_state", nil, err)
	}
	if target == nil || target.ClaudeState == nil || target.ClaudeState.Directory() != workspace.ClaudeStateDirectory {
		return nil, failures.New(failures.ClassLifecycleConflict, "claude_provider_state_binding_missing", "claude-cli-adapter", "resolve_provider_state", nil)
	}
	return target, nil
}

func (r *ClaudeCLIRuntime) resolveSessionClaudeState(ctx context.Context, actor actors.AgentConfig, session *Session) (*workspace.Target, error) {
	var request workspace.ClaudeStateRequest
	var err error
	if authority, ok := effects.AuthorityFromContext(ctx); ok && authority.Kind == effects.AuthorityConversationForkChat {
		request, err = workspace.ClaudeForkState(actor.Identity, session.ID)
	} else if claim, ok := deliverylifecycle.ClaimFromContext(ctx); ok && !session.Memory.Enabled {
		request, err = workspace.ClaudeDeliveryState(actor.Identity, claim)
	} else {
		request, err = workspace.ClaudeSessionState(session.Memory, actor.Identity, session.ID)
	}
	if err != nil {
		return nil, err
	}
	target, err := r.resolveClaudeState(ctx, actor, request, session.ProviderSessionID)
	if err == nil {
		session.claudeState = target.ClaudeState
	}
	return target, err
}

func (*ClaudeCLIRuntime) releaseInvocationState(ctx context.Context, session *Session) error {
	if session == nil || session.claudeState == nil {
		return nil
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err := session.claudeState.Release(cleanup)
	if err != nil {
		// Cleanup is not permission to retry a delivery whose provider may have
		// already settled. Keep its cause without exposing a timeout retry.
		return failures.Wrap(failures.ClassLifecycleConflict, "claude_provider_state_release_failed", "claude-cli-adapter", "release_provider_state", nil, err)
	}
	session.claudeState = nil
	return nil
}
