package manager

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

// ProveUnpublishedActivation is an observation of the existing lifecycle owner,
// not a second admission grant. An old generation or published route cannot use
// the pre-publication probe entrance.
func (am *AgentManager) ProveUnpublishedActivation(token effects.LifecycleToken) error {
	_, err := am.unpublishedActivationRevision(token)
	return err
}

func (am *AgentManager) unpublishedActivationRevision(token effects.LifecycleToken) (string, error) {
	if am != nil && am.lifecycle != nil && token.Valid() {
		am.lifecycle.mu.Lock()
		defer am.lifecycle.mu.Unlock()
		cell := am.lifecycle.cells[token.Identity]
		if am.lifecycle.phase == runtimeLifecycleRunning && cell != nil && cell.phase == AgentLifecycleRunning && cell.execution != nil {
			execution := cell.execution
			if execution.token == token && !execution.fenced && execution.routeToken == (effects.LifecycleToken{}) && execution.route == nil && execution.loopDone != nil && execution.generationCtx != nil && execution.generationCtx.Err() == nil {
				return cell.configRevision, nil
			}
		}
	}
	return "", failures.New(failures.ClassLifecycleConflict, "activation_probe_generation_not_current", "agent-lifecycle", "observe_workspace_gateway", nil)
}

func (am *AgentManager) proveUnpublishedActivationActor(ctx context.Context, token effects.LifecycleToken, actor actors.AgentConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !token.Valid() || actor.Identity != token.Identity {
		return fmt.Errorf("activation observation requires its exact actor and lifecycle token")
	}
	revision, err := lifecycleConfigRevision(PersistedAgent{Config: actor})
	if err != nil {
		return err
	}
	current, err := am.unpublishedActivationRevision(token)
	if err != nil {
		return err
	}
	if revision != current {
		return failures.New(failures.ClassLifecycleConflict, "activation_probe_configuration_not_current", "agent-lifecycle", "observe_workspace_gateway", nil)
	}
	return ctx.Err()
}

func (am *AgentManager) observeUnpublishedActivation(ctx context.Context, token effects.LifecycleToken, actor actors.AgentConfig) error {
	if err := am.proveUnpublishedActivationActor(ctx, token, actor); err != nil {
		return err
	}
	if err := am.workspaceGatewayAdmission(ctx, actor); err != nil {
		return err
	}
	return am.proveUnpublishedActivationActor(ctx, token, actor)
}
