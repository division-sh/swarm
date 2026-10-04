package runtime

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func (rt *Runtime) observeActivationWorkspaceGateway(ctx context.Context, actor actors.AgentConfig) error {
	resolved, err := rt.LLMRuntimes.ResolveAgentRuntime(actor)
	if err != nil {
		return err
	}
	if resolved.Selection.Profile.ID != selection.BackendClaudeCLI && resolved.Selection.Mode != effects.ExecutionModeMock {
		return nil
	}
	token, ok := effects.LifecycleTokenFromContext(ctx)
	if !ok || token.Identity != actor.Identity {
		return fmt.Errorf("workspace activation observation requires its exact lifecycle token")
	}
	if err := rt.Manager.ProveUnpublishedActivation(token); err != nil {
		return err
	}
	grant, err := rt.currentStartupProbeAuthority()
	if err != nil {
		return err
	}
	if grant.State != startupownership.GrantAdmitted {
		return fmt.Errorf("workspace activation observation requires the current admitted runtime grant")
	}
	preflight, err := rt.managedProviderPreflightAuthority(grant)
	if err != nil {
		return err
	}
	ctx, definitions, capabilities, release, err := startupToolPlan(ctx, actor, resolved.Runtime, rt.ToolExecutor)
	if err != nil {
		return err
	}
	defer release()
	plan, err := actor.Identity.Plan()
	if err != nil {
		return err
	}
	capabilityAuthority, effectAuthority, err := preflight.probeAuthority(ctx, uuid.NewString(), managedProviderPreflightAgent{config: actor, plan: plan})
	if err != nil {
		return err
	}
	surface, err := llm.ManagedCapabilitySurfaceForStartup(ctx, plan, resolved.Runtime, definitions, capabilities, capabilityAuthority)
	if err != nil {
		return err
	}
	ctx = managedcapabilities.WithContext(ctx, surface)
	ctx = effects.WithAuthority(ctx, effectAuthority)
	ctx = effects.WithController(ctx, preflight.EffectController)
	_, err = llm.ObserveWorkspaceGateway(ctx, rt.Config, rt.MCPTurns, actor, definitions, rt.Options.ToolGatewayBinding, rt.Workspace)
	return err
}
