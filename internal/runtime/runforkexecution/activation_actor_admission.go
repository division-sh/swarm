package runforkexecution

import (
	"context"
	"errors"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

func observeSelectedActivationActor(ctx context.Context, req publishSelectedContractForkEventsRequest, managed *manager.AgentManager, runtimes *llm.AgentRuntimeSet, executor *tools.Executor, binding toolgateway.Binding, turns *mcp.TurnContextRegistry, actor actors.AgentConfig) error {
	token, ok := effects.LifecycleTokenFromContext(ctx)
	if !ok || managed == nil || token.Identity != actor.Identity || req.Prepared == nil || req.Prepared.catalog == nil {
		return errors.New("selected activation requires its exact lifecycle token and preparation")
	}
	if err := managed.ProveUnpublishedActivation(token); err != nil {
		return err
	}
	plan, err := actor.Identity.Plan()
	if err != nil {
		return err
	}
	options := req.AgentRuntime.Options
	for _, original := range req.Prepared.catalog.Actors() {
		if original.Plan == plan {
			// An original actor never falls back to a newly inferred catalog when
			// its frozen configuration no longer agrees.
			return req.Prepared.catalog.ObserveActivationGateway(ctx, options.Config, binding, turns, options.Workspace, actor, req.Prepared.preparationID, req.Prepared.coordinates, options.ProcessCapability)
		}
	}
	admission, err := managed.ProveUnpublishedFlowActor(ctx, token, actor)
	if err != nil {
		return err
	}
	if admission.RunID != req.ForkRunID {
		return errors.New("selected activation belongs to another child")
	}
	configuration, err := selectedPreparationFingerprint(options.Config)
	if err != nil {
		return err
	}
	if configuration != req.Prepared.coordinates.ConfigurationFingerprint {
		return errors.New("selected activation runtime configuration changed after preparation")
	}
	prospective := actor
	prospective.Identity = agentidentity.Identity{}
	catalog, err := runtimepkg.PrepareSelectedForkProviderCatalog(ctx, runtimes, executor, []manager.AgentMaterializationBlueprint{{Identity: plan, Config: prospective}})
	if err != nil {
		return err
	}
	coordinates := req.Prepared.coordinates
	coordinates.AdmittedPlanFingerprint = admission.PlanFingerprint
	coordinates.CatalogFingerprint = catalog.Fingerprint()
	if err := catalog.ObserveActivationGateway(ctx, options.Config, binding, turns, options.Workspace, actor, uuid.NewString(), coordinates, options.ProcessCapability); err != nil {
		return err
	}
	current, err := managed.ProveUnpublishedFlowActor(ctx, token, actor)
	if err != nil {
		return err
	}
	if current != admission {
		return errors.New("selected activation changed construction attempt during observation")
	}
	return nil
}
