package manager

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type RetainedActorInspectionReader interface {
	LoadAgents(context.Context) ([]PersistedAgent, error)
	ObserveOrdinaryRunSource(context.Context, correlation.SourceArtifactFact, string) (bool, error)
}

type RetainedActorInspection struct {
	Observed        int
	Foreign         int
	Retiring        int
	Configurations  []models.AgentConfig
	Reconciliations int
}

// InspectRetainedActors projects declaration reconciliation before applying
// hydration's configuration owners. No topology, lifecycle or provider is built.
func InspectRetainedActors(ctx context.Context, reader RetainedActorInspectionReader, source semanticview.Source, fact correlation.SourceArtifactFact, options AgentManagerOptions) (RetainedActorInspection, error) {
	var inspection RetainedActorInspection
	if err := ctx.Err(); err != nil {
		return inspection, err
	}
	if reader == nil || source == nil {
		return inspection, fmt.Errorf("retained actor reader and admitted source are required")
	}
	if err := fact.Validate(); err != nil {
		return inspection, err
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.SourceArtifact == nil || bundle.SourceArtifact.BundleHash() != fact.BundleHash() {
		return inspection, fmt.Errorf("retained actor source differs from its exact artifact fact")
	}
	byKey, admission, err := retainedActorDeclarationAdmission(options, source, fact)
	if err != nil {
		return inspection, err
	}
	actors, err := reader.LoadAgents(ctx)
	if err != nil {
		return inspection, err
	}
	for _, actor := range actors {
		if err := ctx.Err(); err != nil {
			return inspection, err
		}
		inspection.Observed++
		identity, err := actor.Config.ConcreteIdentity()
		if err != nil {
			return inspection, err
		}
		if err := actor.Topology.Validate(); err != nil {
			return inspection, err
		}
		owned, err := reader.ObserveOrdinaryRunSource(ctx, fact, identity.RunID)
		if err != nil {
			return inspection, err
		}
		if !owned {
			inspection.Foreign++
			continue
		}
		projected, changed, err := projectRetainedActorDeclaration(actor, byKey, admission, fact)
		if err != nil {
			return inspection, err
		}
		if projected == nil {
			inspection.Retiring++
			continue
		}
		if changed {
			inspection.Reconciliations++
		}
		actor = *projected
		if err := inspectRetainedActorHydrationConfiguration(options, source, &actor.Config); err != nil {
			return inspection, err
		}
		inspection.Configurations = append(inspection.Configurations, actor.Config)
	}
	return inspection, ctx.Err()
}

func retainedActorDeclarationAdmission(options AgentManagerOptions, source semanticview.Source, fact correlation.SourceArtifactFact) (map[string]staticAgentBlueprint, agenttopology.Admission, error) {
	blueprints, err := ResolveStaticTopologyBlueprints(options, source)
	if err != nil {
		return nil, agenttopology.Admission{}, err
	}
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	desired, err := desiredAgentsFromBlueprints(blueprints, coordinate)
	if err != nil {
		return nil, agenttopology.Admission{}, err
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		return nil, agenttopology.Admission{}, err
	}
	admission, err := agenttopology.StaticAdmission(plan.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		return nil, agenttopology.Admission{}, err
	}
	byKey := make(map[string]staticAgentBlueprint, len(blueprints))
	for _, blueprint := range blueprints {
		key, err := blueprint.Identity.Fingerprint()
		if err != nil {
			return nil, agenttopology.Admission{}, err
		}
		byKey[key] = blueprint
	}
	return byKey, admission, nil
}

func projectRetainedActorDeclaration(actor PersistedAgent, byKey map[string]staticAgentBlueprint, admission agenttopology.Admission, fact correlation.SourceArtifactFact) (*PersistedAgent, bool, error) {
	if actor.Topology.Authority.Kind != agenttopology.AuthorityStaticDeclarationPlan {
		return &actor, false, nil
	}
	if actor.LifecyclePhase == AgentLifecycleTerminated {
		if !actor.Topology.Equal(admission) {
			return nil, false, nil
		}
		return &actor, false, nil
	}
	if actor.Topology.Authority.Static.BundleHash != fact.BundleHash() {
		return nil, false, nil
	}
	return projectStaticTopologyActor(actor, byKey, admission)
}

func inspectRetainedActorHydrationConfiguration(options AgentManagerOptions, source semanticview.Source, config *models.AgentConfig) error {
	if err := ValidatePersistedAgentExecution(options, *config); err != nil {
		return err
	}
	if err := options.ExecutionPosture.Admit(config.ExecutionMode, "agent lifecycle reconstruction"); err != nil {
		return err
	}
	if err := bindCanonicalAgentPrompt(source, config); err != nil {
		return err
	}
	if _, err := admitAgentConfigSubscriptions(source, config, nil); err != nil {
		return err
	}
	if err := agentmemory.ValidateFlowOwnership(config.Memory, config.CanonicalFlowPath()); err != nil {
		return err
	}
	return ValidateAgentBuildConfiguration(*config)
}
