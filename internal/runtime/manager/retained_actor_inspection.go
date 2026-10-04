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
	blueprints, err := ResolveStaticTopologyBlueprints(options, source)
	if err != nil {
		return inspection, err
	}
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	desired, err := desiredAgentsFromBlueprints(blueprints, coordinate)
	if err != nil {
		return inspection, err
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		return inspection, err
	}
	admission, err := agenttopology.StaticAdmission(plan.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		return inspection, err
	}
	byKey := make(map[string]staticAgentBlueprint, len(blueprints))
	for _, blueprint := range blueprints {
		key, err := blueprint.Identity.Fingerprint()
		if err != nil {
			return inspection, err
		}
		byKey[key] = blueprint
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
		if actor.Topology.Authority.Kind == agenttopology.AuthorityStaticDeclarationPlan {
			if actor.LifecyclePhase != AgentLifecycleTerminated {
				if actor.Topology.Authority.Static.BundleHash != fact.BundleHash() {
					inspection.Retiring++
					continue
				}
				projected, changed, err := projectStaticTopologyActor(actor, byKey, admission)
				if err != nil {
					return inspection, err
				}
				if projected == nil {
					inspection.Retiring++
					continue
				}
				actor = *projected
				if changed {
					inspection.Reconciliations++
				}
			} else if !actor.Topology.Equal(admission) {
				inspection.Retiring++
				continue
			}
		}
		if err := ValidatePersistedAgentExecution(options, actor.Config); err != nil {
			return inspection, err
		}
		if err := options.ExecutionPosture.Admit(actor.Config.ExecutionMode, "agent lifecycle reconstruction"); err != nil {
			return inspection, err
		}
		if err := bindCanonicalAgentPrompt(source, &actor.Config); err != nil {
			return inspection, err
		}
		if _, err := admitAgentConfigSubscriptions(source, &actor.Config, nil); err != nil {
			return inspection, err
		}
		if err := agentmemory.ValidateFlowOwnership(actor.Config.Memory, actor.Config.CanonicalFlowPath()); err != nil {
			return inspection, err
		}
		if err := ValidateAgentBuildConfiguration(actor.Config); err != nil {
			return inspection, err
		}
		inspection.Configurations = append(inspection.Configurations, actor.Config)
	}
	return inspection, ctx.Err()
}
