package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type ScenarioSetupCommand struct {
	Setup       pipeline.ScenarioSetupRequest
	Activations []FlowInstanceActivationCommand
}

type ScenarioSetupCommitOwner interface {
	CommitScenarioSetup(context.Context, ScenarioSetupCommand) (pipeline.ScenarioSetupResult, error)
}

// SetupScenarioEntities owns the explicit import-to-construction handoff.
// Arbitrary field-only imports remain non-executable; a selected root seed
// must independently admit the canonical no-argument constructor.
func (eb *EventBus) SetupScenarioEntities(ctx context.Context, request pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error) {
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return pipeline.ScenarioSetupResult{}, err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return pipeline.ScenarioSetupResult{}, err
	}
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || fact != eb.sourceArtifactFact {
		return pipeline.ScenarioSetupResult{}, fmt.Errorf("scenario setup requires the exact selected runtime source")
	}
	owner := eb.durable.ScenarioSetup
	if owner == nil || eb.templateInstancePlanner == nil || eb.flowActivationFinalizer == nil {
		return pipeline.ScenarioSetupResult{}, fmt.Errorf("scenario setup requires canonical construction and commit owners")
	}
	ctx = correlation.WithRunID(ctx, request.RunID)
	mode := eb.executionPosture.RootMode()
	if err := eb.executionPosture.Admit(mode, "scenario construction"); err != nil {
		return pipeline.ScenarioSetupResult{}, err
	}
	if selected, present := effects.ExecutionModeFromContext(ctx); present && selected != mode {
		return pipeline.ScenarioSetupResult{}, fmt.Errorf("scenario construction execution mode disagrees with the selected runtime")
	}
	ctx = effects.WithExecutionMode(ctx, mode)
	command := ScenarioSetupCommand{Setup: request}
	for _, seed := range request.Entities {
		path := strings.Trim(seed.FlowInstance, "/")
		if seed.EntityID != request.RunID || (path != "" && path != request.RunID) {
			continue
		}
		plan, err := eb.templateInstancePlanner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
			ContractBundle: eb.semanticSource,
			Instance:       flowidentity.Stored(eb.semanticSource, semanticview.RootExecutionFlowID(eb.semanticSource), request.RunID, request.RunID, "", ""),
			OccurredAt:     request.CreatedAt,
			ScenarioSeed:   &seed,
		})
		if err != nil {
			return pipeline.ScenarioSetupResult{}, fmt.Errorf("prepare scenario construction: %w", err)
		}
		topology, err := eb.prepareFlowInstanceActivationRouteTopology(ctx, []pipeline.FlowInstanceActivationPlan{plan})
		if err != nil {
			return pipeline.ScenarioSetupResult{}, err
		}
		command.Activations = append(command.Activations, FlowInstanceActivationCommand{Plan: plan, RouteTopology: topology})
	}
	result, commitErr := owner.CommitScenarioSetup(ctx, command)
	if !result.Acknowledged {
		return pipeline.ScenarioSetupResult{}, errors.Join(commitErr, fmt.Errorf("scenario setup commit was not acknowledged"))
	}
	return result, errors.Join(commitErr, eb.finalizeCommittedFlowInstanceActivations(ctx, result.Activations))
}
