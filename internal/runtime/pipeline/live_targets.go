package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type LiveTargetMutation struct {
	FlowPath   string
	Activation FlowInstanceActivationRequest
}

type LiveTargetMutationRequest struct {
	Targets    []LiveTargetMutation
	ObservedAt time.Time
}

type LiveTargetMutationResult struct {
	Reconciliation runlifecycle.FlowRunReconciliation
	Instance       flowidentity.Instance
	Created        bool
}

func (pc *PipelineCoordinator) ReconcileFlowRun(ctx context.Context, scope runlifecycle.LiveRunScope, source correlation.SourceArtifactFact) (runlifecycle.FlowRunReconciliation, error) {
	if pc == nil || pc.workflowStore == nil {
		return runlifecycle.FlowRunReconciliation{}, fmt.Errorf("live flow reconciliation requires workflow persistence")
	}
	owner, ok := pc.workflowStore.runLifecycle.(runlifecycle.FlowRunOwner)
	if !ok {
		return runlifecycle.FlowRunReconciliation{}, fmt.Errorf("live flow reconciliation requires its native run owner")
	}
	return owner.ReconcileFlowRun(ctx, scope, source)
}

func (pc *PipelineCoordinator) LoadLiveRun(ctx context.Context, scope runlifecycle.LiveRunScope) (runlifecycle.LiveRun, bool, error) {
	if pc == nil || pc.workflowStore == nil {
		return runlifecycle.LiveRun{}, false, fmt.Errorf("live flow read requires workflow persistence")
	}
	owner, ok := pc.workflowStore.runLifecycle.(runlifecycle.LiveRunReader)
	if !ok {
		return runlifecycle.LiveRun{}, false, fmt.Errorf("live flow read requires its native run owner")
	}
	return owner.LoadLiveRun(ctx, scope)
}

func (s *workflowInstanceStore) LoadRunLiveScope(ctx context.Context, runID string) (runlifecycle.LiveRunScope, error) {
	owner, ok := s.runLifecycle.(runlifecycle.FlowScopeReader)
	if !ok {
		return runlifecycle.LiveRunScope{}, fmt.Errorf("flow scope requires its native run owner")
	}
	return owner.LoadRunLiveScope(ctx, runID)
}

func (pc *PipelineCoordinator) LoadRunLiveScope(ctx context.Context, runID string) (runlifecycle.LiveRunScope, error) {
	if pc == nil || pc.workflowStore == nil {
		return runlifecycle.LiveRunScope{}, fmt.Errorf("flow scope requires workflow persistence")
	}
	return pc.workflowStore.LoadRunLiveScope(ctx, runID)
}

func (pc *PipelineCoordinator) CommitLiveTargets(ctx context.Context, req LiveTargetMutationRequest, owner StandingFlowInstanceOwner) ([]LiveTargetMutationResult, error) {
	results, _, err := pc.prepareLiveTargets(ctx, req, owner, nil)
	return results, err
}

func (pc *PipelineCoordinator) PrepareLiveTargets(ctx context.Context, req LiveTargetMutationRequest, owner StandingFlowInstancePreparationOwner) ([]LiveTargetMutationResult, func() error, error) {
	return pc.prepareLiveTargets(ctx, req, owner, owner)
}

func (pc *PipelineCoordinator) prepareLiveTargets(ctx context.Context, req LiveTargetMutationRequest, owner StandingFlowInstanceOwner, preparation StandingFlowInstancePreparationOwner) ([]LiveTargetMutationResult, func() error, error) {
	if pc == nil || pc.workflowStore == nil || owner == nil || req.ObservedAt.IsZero() {
		return nil, nil, fmt.Errorf("live targets require native persistence, instance owner and observed time")
	}
	var completions []func() error
	results := make([]LiveTargetMutationResult, 0, len(req.Targets))
	for _, target := range req.Targets {
		if err := requireEagerStandingRoot(target.Activation.ContractBundle); err != nil {
			return nil, nil, err
		}
		source, ok := correlation.SourceArtifactFactFromContext(ctx)
		if !ok {
			return nil, nil, fmt.Errorf("live target requires its admitted source")
		}
		reconciled, err := pc.ReconcileFlowRun(ctx, runlifecycle.LiveRunScope{FlowPath: target.FlowPath}, source)
		if err != nil {
			return nil, nil, err
		}
		result := LiveTargetMutationResult{Reconciliation: reconciled}
		if !reconciled.State.Active() || !reconciled.Source.Matches(source) {
			results = append(results, result)
			continue
		}
		if err := pc.workflowStore.AdmitRunExecutionPosture(ctx, reconciled.RunID, pc.executionPosture); err != nil {
			return nil, nil, err
		}
		operationCtx := correlation.WithRunID(ctx, reconciled.RunID)
		operationCtx = effects.WithExecutionMode(operationCtx, effects.ExecutionMode(pc.executionPosture.RootMode()))
		if preparation == nil {
			if err := owner.ReconcileDynamicFlowRuntimeReadinessPlansForRun(operationCtx, req.ObservedAt.UTC()); err != nil {
				return nil, nil, err
			}
		}
		result.Instance, err = standingGenerationConstruction(target.Activation.ContractBundle, target.FlowPath, reconciled.RunID)
		if err != nil {
			return nil, nil, err
		}
		activation := target.Activation
		activation.Instance, err = standingGenerationConstruction(activation.ContractBundle, semanticview.RootExecutionFlowID(activation.ContractBundle), reconciled.RunID)
		if err != nil {
			return nil, nil, err
		}
		if activation.Instance != (flowidentity.Instance{}) {
			if result.Instance.TemplateID != activation.Instance.TemplateID {
				activation.InitialState, activation.Bookkeeping = "", nil
			}
			if preparation != nil {
				var complete func() error
				result.Created, complete, err = preparation.PrepareStandingFlowInstance(operationCtx, activation)
				if err == nil {
					completions = append(completions, complete)
				}
			} else {
				result.Created, err = owner.EnsureFlowInstance(operationCtx, activation)
			}
			if err != nil {
				return nil, nil, err
			}
		}
		results = append(results, result)
	}
	return results, func() error {
		for _, complete := range completions {
			if err := complete(); err != nil {
				return err
			}
		}
		return nil
	}, nil
}
