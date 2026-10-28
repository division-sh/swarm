package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// Reset inspection uses the configured immutable-source compiler without
// allocating a runtime projection or admitting a selected execution.
type resetSourceInspection struct {
	SourceArtifactSelectedContractSourceLoader
}

func (l resetSourceInspection) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	return l.InspectRunForkSelectedContractSourceForRequest(ctx, req)
}

func (o SelectedContractExecutionOwner) PrepareResetHistory(ctx context.Context, runIDs []string, loader SourceArtifactSelectedContractSourceLoader) ([]runfork.SelectedResetPreparation, error) {
	if o.ports == nil || o.ports.fork == nil {
		return nil, errors.New("selected reset requires its original fork reader")
	}
	var out []runfork.SelectedResetPreparation
	seen := make(map[string]bool, len(runIDs))
	for _, runID := range runIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id, err := uuid.Parse(runID)
		if err != nil || id == uuid.Nil || id.String() != runID || seen[runID] {
			return nil, errors.New("selected reset requires unique canonical run identities")
		}
		seen[runID] = true
		binding, found, err := o.ports.fork.LoadRunForkSelectedContractBinding(ctx, runID)
		if err != nil {
			return nil, err
		}
		if !found {
			// Detached history is validated from its committed receipt by cleanup;
			// it must not require a source artifact or pack that reset deleted.
			continue
		}
		preparation, err := o.prepareResetBindingHistory(ctx, runID, binding, loader)
		if err != nil {
			return nil, err
		}
		out = append(out, preparation)
	}
	return out, ctx.Err()
}

func (o SelectedContractExecutionOwner) prepareResetBindingHistory(ctx context.Context, runID string, binding runfork.RunForkSelectedContractBinding, loader SourceArtifactSelectedContractSourceLoader) (runfork.SelectedResetPreparation, error) {
	if err := binding.ValidateExecutionBinding(runID); err != nil {
		return runfork.SelectedResetPreparation{}, err
	}
	if loader.PlatformPackBases == nil {
		return runfork.SelectedResetPreparation{}, errors.New("attached selected reset requires its configured pack generation resolver")
	}
	inspection := resetSourceInspection{loader}
	loaded, err := inspection.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		Responsibility: OriginalRunSource(binding.SourceRunID),
		Selection:      runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
	})
	if err != nil {
		return runfork.SelectedResetPreparation{}, fmt.Errorf("inspect selected reset source: %w", err)
	}
	original, err := semanticview.CompileOriginalLoopCarriage(loaded.Source)
	if err != nil {
		return runfork.SelectedResetPreparation{}, err
	}
	if err := original.RequireSource(loaded.SourceArtifactFact.BundleHash()); err != nil {
		return runfork.SelectedResetPreparation{}, err
	}
	request := runfork.RunForkPlanRequest{SourceRunID: binding.SourceRunID,
		At: binding.ForkPoint.EventID, AtStart: binding.ForkPoint.Kind == runfork.RunForkPointRunStart}
	if binding.ForkPoint.Kind == runfork.RunForkPointDeploymentRevision || binding.ForkPoint.Kind == runfork.RunForkPointStateRevision {
		request.ResolvedPoint = &binding.ForkPoint
	}
	plan, err := o.ports.fork.PlanRunFork(ctx, request)
	if err != nil {
		return runfork.SelectedResetPreparation{}, err
	}
	if plan.ForkPoint.Kind != binding.ForkPoint.Kind || plan.ForkPoint.Revision != binding.ForkPoint.Revision || plan.ForkPoint.EventID != binding.ForkPoint.EventID {
		return runfork.SelectedResetPreparation{}, errors.New("selected reset source plan contradicts its immutable binding cut")
	}
	original, err = retainOriginalActivityHistory(ctx, inspection, original, plan)
	if err != nil {
		return runfork.SelectedResetPreparation{}, err
	}
	return runfork.SelectedResetPreparation{Binding: binding, Source: loaded.SourceArtifactFact, Plan: plan, Original: original}, nil
}
