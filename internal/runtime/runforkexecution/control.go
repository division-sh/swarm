package runforkexecution

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// RequireNormalControl classifies the exact durable run, never loaded artifacts.
// It performs no mutation and cannot construct a selected execution context.
func (o SelectedContractExecutionOwner) RequireNormalControl(ctx context.Context, runID string, operation runfork.SelectedControl) error {
	if err := operation.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ports, err := o.require()
	if err != nil {
		return err
	}
	binding, exists, err := ports.fork.LoadRunForkSelectedContractBinding(ctx, runID)
	if err != nil {
		return err
	}
	if exists {
		ordinary, err := o.ordinaryHandoff(ctx, runID, binding)
		if err != nil || ordinary {
			return err
		}
	}
	return runfork.RequireNormalControlForBinding(runID, operation, binding, exists)
}

func (o SelectedContractExecutionOwner) ordinaryHandoff(ctx context.Context, runID string, binding runfork.RunForkSelectedContractBinding) (bool, error) {
	var selected *runfork.SelectedForkControlUnsupported
	if err := runfork.RequireNormalControlForBinding(runID, runfork.ControlRunPause, binding, true); !errors.As(err, &selected) {
		return false, err
	}
	reader, available := o.ports.manager.(manager.RetainedActorInspectionReader)
	if !available {
		return false, nil
	}
	availability, err := o.ports.fork.LoadRunBundleAvailability(ctx, runID)
	if err != nil {
		return false, err
	}
	fact, err := correlation.DecodeSourceArtifactFact(availability.BundleHash)
	if err != nil {
		return false, err
	}
	return reader.ObserveOrdinaryRunSource(ctx, fact, runID)
}
