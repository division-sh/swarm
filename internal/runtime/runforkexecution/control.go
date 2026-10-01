package runforkexecution

import (
	"context"

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
	return runfork.RequireNormalControlForBinding(runID, operation, binding, exists)
}
