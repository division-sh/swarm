package runforkexecution

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
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
	if err != nil || !exists {
		return err
	}
	if err := validateSelectedContractExecutionBinding(runID, binding); err != nil {
		return err
	}
	id, err := uuid.Parse(binding.BindingID)
	if err != nil || id == uuid.Nil || id.String() != binding.BindingID {
		return fmt.Errorf("selected control binding has invalid identity")
	}
	return &runfork.SelectedForkControlUnsupported{Operation: operation, RunID: runID, BindingID: binding.BindingID}
}
