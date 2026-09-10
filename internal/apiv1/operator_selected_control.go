package apiv1

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type SelectedForkControlAdmission interface {
	RequireNormalControl(context.Context, string, runfork.SelectedControl) error
}

func requireNormalRunControl(ctx context.Context, owner SelectedForkControlAdmission, runID string, operation runfork.SelectedControl) error {
	if owner == nil {
		return nil
	}
	err := owner.RequireNormalControl(ctx, runID, operation)
	var unsupported *runfork.SelectedForkControlUnsupported
	if errors.As(err, &unsupported) {
		return NewApplicationError("SELECTED_FORK_CONTROL_UNSUPPORTED", false, map[string]any{
			"operation": unsupported.Operation, "run_id": unsupported.RunID, "binding_id": unsupported.BindingID,
			"required_capability": "selected_fork_deferred_execution",
		})
	}
	return err
}
