package runforkexecution

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// Component fixtures retain the process acquired by their execution owner.
// Supplied originals, including refused or retired ones, are never replaced.
func executeSelectedContractRunForkForTest(ctx context.Context, req SelectedContractExecutionRequest) (SelectedContractExecutionResult, error) {
	if !startupownership.HasMutationAuthority(ctx) && req.Owner.ports.contexts != nil && req.Owner.ports.contexts.capability != nil {
		var err error
		ctx, err = startupownership.WithProcessMutationAuthority(ctx, req.Owner.ports.contexts.capability)
		if err != nil {
			return SelectedContractExecutionResult{}, err
		}
	}
	return ExecuteSelectedContractRunFork(ctx, req)
}
