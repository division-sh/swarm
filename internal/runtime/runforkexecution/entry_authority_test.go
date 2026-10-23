package runforkexecution

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// Component fixtures retain the process acquired by their execution owner.
// Supplied originals, including refused or retired ones, are never replaced.
func executeSelectedContractRunForkForTest(ctx context.Context, req SelectedContractExecutionRequest) (SelectedContractExecutionResult, error) {
	ctx, err := selectedFixtureEntryContext(ctx, req.Owner)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	return ExecuteSelectedContractRunFork(ctx, req)
}

func selectedFixtureEntryContext(ctx context.Context, owner SelectedContractExecutionOwner) (context.Context, error) {
	if startupownership.HasMutationAuthority(ctx) || owner.ports == nil || owner.ports.contexts == nil || owner.ports.contexts.capability == nil {
		return ctx, nil
	}
	return startupownership.WithProcessMutationAuthority(ctx, owner.ports.contexts.capability)
}
