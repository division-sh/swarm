package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (c selectedContractForkLocalRuntimeContainer) prepareCompletionOwner(ctx context.Context, occurrence *worklifetime.SelectedForkOccurrence, attachment *selectedContractAgentRuntime) error {
	catalog, err := runtime.RunLifecycleTerminalCatalog(c.req.LoadedSource.Source)
	if err != nil {
		return fmt.Errorf("build selected completion stage catalog: %w", err)
	}
	scope := runlifecycle.CandidateScope{BundleHash: c.req.LoadedSource.SourceArtifactFact.BundleHash(), SelectedForkRunID: c.req.ForkRunID}
	executor, err := runlifecycle.NewExecutor(c.ports.candidates, scope, catalog, occurrence,
		runlifecycle.ExecutorOptions{GenericSchedules: attachment.genericSchedules, ExecutionContext: ctx})
	if err != nil {
		return err
	}
	attachment.completionExecutor = executor
	attachment.completionDiagnostics = c.diagnostics
	registration, err := c.ports.candidates.RegisterCompletionCandidateSink(ctx, scope, executor)
	if err != nil {
		return errors.Join(err, executor.Retire(context.WithoutCancel(ctx)), executor.Wait(context.WithoutCancel(ctx)))
	}
	attachment.completionRegistration = registration
	return nil
}
