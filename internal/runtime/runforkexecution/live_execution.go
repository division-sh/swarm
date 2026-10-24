package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// Dispatch remains selected until canonical publication settlement. The native
// activation then checks the exact receipts and parks/transfers in one attempt.
func (c selectedContractForkLocalRuntimeContainer) activateLive(p *PreparedSelectedFork, req SelectedContractExecutionRequest, result SelectedContractExecutionResult) (out SelectedContractExecutionResult, finalErr error) {
	out = result
	runtime := c.attachment.runtime
	if err := runtime.startExecution(c.attachment.ctx, c.attachment.pipeline, c.diagnostics, p.recoveryFromExecutionID != ""); err != nil {
		return out, err
	}
	inputs, err := c.commitSelectedInputs()
	defer func() {
		for _, input := range inputs {
			finalErr = errors.Join(finalErr, runtime.bus.AbandonPreparedPublish(context.WithoutCancel(input.ctx), input.prepared))
		}
	}()
	if err != nil {
		return out, err
	}
	for _, input := range inputs {
		if err := c.dispatchCommittedInput(input); err != nil {
			return out, err
		}
		out.ExecutedEventCount++
	}
	// Stop actors and join accepted work while their selected grant still exists.
	// Shutdown retires that grant and therefore belongs after the native handoff.
	if err := runtime.stopExecutionWork(); err != nil {
		return out, err
	}
	evidence, err := req.LiveRuntime.CurrentStartupGrantEvidence()
	if err != nil {
		return out, err
	}
	ctx := startupownership.WithGenerationGrantEvidence(p.operation.Context(), evidence)
	ctx = effects.WithAuthority(ctx, c.authority)
	activation, err := c.ports.fork.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		InheritedSchedules: p.readiness, ForkOperation: p.forkOperation, DataPins: result.Materialization.DataPins,
		ExecutionSource: p.loadedSource.Source, ForkRunID: result.Materialization.ForkRunID,
		AllowedSourceEventIDs: selectedContractExecutionFrontierEventIDs(p.frontier.FrontierEvents),
		FrontierAdmission:     p.frontier, RouteTopology: p.routeTopology, RecipientPlanning: *p.model.RecipientPlanning,
		LiveHandoff: true,
	})
	out.Activation = activation
	if !out.Activation.Activated {
		return out, errors.Join(err, fmt.Errorf("live handoff was not acknowledged; selected work remains recoverable"))
	}
	// Post-commit reconciliation can fail without undoing the immutable ACK.
	return out, err
}
