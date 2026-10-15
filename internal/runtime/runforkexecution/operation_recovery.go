package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func (o SelectedContractExecutionOwner) recoverSelectedForkOperation(ctx context.Context, record runfork.ForkOperationRecord, operation *selectedContractOperation, request SelectedContractExecutionRequest) (SelectedContractExecutionResult, error) {
	if err := o.requirePreparationProcess(ctx, request.AgentRuntime.ProcessCapability); err != nil {
		return SelectedContractExecutionResult{}, err
	}
	contexts := o.ports.contexts
	contexts.mu.Lock()
	entry := contexts.entries[operation]
	if contexts.retired || entry == nil || entry.retiring || !entry.recoveryReserved || entry.binding.ForkRunID != record.ForkRunID {
		contexts.mu.Unlock()
		return SelectedContractExecutionResult{}, errors.New("selected operation recovery lost its exact reservation")
	}
	recoveryEntry := runfork.SelectedForkRecoveryEntry{Binding: entry.binding, BundleHash: record.Request.TargetBundleHash}
	capability := contexts.capability
	contexts.mu.Unlock()
	process, err := capability.Evidence()
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	fact, err := admitSelectedRecoverySource(ctx, o.ports.fork, recoveryEntry)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	ctx = authoractivity.WithScope(correlation.WithSourceArtifactFact(ctx, fact), authoractivity.BundleScope(process.RuntimeInstanceID, recoveryEntry.BundleHash))
	recoveryRequest := runcontrol.SelectedForkRecoveryRequest{
		Entry: recoveryEntry, Process: process.LeaseStamp(), Effects: effects.NewRecoveryRequest(time.Now().UTC(), request.AgentRuntime.ExecutionPosture),
	}
	recovered, err := o.ports.fork.RecoverSelectedFork(ctx, recoveryRequest)
	if err != nil || ctx.Err() != nil {
		return SelectedContractExecutionResult{}, errors.Join(err, ctx.Err())
	}
	action, err := selectedRecoveryActionFor(recovered, recoveryEntry)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	inspector, _ := request.SourceLoader.(SelectedContractSourceInspector)
	environment := SelectedForkRecoveryEnvironment{SourceLoader: request.SourceLoader, SourceInspector: inspector, AgentRuntime: request.AgentRuntime}
	for action == selectedRecoverySettleCancellations {
		recovered, err = o.settleRecoveredSelectedCancellations(ctx, recoveryRequest, recovered, environment, operation)
		if err != nil {
			return SelectedContractExecutionResult{}, err
		}
		action, err = selectedRecoveryActionFor(recovered, recoveryEntry)
		if err != nil {
			return SelectedContractExecutionResult{}, err
		}
	}
	if recovered.Operation == nil || recovered.Operation.Request.OperationID != record.Request.OperationID ||
		recovered.Operation.ForkRunID != record.ForkRunID || recovered.Operation.BindingID != record.BindingID {
		return SelectedContractExecutionResult{}, errors.New("selected operation recovery changed the elected operation")
	}
	if action != selectedRecoveryResume && action != selectedRecoveryActivate {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected operation recovery is not executable: %s", recovered.Disposition)
	}
	return o.resumeRecoveredSelectedFork(ctx, recovered, SelectedContractExecutionRequest{SourceLoader: request.SourceLoader, AgentRuntime: request.AgentRuntime}, operation)
}
