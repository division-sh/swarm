package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type selectedRecoveryAdmission struct {
	request SelectedContractExecutionRequest
	binding runfork.RunForkSelectedContractBinding
	action  selectedRecoveryAction
}

func (o SelectedContractExecutionOwner) admitSelectedRecovery(ctx context.Context, recovered runfork.SelectedForkRecoveryResult, request SelectedContractExecutionRequest) (selectedRecoveryAdmission, error) {
	if err := ctx.Err(); err != nil {
		return selectedRecoveryAdmission{}, err
	}
	if recovered.Operation == nil || recovered.Continuation == nil {
		return selectedRecoveryAdmission{}, errors.New("selected recovery lacks durable operation or child attachment")
	}
	if request.SourceRunID != "" || request.At != "" || request.AtStart || request.ExpectedBundleHash != "" || request.ForkOperation != nil ||
		request.DataPinOverrides != nil || request.ContractSelection != (runfork.RunForkContractSelection{}) ||
		request.AllowSourceFreeze || request.SourceArtifactFact.BundleHash() != "" ||
		request.EffectiveSourceIdentity.Digest() != "" || request.EffectiveSourceIdentity.SourceArtifactFact().BundleHash() != "" ||
		request.Recovery != nil {
		return selectedRecoveryAdmission{}, errors.New("selected recovery accepts source loader and runtime options, not caller-selected fork semantics")
	}
	ports, err := o.require()
	if err != nil {
		return selectedRecoveryAdmission{}, err
	}
	if err := recovered.Operation.Validate(); err != nil {
		return selectedRecoveryAdmission{}, fmt.Errorf("selected recovery operation: %w", err)
	}
	operation := recovered.Operation.Request
	binding, err := ports.fork.RequireRunForkSelectedContractBinding(ctx, recovered.RunID)
	if err != nil {
		return selectedRecoveryAdmission{}, err
	}
	action, err := selectedRecoveryActionFor(recovered, runfork.SelectedForkRecoveryEntry{Binding: binding, BundleHash: operation.TargetBundleHash})
	if err != nil {
		return selectedRecoveryAdmission{}, err
	}
	if action != selectedRecoveryResume && action != selectedRecoveryActivate {
		return selectedRecoveryAdmission{}, errors.New("selected recovery has no executable action")
	}
	request.Owner = o
	request.SourceRunID = operation.SourceRunID
	request.AtStart = operation.AtStart
	request.At = operation.ResolvedPoint.Input
	request.ExpectedBundleHash = operation.TargetBundleHash
	request.ContractSelection = operation.ContractSelection
	request.AllowSourceFreeze = operation.AllowSourceFreeze
	operation.DataPinOverrides = append(operation.DataPinOverrides[:0:0], operation.DataPinOverrides...)
	request.DataPinOverrides = append(operation.DataPinOverrides[:0:0], operation.DataPinOverrides...)
	point := *operation.ResolvedPoint
	operation.ResolvedPoint = &point
	request.ForkOperation = &operation
	return selectedRecoveryAdmission{request: request, binding: binding, action: action}, nil
}

// Activation recovery also prepares a fresh exact attachment. The displaced
// drain-only path cannot mint current execution authority for activation.
func (o SelectedContractExecutionOwner) ActivateRecoveredSelectedFork(ctx context.Context, recovered runfork.SelectedForkRecoveryResult, request SelectedContractExecutionRequest) (runfork.RunForkActivation, error) {
	if recovered.Disposition != runfork.SelectedForkRecoveryActivate {
		return runfork.RunForkActivation{}, errors.New("selected activation requires materialized recovery evidence")
	}
	result, err := o.ResumeRecoveredSelectedFork(ctx, recovered, request)
	return result.Activation, err
}
