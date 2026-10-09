package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// SelectedForkRecoveryEnvironment is supplied by serve composition, never
// reconstructed from a selected run's loaded runtime.
type SelectedForkRecoveryEnvironment struct {
	SourceLoader    SelectedContractSourceLoader
	SourceInspector SelectedContractSourceInspector
	AgentRuntime    SelectedContractAgentRuntimeOptions
}

type SelectedContractSourceInspector interface {
	InspectRunForkSelectedContractSourceForRequest(context.Context, SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error)
}

// RecoverSelectedForkContexts classifies durable bindings before admitting
// preparation. Executable handoffs run only after the inventory lock is released.
func (o SelectedContractExecutionOwner) RecoverSelectedForkContexts(ctx context.Context, req effects.RecoveryRequest, environment SelectedForkRecoveryEnvironment) (_ []runfork.SelectedForkRecoveryResult, finalErr error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	ports, err := o.require()
	if err != nil {
		return nil, err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	locked := true
	defer func() {
		if locked {
			contexts.mu.Unlock()
		}
	}()
	if contexts.retired || contexts.recovered || contexts.recovering || contexts.process == nil || contexts.capability == nil || len(contexts.entries) != 0 || len(contexts.stops) != 0 {
		return nil, errors.New("selected recovery requires fresh bound process composition")
	}
	lease, err := contexts.process.Begin(ctx)
	if err != nil {
		return nil, err
	}
	contexts.recovering = true
	admissionFailed := true
	defer func() {
		leaseErr := lease.Done()
		finalErr = errors.Join(finalErr, leaseErr)
		if !locked {
			contexts.mu.Lock()
		}
		contexts.recovering = false
		if admissionFailed || leaseErr != nil {
			contexts.retired = true
		}
		if !locked {
			contexts.mu.Unlock()
		}
	}()
	ctx = lease.Context()
	process, err := contexts.capability.Evidence()
	if err != nil {
		return nil, err
	}
	if err := contexts.capability.ProveCurrent(ctx); err != nil {
		return nil, err
	}
	entries, err := ports.fork.ListSelectedForkRecoveryEntries(ctx)
	if err != nil {
		return nil, err
	}
	contexts.mu.Unlock()
	locked = false
	results := make([]runfork.SelectedForkRecoveryResult, 0, len(entries))
	continuations := make([]runfork.SelectedForkRecoveryResult, 0)
	var diagnostics error
	for _, entry := range entries {
		fact, err := admitSelectedRecoverySource(ctx, ports.fork, entry)
		if err != nil {
			return results, errors.Join(diagnostics, err)
		}
		runCtx := authoractivity.WithScope(correlation.WithSourceArtifactFact(ctx, fact), authoractivity.BundleScope(process.RuntimeInstanceID, entry.BundleHash))
		result, err := ports.fork.RecoverSelectedFork(runCtx, runcontrol.SelectedForkRecoveryRequest{Entry: entry, Process: process, Effects: req})
		if result.RunID != entry.Binding.ForkRunID {
			return results, errors.Join(diagnostics, err, fmt.Errorf("recover selected fork %s: missing or mismatched acknowledged result", entry.Binding.ForkRunID))
		}
		action, actionErr := selectedRecoveryActionFor(result, entry)
		if actionErr != nil {
			return results, errors.Join(diagnostics, err, actionErr)
		}
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return results, errors.Join(diagnostics, err, ctx.Err())
		}
		for action == selectedRecoverySettleCancellations {
			if err != nil {
				return results, errors.Join(diagnostics, err)
			}
			result, err = o.settleRecoveredSelectedCancellations(runCtx, runcontrol.SelectedForkRecoveryRequest{Entry: entry, Process: process, Effects: req}, result, environment)
			if err != nil {
				return append(results, result), errors.Join(diagnostics, err)
			}
			action, actionErr = selectedRecoveryActionFor(result, entry)
			if actionErr != nil {
				return results, errors.Join(err, actionErr)
			}
		}
		results = append(results, result)
		if action == selectedRecoveryRejectCurrent {
			return results, errors.Join(diagnostics, err, fmt.Errorf("selected startup encountered unregistered current-process execution"))
		}
		if action == selectedRecoveryResume || action == selectedRecoveryActivate {
			if err != nil {
				return results, errors.Join(diagnostics, err)
			}
			continuations = append(continuations, result)
		}
		if err != nil {
			diagnostics = errors.Join(diagnostics, fmt.Errorf("recover selected fork %s: %w", entry.Binding.ForkRunID, err))
		}
	}
	if err := ctx.Err(); err != nil {
		return results, errors.Join(diagnostics, err)
	}
	if len(continuations) != 0 && (environment.SourceLoader == nil || environment.AgentRuntime.ProcessCapability == nil) {
		return results, errors.New("selected fork recovery requires source loader and bound process capability")
	}
	contexts.mu.Lock()
	if contexts.retired {
		contexts.mu.Unlock()
		return results, errors.Join(diagnostics, errors.New("selected recovery owner retired during startup"))
	}
	contexts.recovered = true
	contexts.mu.Unlock()
	request := SelectedContractExecutionRequest{SourceLoader: environment.SourceLoader, AgentRuntime: environment.AgentRuntime}
	for _, result := range continuations {
		if err := ctx.Err(); err != nil {
			return results, errors.Join(diagnostics, err)
		}
		switch result.Disposition {
		case runfork.SelectedForkRecoveryActivate:
			if _, err := o.ActivateRecoveredSelectedFork(ctx, result, request); err != nil {
				return results, errors.Join(diagnostics, fmt.Errorf("activate recovered selected fork %s: %w", result.RunID, err))
			}
		case runfork.SelectedForkRecoveryResume:
			if _, err := o.ResumeRecoveredSelectedFork(ctx, result, request); err != nil {
				return results, errors.Join(diagnostics, fmt.Errorf("resume recovered selected fork %s: %w", result.RunID, err))
			}
		default:
			return results, errors.Join(diagnostics, fmt.Errorf("selected fork recovery changed disposition %q", result.Disposition))
		}
	}
	admissionFailed = false
	return results, diagnostics
}

func admitSelectedRecoverySource(ctx context.Context, reader interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}, entry runfork.SelectedForkRecoveryEntry) (correlation.SourceArtifactFact, error) {
	availability, err := reader.LoadRunBundleAvailability(ctx, entry.Binding.ForkRunID)
	if err != nil {
		return correlation.SourceArtifactFact{}, err
	}
	if availability.DataIntegrityError() || availability.BundleHash != entry.BundleHash {
		return correlation.SourceArtifactFact{}, fmt.Errorf("selected recovery source integrity: %s", availability.DetailString())
	}
	return correlation.NewSourceArtifactFact(entry.BundleHash)
}

type selectedRecoveryAction uint8

const (
	selectedRecoveryTerminal selectedRecoveryAction = iota + 1
	selectedRecoveryStaged
	selectedRecoveryControlOnly
	selectedRecoveryFailed
	selectedRecoveryRejectCurrent
	selectedRecoveryResume
	selectedRecoveryActivate
	selectedRecoverySettleCancellations
)

// Boot recognizes these outcomes but never treats one as executable authority.
func selectedRecoveryActionFor(result runfork.SelectedForkRecoveryResult, entry runfork.SelectedForkRecoveryEntry) (selectedRecoveryAction, error) {
	if result.RunID != entry.Binding.ForkRunID {
		return 0, fmt.Errorf("selected recovery result differs from its fork binding")
	}
	if len(result.PendingCancellations) != 0 {
		return selectedCancellationRecoveryAction(result)
	}
	if result.Disposition != runfork.SelectedForkRecoveryResume && result.Disposition != runfork.SelectedForkRecoveryActivate && result.Continuation != nil {
		return 0, fmt.Errorf("selected recovery non-resume disposition carries executable work")
	}
	if err := validateSelectedRecoveryOperation(result, entry); err != nil {
		return 0, err
	}
	switch result.Disposition {
	case runfork.SelectedForkRecoveryTerminal:
		return selectedRecoveryTerminal, nil
	case runfork.SelectedForkRecoveryStaged:
		return selectedRecoveryStaged, nil
	case runfork.SelectedForkRecoveryControlOnly:
		return selectedRecoveryControlOnly, nil
	case runfork.SelectedForkRecoveryFailed:
		return selectedRecoveryFailed, nil
	case runfork.SelectedForkRecoveryCurrent:
		return selectedRecoveryRejectCurrent, nil
	case runfork.SelectedForkRecoveryResume, runfork.SelectedForkRecoveryActivate:
		if result.Continuation == nil || result.Operation == nil {
			return 0, fmt.Errorf("selected fork recovery lacks exact operation")
		}
		if err := validateSelectedRecoveryContinuation(result); err != nil {
			return 0, err
		}
		pins := make(map[string]string, len(result.Continuation.Pins))
		for _, pin := range result.Continuation.Pins {
			if err := pin.Validate(); err != nil || pin.RunID != result.RunID || pin.RunState != result.Continuation.ForkRunStatus {
				return 0, fmt.Errorf("selected fork recovery pin differs from current child: %v", err)
			}
			if _, exists := pins[pin.Declaration.Key()]; exists {
				return 0, fmt.Errorf("selected fork recovery repeats declaration %s", pin.Declaration.Key())
			}
			pins[pin.Declaration.Key()] = string(pin.VersionID)
		}
		for _, explicit := range result.Operation.Request.DataPinOverrides {
			if pins[explicit.Declaration.Key()] != string(explicit.VersionID) {
				return 0, fmt.Errorf("selected recovery lost permanent pin override %s", explicit.Declaration.Key())
			}
		}
		if result.Disposition == runfork.SelectedForkRecoveryActivate {
			return selectedRecoveryActivate, nil
		}
		return selectedRecoveryResume, nil
	default:
		return 0, fmt.Errorf("selected recovery disposition %q has no boot action", result.Disposition)
	}
}

func selectedCancellationRecoveryAction(result runfork.SelectedForkRecoveryResult) (selectedRecoveryAction, error) {
	id, err := uuid.Parse(result.ExecutionID)
	if err != nil || id == uuid.Nil || id.String() != result.ExecutionID {
		return 0, errors.New("selected pending cancellation lacks exact predecessor execution")
	}
	if result.Disposition != runfork.SelectedForkRecoveryFailed && result.Disposition != runfork.SelectedForkRecoveryControlOnly && result.Disposition != runfork.SelectedForkRecoveryResume && result.Disposition != runfork.SelectedForkRecoveryActivate {
		return 0, errors.New("selected pending cancellation contradicts its recovery disposition")
	}
	for _, turn := range result.PendingCancellations {
		if turn.Cancellation.ValidateIntent() != nil || !turn.Cancellation.Origin.Same(turn.Attempt.Origin) {
			return 0, errors.New("selected pending cancellation lacks exact origin evidence")
		}
		if turn.Attempt.AttemptID != "" && (turn.Attempt.Authority.Kind != effects.AuthoritySelectedContractFork || turn.Attempt.Authority.ID != result.ExecutionID || turn.Attempt.Authority.Target.RunID != result.RunID) {
			return 0, errors.New("selected pending cancellation differs from its execution scope")
		}
	}
	return selectedRecoverySettleCancellations, nil
}

func validateSelectedRecoveryOperation(result runfork.SelectedForkRecoveryResult, entry runfork.SelectedForkRecoveryEntry) error {
	if result.Operation == nil {
		return nil
	}
	record := result.Operation
	if err := record.Validate(); err != nil {
		return fmt.Errorf("selected recovery operation: %w", err)
	}
	operation := record.Request
	if record.ForkRunID != result.RunID || record.BindingID != entry.Binding.BindingID ||
		operation.ResolvedPoint == nil || !operation.ResolvedPoint.SameIdentity(entry.Binding.ForkPoint) ||
		operation.SourceRunID != entry.Binding.SourceRunID || operation.TargetBundleHash != entry.BundleHash ||
		operation.ContractSelection != entry.Binding.ContractSelection {
		return fmt.Errorf("selected recovery operation differs from its fixed binding")
	}
	return nil
}

func validateSelectedRecoveryContinuation(result runfork.SelectedForkRecoveryResult) error {
	switch result.Operation.Status {
	case runfork.ForkOperationMaterialized:
		if result.Continuation.ForkRunStatus != runfork.RunForkMaterializedStatus {
			return fmt.Errorf("materialized selected recovery child is not paused")
		}
	case runfork.ForkOperationActivated:
		status := result.Continuation.ForkRunStatus
		if result.Disposition != runfork.SelectedForkRecoveryResume || (status != runfork.RunForkActivatedStatus && status != runfork.RunForkMaterializedStatus) {
			return fmt.Errorf("activated selected recovery cannot reactivate or change child state")
		}
	default:
		return fmt.Errorf("selected recovery cannot resume a failed or uncertain operation")
	}
	if result.ExecutionID == "" {
		if result.Operation.Status != runfork.ForkOperationMaterialized || result.Disposition != runfork.SelectedForkRecoveryResume {
			return fmt.Errorf("selected recovery lacks exact predecessor execution")
		}
		return nil
	}
	id, err := uuid.Parse(result.ExecutionID)
	if err != nil || id == uuid.Nil || id.String() != result.ExecutionID {
		return fmt.Errorf("selected recovery lacks exact predecessor execution")
	}
	return nil
}
