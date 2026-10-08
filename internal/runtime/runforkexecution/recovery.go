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
	finite := make([]runfork.SelectedForkRecoveryResult, 0)
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
		if action == selectedRecoveryResumeFiniteFeed || action == selectedRecoveryActivateFiniteFeed {
			if err != nil {
				return results, errors.Join(diagnostics, err)
			}
			finite = append(finite, result)
		}
		if err != nil {
			diagnostics = errors.Join(diagnostics, fmt.Errorf("recover selected fork %s: %w", entry.Binding.ForkRunID, err))
		}
	}
	if err := ctx.Err(); err != nil {
		return results, errors.Join(diagnostics, err)
	}
	if len(finite) != 0 && (environment.SourceLoader == nil || environment.AgentRuntime.ProcessCapability == nil) {
		return results, errors.New("selected finite-feed recovery requires source loader and bound process capability")
	}
	contexts.mu.Lock()
	if contexts.retired {
		contexts.mu.Unlock()
		return results, errors.Join(diagnostics, errors.New("selected recovery owner retired during startup"))
	}
	contexts.recovered = true
	contexts.mu.Unlock()
	request := SelectedContractExecutionRequest{SourceLoader: environment.SourceLoader, AgentRuntime: environment.AgentRuntime}
	for _, result := range finite {
		if err := ctx.Err(); err != nil {
			return results, errors.Join(diagnostics, err)
		}
		switch result.Disposition {
		case runfork.SelectedForkRecoveryActivateFiniteFeed:
			if _, err := o.ActivateRecoveredSelectedFiniteFeed(ctx, result, request); err != nil {
				return results, errors.Join(diagnostics, fmt.Errorf("activate recovered selected finite feed %s: %w", result.RunID, err))
			}
		case runfork.SelectedForkRecoveryResumeFiniteFeed:
			if _, err := o.ResumeRecoveredSelectedFiniteFeed(ctx, result, request); err != nil {
				return results, errors.Join(diagnostics, fmt.Errorf("resume recovered selected finite feed %s: %w", result.RunID, err))
			}
		default:
			return results, errors.Join(diagnostics, fmt.Errorf("selected finite-feed recovery changed disposition %q", result.Disposition))
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
	selectedRecoveryResumeFiniteFeed
	selectedRecoveryActivateFiniteFeed
	selectedRecoverySettleCancellations
)

// Boot recognizes these outcomes but never treats one as executable authority.
func selectedRecoveryActionFor(result runfork.SelectedForkRecoveryResult, entry runfork.SelectedForkRecoveryEntry) (selectedRecoveryAction, error) {
	if result.RunID != entry.Binding.ForkRunID {
		return 0, fmt.Errorf("selected recovery result differs from its fork binding")
	}
	if len(result.PendingCancellations) != 0 {
		id, err := uuid.Parse(result.ExecutionID)
		if err != nil || id == uuid.Nil || id.String() != result.ExecutionID {
			return 0, errors.New("selected pending cancellation lacks exact predecessor execution")
		}
		if result.Disposition != runfork.SelectedForkRecoveryFailed && result.Disposition != runfork.SelectedForkRecoveryControlOnly && result.Disposition != runfork.SelectedForkRecoveryResumeFiniteFeed && result.Disposition != runfork.SelectedForkRecoveryActivateFiniteFeed {
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
	if result.Disposition != runfork.SelectedForkRecoveryResumeFiniteFeed && result.Disposition != runfork.SelectedForkRecoveryActivateFiniteFeed && result.Resume != nil {
		return 0, fmt.Errorf("selected recovery non-resume disposition carries executable work")
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
	case runfork.SelectedForkRecoveryResumeFiniteFeed, runfork.SelectedForkRecoveryActivateFiniteFeed:
		if result.Resume == nil {
			return 0, fmt.Errorf("selected finite-feed recovery lacks exact operation")
		}
		if result.Resume.ForkRunStatus != runfork.RunForkMaterializedStatus {
			return 0, fmt.Errorf("selected finite-feed recovery child is not paused")
		}
		id, err := uuid.Parse(result.ExecutionID)
		if err != nil || id == uuid.Nil || id.String() != result.ExecutionID {
			return 0, fmt.Errorf("selected finite-feed recovery lacks exact predecessor execution")
		}
		operation := result.Resume.Operation
		if _, _, err := operation.Canonical(); err != nil {
			return 0, fmt.Errorf("selected finite-feed recovery operation: %w", err)
		}
		if operation.ResolvedPoint == nil || *operation.ResolvedPoint != entry.Binding.ForkPoint ||
			operation.SourceRunID != entry.Binding.SourceRunID || operation.TargetBundleHash != entry.BundleHash ||
			operation.ContractSelection != entry.Binding.ContractSelection {
			return 0, fmt.Errorf("selected finite-feed recovery operation differs from its fixed binding")
		}
		if len(result.Resume.Pins) == 0 {
			return 0, fmt.Errorf("selected finite-feed recovery lacks durable pins")
		}
		pins := make(map[string]struct{}, len(result.Resume.Pins))
		for _, pin := range result.Resume.Pins {
			if err := pin.Validate(); err != nil || pin.RunID != result.RunID || pin.RunState != "paused" {
				return 0, fmt.Errorf("selected finite-feed recovery pin differs from paused child: %v", err)
			}
			if _, exists := pins[pin.Declaration.Key()]; exists {
				return 0, fmt.Errorf("selected finite-feed recovery repeats declaration %s", pin.Declaration.Key())
			}
			pins[pin.Declaration.Key()] = struct{}{}
		}
		if result.Disposition == runfork.SelectedForkRecoveryActivateFiniteFeed {
			return selectedRecoveryActivateFiniteFeed, nil
		}
		return selectedRecoveryResumeFiniteFeed, nil
	default:
		return 0, fmt.Errorf("selected recovery disposition %q has no boot action", result.Disposition)
	}
}
