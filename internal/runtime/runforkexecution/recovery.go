package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// RecoverSelectedForkContexts is boot reconciliation, not executable recovery.
// Preparation/control admission remains closed until every binding is classified.
func (o SelectedContractExecutionOwner) RecoverSelectedForkContexts(ctx context.Context, req effects.RecoveryRequest) (_ []runfork.SelectedForkRecoveryResult, finalErr error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	ports, err := o.require()
	if err != nil {
		return nil, err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if contexts.retired || contexts.recovered || contexts.process == nil || contexts.capability == nil || len(contexts.entries) != 0 || len(contexts.stops) != 0 {
		return nil, errors.New("selected recovery requires fresh bound process composition")
	}
	lease, err := contexts.process.Begin(ctx)
	if err != nil {
		return nil, err
	}
	admissionFailed := true
	defer func() {
		leaseErr := lease.Done()
		finalErr = errors.Join(finalErr, leaseErr)
		if admissionFailed || leaseErr != nil {
			contexts.retired = true
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
	results := make([]runfork.SelectedForkRecoveryResult, 0, len(entries))
	var diagnostics error
	for _, entry := range entries {
		availability, err := ports.fork.LoadRunBundleAvailability(ctx, entry.Binding.ForkRunID)
		if err != nil {
			return results, errors.Join(diagnostics, err)
		}
		if availability.DataIntegrityError() || availability.BundleHash != entry.BundleHash {
			return results, errors.Join(diagnostics, fmt.Errorf("selected recovery source integrity: %s", availability.DetailString()))
		}
		fact, err := correlation.NewSourceArtifactFact(entry.BundleHash)
		if err != nil {
			return results, errors.Join(diagnostics, err)
		}
		runCtx := authoractivity.WithScope(correlation.WithSourceArtifactFact(ctx, fact), authoractivity.BundleScope(process.RuntimeInstanceID, entry.BundleHash))
		result, err := ports.fork.RecoverSelectedFork(runCtx, runcontrol.SelectedForkRecoveryRequest{Entry: entry, Process: process, Effects: req})
		if result.RunID != entry.Binding.ForkRunID || !selectedRecoveryDispositionValid(result.Disposition) {
			return results, errors.Join(diagnostics, err, fmt.Errorf("recover selected fork %s: missing or mismatched acknowledged result", entry.Binding.ForkRunID))
		}
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return results, errors.Join(diagnostics, err, ctx.Err())
		}
		results = append(results, result)
		if result.Disposition == runfork.SelectedForkRecoveryCurrent {
			return results, errors.Join(diagnostics, err, fmt.Errorf("selected startup encountered unregistered current-process execution"))
		}
		if err != nil {
			diagnostics = errors.Join(diagnostics, fmt.Errorf("recover selected fork %s: %w", entry.Binding.ForkRunID, err))
		}
	}
	if err := ctx.Err(); err != nil {
		return results, errors.Join(diagnostics, err)
	}
	contexts.recovered = true
	admissionFailed = false
	return results, diagnostics
}

func selectedRecoveryDispositionValid(disposition runfork.SelectedForkRecoveryDisposition) bool {
	switch disposition {
	case runfork.SelectedForkRecoveryTerminal, runfork.SelectedForkRecoveryStaged,
		runfork.SelectedForkRecoveryControlOnly, runfork.SelectedForkRecoveryCurrent,
		runfork.SelectedForkRecoveryFailed:
		return true
	default:
		return false
	}
}
