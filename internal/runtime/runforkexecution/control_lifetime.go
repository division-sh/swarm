package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// SelectedForkContextUse is an exact, non-executable control acquisition. Its
// process lease outlives retirement of the selected execution it must join.
type SelectedForkContextUse struct {
	owner      SelectedContractExecutionOwner
	lease      *worklifetime.Lease
	ctx        context.Context
	binding    runfork.RunForkSelectedContractBinding
	process    startupownership.Authority
	entry      *selectedForkContext
	done       chan struct{}
	once       sync.Once
	cleanupErr error
}

func (o SelectedContractExecutionOwner) BindSelectedProcess(ctx context.Context, process *worklifetime.Process, capability startupownership.ProcessCapability) error {
	ports, err := o.require()
	if err != nil {
		return err
	}
	if process == nil || capability == nil {
		return errors.New("selected controls require explicit process construction")
	}
	if err := capability.ProveCurrent(ctx); err != nil {
		return err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if contexts.retired || contexts.process != nil {
		return errors.New("selected control process must be bound exactly once")
	}
	contexts.process, contexts.capability = process, capability
	contexts.stops = make(map[string]*SelectedForkContextUse)
	return nil
}

func (u *SelectedForkContextUse) Done() error {
	if u == nil {
		return nil
	}
	u.once.Do(func() {
		u.cleanupErr = u.lease.Done()
		contexts := u.owner.ports.contexts
		contexts.mu.Lock()
		delete(contexts.stops, u.binding.ForkRunID)
		contexts.cleanupErr = errors.Join(contexts.cleanupErr, u.cleanupErr)
		close(u.done)
		contexts.mu.Unlock()
	})
	return u.cleanupErr
}

func (o SelectedContractExecutionOwner) acquireSelectedStop(ctx context.Context, runID string) (_ *SelectedForkContextUse, selected bool, finalErr error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	ports, err := o.require()
	if err != nil {
		return nil, false, err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if contexts.retired {
		return nil, false, worklifetime.ErrRetired
	}
	if contexts.process == nil || contexts.capability == nil {
		return nil, false, errors.New("selected control process is not bound")
	}
	if !contexts.recovered {
		return nil, false, errors.New("selected control process has not reconciled predecessor work")
	}
	// A stop is not work of the occurrence it retires. Build its exact source
	// scope on a newly admitted process lease, never a borrowed runtime context.
	lease, err := contexts.process.Begin(context.Background())
	if err != nil {
		return nil, false, err
	}
	transferred := false
	defer func() {
		if !transferred {
			finalErr = errors.Join(finalErr, lease.Done())
		}
	}()
	owned := lease.Context()
	binding, exists, err := ports.fork.LoadRunForkSelectedContractBinding(owned, runID)
	if err != nil || !exists {
		return nil, exists, err
	}
	if previous := contexts.stops[runID]; previous != nil {
		return nil, true, errors.New("selected fork stop is already admitted")
	}
	process, err := contexts.capability.Evidence()
	if err != nil {
		return nil, true, err
	}
	if err := (runcontrol.SelectedStopRequest{Transition: runcontrol.TransitionRequest{RunID: runID}, Binding: binding, Process: process}).Validate(); err != nil {
		return nil, true, err
	}
	availability, err := ports.fork.LoadRunBundleAvailability(owned, runID)
	if err != nil {
		return nil, true, err
	}
	if availability.DataIntegrityError() {
		return nil, true, fmt.Errorf("selected stop bundle integrity: %s", availability.DetailString())
	}
	fact, err := correlation.NewSourceArtifactFact(availability.BundleHash)
	if err != nil {
		return nil, true, err
	}
	owned = authoractivity.WithScope(correlation.WithSourceArtifactFact(owned, fact), authoractivity.BundleScope(process.RuntimeInstanceID, fact.BundleHash()))
	use := &SelectedForkContextUse{owner: o, lease: lease, ctx: owned, binding: binding, process: process, done: make(chan struct{})}
	for _, entry := range contexts.entries {
		if entry.binding.ForkRunID != runID {
			continue
		}
		if entry.binding.BindingID != binding.BindingID || entry.retiring {
			return nil, true, errors.New("selected control context is stale or retiring")
		}
		entry.retiring = true
		entry.operation.retireForStop()
		use.entry = entry
		break
	}
	contexts.stops[runID] = use
	transferred = true
	return use, true, nil
}

func (o SelectedContractExecutionOwner) StopSelectedFork(ctx context.Context, req runcontrol.TransitionRequest) (result runcontrol.TransitionResult, selected bool, finalErr error) {
	use, selected, err := o.acquireSelectedStop(ctx, req.RunID)
	if err != nil || !selected {
		return result, selected, err
	}
	defer func() { finalErr = errors.Join(finalErr, use.Done()) }()
	if entry := use.entry; entry != nil {
		if entry.retained != nil {
			if err := o.disposePreparation(entry.retained); err != nil {
				return result, true, err
			}
		}
		// Accepted terminal work remains owned after caller cancellation. Neither
		// reset nor shutdown can release this store before Done settles the use.
		<-entry.done
		if entry.err != nil {
			return result, true, entry.err
		}
	}
	state, err := o.ports.fork.StopSelectedFork(context.WithoutCancel(use.ctx), runcontrol.SelectedStopRequest{Transition: req, Binding: use.binding, Process: use.process})
	if err != nil {
		return result, true, err
	}
	result = runcontrol.TransitionResult{RunID: state.RunID, Status: state.Status, AbandonedDeliveries: state.AbandonedDeliveries,
		Recovery: runcontrol.PostCommitRecovery{Disposition: runcontrol.RecoveryComplete}}
	if len(state.TimerCancellations) != 0 {
		result.Recovery = runcontrol.PostCommitRecovery{Disposition: runcontrol.RecoveryFailed, Err: errors.New("selected stop encountered unsupported durable timer work")}
	}
	return result, true, nil
}
