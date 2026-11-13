package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// The inventory belongs to one selected execution owner, not a global task
// tracker. Work tokens remain exclusively owned by worklifetime.Process and
// SelectedForkOccurrence; entries preserve their exact binding and resource handoff.
type selectedForkContexts struct {
	mu         sync.Mutex
	retired    bool
	draining   bool
	recovered  bool
	recovering bool
	entries    map[*selectedContractOperation]*selectedForkContext
	cleanupErr error
	process    *worklifetime.Process
	capability startupownership.ProcessCapability
	stops      map[string]*SelectedForkContextUse
}

type selectedForkContext struct {
	operation           *selectedContractOperation
	binding             runfork.RunForkSelectedContractBinding
	retained            *PreparedSelectedFork
	scheduler           *pipeline.Scheduler
	retiring            bool
	recoveryReserved    bool
	done                chan struct{}
	orchestrationDone   chan struct{}
	orchestrationJoined bool
	orchestrating       bool
	closed              bool
	err                 error
}

func (o SelectedContractExecutionOwner) beginPreparation(ctx context.Context) (*selectedContractOperation, error) {
	ports, err := o.require()
	if err != nil {
		return nil, err
	}
	op, err := beginSelectedContractOperation(ctx)
	if err != nil {
		return nil, err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if contexts.retired || contexts.draining {
		return nil, errors.Join(worklifetime.ErrRetired, op.Finish())
	}
	if contexts.process == nil || !contexts.recovered || contexts.process != op.process {
		return nil, errors.Join(errors.New("selected preparation requires its reconciled process owner"), op.Finish())
	}
	if contexts.entries == nil {
		contexts.entries = make(map[*selectedContractOperation]*selectedForkContext)
	}
	contexts.entries[op] = &selectedForkContext{operation: op, done: make(chan struct{}), orchestrationDone: make(chan struct{})}
	return op, nil
}

func (o SelectedContractExecutionOwner) materializePrepared(ctx context.Context, p *PreparedSelectedFork) (runfork.RunForkMaterialization, bool, error) {
	req, err := p.MaterializationRequest()
	if err != nil {
		return runfork.RunForkMaterialization{}, false, err
	}
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	entry := contexts.entries[p.operation]
	if contexts.retired || entry == nil || entry.retiring || entry.binding.BindingID != "" {
		return runfork.RunForkMaterialization{}, false, errors.New("selected materialization requires its admitted preparation")
	}
	// Publish the concrete binding under the same admission lock as stop/reset.
	// Those consumers cannot observe a committed fork with an unregistered owner.
	result, err := o.ports.fork.MaterializeRunForkForSelectedContractExecution(ctx, req)
	if err != nil {
		committed, ok := isolatedSelectedForkMaterializationCommit(err)
		if !ok {
			return runfork.RunForkMaterialization{}, false, err
		}
		result = committed
	}
	if bindingErr := requireExactSelectedForkMaterialization(req, result); bindingErr != nil {
		return runfork.RunForkMaterialization{}, false, errors.Join(err, bindingErr)
	}
	entry.binding = *result.SelectedContractBinding
	return result, true, err
}

func (o SelectedContractExecutionOwner) requirePreparationProcess(ctx context.Context, supplied startupownership.ProcessCapability) error {
	ports, err := o.require()
	if err != nil {
		return err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	bound := contexts.capability
	admitted := contexts.recovered && !contexts.retired
	contexts.mu.Unlock()
	if !admitted || bound == nil || supplied == nil {
		return errors.New("selected preparation requires its bound process capability")
	}
	if err := bound.ProveCurrent(ctx); err != nil {
		return fmt.Errorf("prove selected owner's process capability: %w", err)
	}
	want, err := bound.Evidence()
	if err != nil {
		return err
	}
	if err := supplied.ProveCurrent(ctx); err != nil {
		return err
	}
	got, err := supplied.Evidence()
	if err != nil {
		return err
	}
	if got.AuthorityID != want.AuthorityID || got.AuthorityGeneration != want.AuthorityGeneration ||
		got.OwnerID != want.OwnerID || got.BootID != want.BootID || got.RuntimeInstanceID != want.RuntimeInstanceID ||
		got.Backend != want.Backend {
		return errors.New("selected preparation process capability differs from its bound owner")
	}
	return nil
}

func (contexts *selectedForkContexts) bindStagedPreparationLocked(op *selectedContractOperation, binding runfork.RunForkSelectedContractBinding) error {
	entry := contexts.entries[op]
	if contexts.retired || entry == nil || entry.retiring {
		return worklifetime.ErrRetired
	}
	if entry.binding.BindingID != "" {
		if !entry.recoveryReserved || entry.binding != binding {
			return errors.New("selected preparation already has a binding")
		}
	}
	for _, other := range contexts.entries {
		if other != entry && other.binding.ForkRunID == binding.ForkRunID {
			return fmt.Errorf("selected fork already has an admitted context")
		}
	}
	entry.binding = binding
	entry.recoveryReserved = false
	return nil
}

func (o SelectedContractExecutionOwner) retainPrepared(p *PreparedSelectedFork) error {
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	entry := contexts.entries[p.operation]
	if contexts.retired || entry == nil || entry.retiring || entry.binding.BindingID == "" || entry.retained != nil {
		return errors.New("selected context handoff has no current exact binding")
	}
	if err := p.operation.releaseOrchestration(); err != nil {
		return err
	}
	entry.retained = p
	return nil
}

func (o SelectedContractExecutionOwner) beginSelectedOrchestration(p *PreparedSelectedFork) (func(), error) {
	contexts := o.ports.contexts
	contexts.mu.Lock()
	entry := contexts.entries[p.operation]
	if entry == nil || entry.closed || entry.retiring || entry.orchestrating {
		contexts.mu.Unlock()
		return nil, worklifetime.ErrRetired
	}
	entry.orchestrating = true
	contexts.mu.Unlock()
	return func() {
		contexts.mu.Lock()
		defer contexts.mu.Unlock()
		entry.orchestrating = false
		entry.finishOrchestration()
		if entry.closed && entry.retained == nil {
			delete(contexts.entries, p.operation)
		}
	}, nil
}

func (entry *selectedForkContext) finishOrchestration() {
	if entry.orchestrationDone != nil && !entry.orchestrationJoined {
		entry.orchestrationJoined = true
		close(entry.orchestrationDone)
	}
}

func (o SelectedContractExecutionOwner) completePreparation(p *PreparedSelectedFork) error {
	contexts := o.ports.contexts
	contexts.mu.Lock()
	entry := contexts.entries[p.operation]
	retained := entry != nil && entry.retained != nil
	contexts.mu.Unlock()
	if retained {
		return nil
	}
	return o.disposePreparation(p)
}

func (o SelectedContractExecutionOwner) disposePreparation(p *PreparedSelectedFork) error {
	return p.Close()
}

func (o SelectedContractExecutionOwner) recordPreparationCleanupPending(p *PreparedSelectedFork, err error) {
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if entry := contexts.entries[p.operation]; entry != nil {
		if !entry.orchestrating {
			entry.finishOrchestration()
		}
		// Wake the current retirement attempt with failure, but keep the exact
		// preparation and process possession available for explicit cleanup retry.
		entry.retained, entry.err = p, err
		if !entry.closed {
			entry.closed = true
			close(entry.done)
		}
	}
}

func (o SelectedContractExecutionOwner) recordPreparationClosed(operation *selectedContractOperation, err error) {
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if entry := contexts.entries[operation]; entry != nil {
		entry.retained = nil
		entry.err = err
		contexts.cleanupErr = errors.Join(contexts.cleanupErr, err)
		if !entry.closed {
			entry.closed = true
			close(entry.done)
		}
		if !entry.orchestrating {
			entry.finishOrchestration()
			delete(contexts.entries, operation)
		}
	}
}

// FenceSelectedContexts withdraws admission and cancels accepted operations
// before the supervisor joins any dependent normal runtime.
func (o SelectedContractExecutionOwner) FenceSelectedContexts() error {
	ports, err := o.require()
	if err != nil {
		return err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	contexts.retired = true
	for _, entry := range contexts.entries {
		entry.retiring = true
		entry.operation.Retire()
	}
	return nil
}

// RequireResetPredecessor does not retire a live owner on behalf of a caller.
// The supervisor must already have withdrawn this exact process generation.
func (o SelectedContractExecutionOwner) RequireResetPredecessor(ctx context.Context, process *worklifetime.Process, capability startupownership.ProcessCapability) error {
	ports, err := o.require()
	if err != nil {
		return err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	retired, boundProcess, boundCapability := contexts.retired, contexts.process, contexts.capability
	contexts.mu.Unlock()
	if !retired || process == nil || capability == nil || boundProcess != process || boundCapability == nil {
		return errors.New("selected reset requires its retired process predecessor")
	}
	previous, err := boundCapability.Evidence()
	if err != nil {
		return err
	}
	current, err := capability.Evidence()
	if err != nil {
		return err
	}
	if previous != current {
		return errors.New("selected reset cannot adopt a different process capability")
	}
	return o.RetireSelectedContexts(ctx)
}

// SealSelectedContexts closes new preparation without cancelling admitted work.
func (o SelectedContractExecutionOwner) SealSelectedContexts() error {
	ports, err := o.require()
	if err != nil {
		return err
	}
	ports.contexts.mu.Lock()
	ports.contexts.draining = true
	for _, entry := range ports.contexts.entries {
		if entry.scheduler != nil {
			entry.scheduler.CloseDispatch()
		}
	}
	ports.contexts.mu.Unlock()
	return nil
}

func (o SelectedContractExecutionOwner) bindSelectedScheduler(operation *selectedContractOperation, scheduler *pipeline.Scheduler) error {
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	entry := contexts.entries[operation]
	if contexts.retired || entry == nil || entry.retiring || entry.scheduler != nil {
		return worklifetime.ErrRetired
	}
	entry.scheduler = scheduler
	if contexts.draining {
		scheduler.CloseDispatch()
	}
	return nil
}

// RequireAcceptedLiveHandoff admits only the exact operation and target already
// registered before the process sealed selected preparation.
func (o SelectedContractExecutionOwner) RequireAcceptedLiveHandoff(ctx context.Context, reference sourceartifact.RootAdmissionReference) error {
	ports, err := o.require()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	op, _ := ctx.Value(selectedOperationContextKey{}).(*selectedContractOperation)
	contexts := ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	entry := contexts.entries[op]
	if !contexts.draining || contexts.retired || op == nil || entry == nil || entry.closed || entry.retiring || op.process != contexts.process || entry.binding.TargetRootReference != reference {
		return worklifetime.ErrAdmissionFenced
	}
	return reference.Validate()
}

// DrainSelectedContexts joins finite work and its existing orchestration before
// retirement can dispose the selected runtime or its ordinary handoff target.
func (o SelectedContractExecutionOwner) DrainSelectedContexts(ctx context.Context) error {
	ports, err := o.require()
	if err != nil {
		return err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	contexts.draining = true
	entries := make([]*selectedForkContext, 0, len(contexts.entries))
	for _, entry := range contexts.entries {
		if entry.scheduler != nil {
			entry.scheduler.CloseDispatch()
		}
		entries = append(entries, entry)
	}
	contexts.mu.Unlock()
	var drainErr error
	selected := make([]*worklifetime.SelectedForkOccurrence, len(entries))
	for index, entry := range entries {
		entry.operation.mu.Lock()
		selected[index] = entry.operation.selected
		entry.operation.mu.Unlock()
		if selected[index] != nil {
			if err := selected[index].CloseDispatch(); err != nil && !errors.Is(err, worklifetime.ErrRetired) {
				drainErr = errors.Join(drainErr, err)
			}
		}
	}
	for index, entry := range entries {
		if selected[index] != nil {
			drainErr = errors.Join(drainErr, selected[index].WaitForQuiescence(ctx))
		}
		joined := entry.orchestrationDone
		if joined == nil {
			joined = entry.done
		}
		select {
		case <-joined:
		case <-ctx.Done():
			return errors.Join(drainErr, ctx.Err())
		}
	}
	return drainErr
}

// RetireSelectedContexts rejects further preparation and joins every admitted
// operation or retained context before process/store retirement can proceed.
func (o SelectedContractExecutionOwner) RetireSelectedContexts(ctx context.Context) error {
	ports, err := o.require()
	if err != nil {
		return err
	}
	contexts := ports.contexts
	contexts.mu.Lock()
	contexts.retired = true
	finalErr := contexts.cleanupErr
	entries := make([]*selectedForkContext, 0, len(contexts.entries))
	for _, entry := range contexts.entries {
		entry.retiring = true
		entries = append(entries, entry)
	}
	stops := make([]*SelectedForkContextUse, 0, len(contexts.stops))
	for _, use := range contexts.stops {
		stops = append(stops, use)
	}
	contexts.mu.Unlock()
	for _, entry := range entries {
		entry.operation.Retire()
	}
	for _, entry := range entries {
		contexts.mu.Lock()
		retained := entry.retained
		contexts.mu.Unlock()
		if retained != nil {
			finalErr = errors.Join(finalErr, o.disposePreparation(retained))
		}
		select {
		case <-entry.done:
			contexts.mu.Lock()
			finalErr = errors.Join(finalErr, entry.err)
			contexts.mu.Unlock()
		case <-ctx.Done():
			return errors.Join(finalErr, ctx.Err())
		}
	}
	for _, use := range stops {
		select {
		case <-use.done:
			finalErr = errors.Join(finalErr, use.cleanupErr)
		case <-ctx.Done():
			return errors.Join(finalErr, ctx.Err())
		}
	}
	return finalErr
}
