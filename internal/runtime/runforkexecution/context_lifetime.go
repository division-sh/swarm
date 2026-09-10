package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// The inventory belongs to one selected execution owner, not a global task
// tracker. Work tokens remain exclusively owned by worklifetime.Process and
// SelectedForkOccurrence; entries preserve their exact binding and resource handoff.
type selectedForkContexts struct {
	mu         sync.Mutex
	retired    bool
	recovered  bool
	entries    map[*selectedContractOperation]*selectedForkContext
	cleanupErr error
	process    *worklifetime.Process
	capability startupownership.ProcessCapability
	stops      map[string]*SelectedForkContextUse
}

type selectedForkContext struct {
	operation *selectedContractOperation
	binding   runfork.RunForkSelectedContractBinding
	retained  *PreparedSelectedFork
	retiring  bool
	done      chan struct{}
	closed    bool
	err       error
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
	if contexts.retired {
		return nil, errors.Join(worklifetime.ErrRetired, op.Finish())
	}
	if contexts.process == nil || !contexts.recovered || contexts.process != op.process {
		return nil, errors.Join(errors.New("selected preparation requires its reconciled process owner"), op.Finish())
	}
	if contexts.entries == nil {
		contexts.entries = make(map[*selectedContractOperation]*selectedForkContext)
	}
	contexts.entries[op] = &selectedForkContext{operation: op, done: make(chan struct{})}
	return op, nil
}

func (o SelectedContractExecutionOwner) materializePrepared(ctx context.Context, p *PreparedSelectedFork) (runfork.RunForkMaterialization, error) {
	req, err := p.MaterializationRequest()
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	entry := contexts.entries[p.operation]
	if contexts.retired || entry == nil || entry.retiring || entry.binding.BindingID != "" {
		return runfork.RunForkMaterialization{}, errors.New("selected materialization requires its admitted preparation")
	}
	// Publish the concrete binding under the same admission lock as stop/reset.
	// Those consumers cannot observe a committed fork with an unregistered owner.
	result, err := o.ports.fork.MaterializeRunForkForSelectedContractExecution(ctx, req)
	if result.SelectedContractBinding != nil {
		entry.binding = *result.SelectedContractBinding
	}
	if err == nil && (entry.binding.BindingID == "" || entry.binding.ForkRunID != result.ForkRunID) {
		err = errors.New("selected materialization returned no exact binding")
	}
	return result, err
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
		got.AcquisitionID != want.AcquisitionID || got.Backend != want.Backend {
		return errors.New("selected preparation process capability differs from its bound owner")
	}
	return nil
}

func (o SelectedContractExecutionOwner) bindStagedPreparation(op *selectedContractOperation, binding runfork.RunForkSelectedContractBinding) error {
	if err := validateSelectedContractExecutionBinding(binding.ForkRunID, binding); err != nil {
		return err
	}
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	entry := contexts.entries[op]
	if contexts.retired || entry == nil || entry.retiring {
		return worklifetime.ErrRetired
	}
	for _, other := range contexts.entries {
		if other != entry && other.binding.ForkRunID == binding.ForkRunID {
			return fmt.Errorf("selected fork already has an admitted context")
		}
	}
	entry.binding = binding
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
	err := p.Close()
	o.recordPreparationClosed(p.operation, err)
	return err
}

func (o SelectedContractExecutionOwner) recordPreparationClosed(operation *selectedContractOperation, err error) {
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if entry := contexts.entries[operation]; entry != nil && !entry.closed {
		entry.closed, entry.err = true, err
		contexts.cleanupErr = errors.Join(contexts.cleanupErr, err)
		close(entry.done)
		delete(contexts.entries, operation)
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
		if entry.retained != nil {
			finalErr = errors.Join(finalErr, o.disposePreparation(entry.retained))
		}
		select {
		case <-entry.done:
			finalErr = errors.Join(finalErr, entry.err)
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
