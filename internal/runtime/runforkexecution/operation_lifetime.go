package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

// selectedContractOperation keeps the process responsible for preparation and
// committed work until activation and failure disposition have both settled.
// It is used synchronously by the direct and staged selected execution owners.
type selectedContractOperation struct {
	mu            sync.Mutex
	finishMu      sync.Mutex
	retireMu      sync.Mutex
	retired       bool
	stopRequested bool
	process       *worklifetime.Process
	preparation   *worklifetime.Lease
	preparing     context.Context
	owned         context.Context
	cancel        context.CancelCauseFunc
	cancelOwned   context.CancelCauseFunc
	stop          func()
	selected      *worklifetime.SelectedForkOccurrence
	use           *worklifetime.Lease
}

type selectedOperationContextKey struct{}

func beginSelectedContractOperation(ctx context.Context) (*selectedContractOperation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	process, ok := worklifetime.ProcessFromContext(ctx)
	if !ok {
		return nil, errors.New("selected-contract operation requires its process work owner")
	}
	// Client cancellation governs pre-commit work. Once committed, the process
	// owns disposition even if the client has stopped waiting.
	lease, err := process.Begin(context.WithoutCancel(ctx))
	if err != nil {
		return nil, fmt.Errorf("admit selected-contract operation: %w", err)
	}
	preparing, cancel := context.WithCancelCause(ctx)
	bridgeDone := make(chan struct{})
	stop := context.AfterFunc(lease.Context(), func() {
		defer close(bridgeDone)
		cancel(context.Cause(lease.Context()))
	})
	stopAndJoin := func() {
		if !stop() {
			<-bridgeDone
		}
	}
	owned, cancelOwned := context.WithCancelCause(lease.Context())
	op := &selectedContractOperation{process: process, preparation: lease, cancel: cancel, cancelOwned: cancelOwned, stop: stopAndJoin}
	op.preparing = context.WithValue(preparing, selectedOperationContextKey{}, op)
	op.owned = context.WithValue(owned, selectedOperationContextKey{}, op)
	return op, nil
}

func (o *selectedContractOperation) PreparationContext() context.Context { return o.preparing }

func (o *selectedContractOperation) Context() context.Context {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.use != nil {
		return o.use.Context()
	}
	return o.owned
}

func (o *selectedContractOperation) Bind(identity worklifetime.SelectedForkIdentity) error {
	if o == nil {
		return errors.New("selected-contract binding requires an admitted operation")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.retired {
		return worklifetime.ErrRetired
	}
	if o.preparation == nil {
		return errors.New("selected-contract binding requires an admitted operation")
	}
	if o.selected != nil {
		return errors.New("selected-contract operation is already bound")
	}
	selected, err := o.process.NewSelectedFork(o.owned, identity)
	if err != nil {
		return err
	}
	o.selected = selected
	// This lease owns orchestration, not dispatched work. Quiescence must be
	// able to join transient execution without waiting for its own caller.
	o.use, err = selected.BeginStanding(o.owned)
	return err
}

func (o *selectedContractOperation) Finish() error {
	if o == nil {
		return nil
	}
	o.finishMu.Lock()
	defer o.finishMu.Unlock()
	if o.stop != nil {
		o.stop()
		o.stop = nil
		o.cancel(nil)
	}
	err := o.retireSelected()
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.preparation != nil {
		o.cancelOwned(nil)
		err = o.preparation.Done()
		o.preparation = nil
	}
	return err
}

func (o *selectedContractOperation) retireSelected() error {
	if o == nil {
		return nil
	}
	o.retireMu.Lock()
	defer o.retireMu.Unlock()
	o.mu.Lock()
	o.retired = true
	use, selected := o.use, o.selected
	o.use = nil
	o.mu.Unlock()
	var err error
	if use != nil {
		err = use.Done()
	}
	if selected != nil {
		if joinErr := selected.RetireAndWait(context.Background()); joinErr != nil {
			return errors.Join(err, joinErr)
		}
		o.mu.Lock()
		o.selected = nil
		o.mu.Unlock()
	}
	return err
}

// Retire can race exact binding, but never adopts or cancels a successor. It
// does not wait for orchestration: that caller still owns failure disposition.
func (o *selectedContractOperation) Retire() {
	o.retire(false)
}

func (o *selectedContractOperation) retireForStop() {
	o.retire(true)
}

func (o *selectedContractOperation) retire(stop bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retired = true
	o.stopRequested = o.stopRequested || stop
	o.cancel(worklifetime.ErrRetired)
	o.cancelOwned(worklifetime.ErrRetired)
	o.selected.Retire()
}

func selectedStopOwnsDisposition(ctx context.Context) bool {
	op, ok := ctx.Value(selectedOperationContextKey{}).(*selectedContractOperation)
	if !ok {
		return false
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	return op.stopRequested
}

func (o *selectedContractOperation) releaseOrchestration() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.retired || o.selected == nil || o.use == nil {
		return errors.New("selected context handoff requires its accepted bound operation")
	}
	if err := o.use.Done(); err != nil {
		return err
	}
	o.use = nil
	return nil
}
