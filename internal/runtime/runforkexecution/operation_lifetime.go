package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

// selectedContractOperation keeps the process responsible for preparation and
// committed work until activation and failure disposition have both settled.
// It is used synchronously by the direct and staged selected execution owners.
type selectedContractOperation struct {
	process     *worklifetime.Process
	preparation *worklifetime.Lease
	selected    *worklifetime.SelectedForkOccurrence
	use         *worklifetime.Lease
}

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
	return &selectedContractOperation{process: process, preparation: lease}, nil
}

func (o *selectedContractOperation) Context() context.Context {
	if o.use != nil {
		return o.use.Context()
	}
	return o.preparation.Context()
}

func (o *selectedContractOperation) Bind(identity worklifetime.SelectedForkIdentity) error {
	if o == nil || o.preparation == nil {
		return errors.New("selected-contract binding requires an admitted operation")
	}
	if o.selected != nil {
		return errors.New("selected-contract operation is already bound")
	}
	selected, err := o.process.NewSelectedFork(o.preparation.Context(), identity)
	if err != nil {
		return err
	}
	o.selected = selected
	// This lease owns orchestration, not dispatched work. Quiescence must be
	// able to join transient execution without waiting for its own caller.
	o.use, err = selected.BeginStanding(o.preparation.Context())
	return err
}

func (o *selectedContractOperation) Finish() error {
	if o == nil {
		return nil
	}
	var err error
	if o.use != nil {
		err = o.use.Done()
		o.use = nil
	}
	if o.selected != nil {
		if joinErr := o.selected.RetireAndWait(context.Background()); joinErr != nil {
			return errors.Join(err, joinErr)
		}
		o.selected = nil
	}
	if o.preparation != nil {
		err = errors.Join(err, o.preparation.Done())
		o.preparation = nil
	}
	return err
}
