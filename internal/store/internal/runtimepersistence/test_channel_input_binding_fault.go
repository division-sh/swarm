package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

type ChannelInputBindingFault struct {
	mu              sync.Mutex
	selected        any
	row             *runforkpersistence.ChannelInputBindingContradiction
	cleanupRequired bool
}

func CaptureChannelInputBindingFaultForTest(ctx context.Context, selected any, run string) (*ChannelInputBindingFault, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	fault := &ChannelInputBindingFault{selected: selected}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		fault.row, err = runforkpersistence.CaptureChannelInputBindingContradictionTx(ctx, tx, run)
		return err
	})
	if err != nil {
		return nil, err
	}
	return fault, nil
}

func (f *ChannelInputBindingFault) InstallForTest(ctx context.Context) error {
	if f == nil || f.row == nil {
		return errors.New("channel binding fault requires its captured ordinary run")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cleanupRequired {
		return errors.New("channel binding fault is already installed")
	}
	return runWorkflowProjectionFault(ctx, f.selected, func(ctx context.Context, tx *sql.Tx) error {
		// A lost COMMIT acknowledgement cannot discharge cleanup responsibility.
		f.cleanupRequired = true
		return f.row.InstallTx(ctx, tx)
	})
}

func (f *ChannelInputBindingFault) RestoreForTest(ctx context.Context) error {
	if f == nil || f.row == nil {
		return errors.New("channel binding cleanup requires its captured receipt")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cleanupRequired {
		return nil
	}
	err := runWorkflowProjectionFault(ctx, f.selected, f.row.RestoreTx)
	if err == nil {
		f.cleanupRequired = false
	}
	return err
}
