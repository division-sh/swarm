package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	channel "github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

type ChannelCurrentRenderFault struct {
	mu        sync.Mutex
	selected  any
	row       *channel.CurrentRenderCorruption
	installed bool
}

func CaptureChannelCurrentRenderFaultForTest(ctx context.Context, selected any, card string) (*ChannelCurrentRenderFault, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	fault := &ChannelCurrentRenderFault{selected: selected}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		fault.row, err = channel.CaptureCurrentRenderCorruptionTx(ctx, tx, card)
		return err
	})
	if err != nil {
		return nil, err
	}
	return fault, nil
}

func (f *ChannelCurrentRenderFault) InstallForTest(ctx context.Context) error {
	if f == nil || f.row == nil {
		return errors.New("channel render fault requires its original captured row")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.installed {
		return errors.New("channel render fault is already installed")
	}
	err := runWorkflowProjectionFault(ctx, f.selected, f.row.InstallTx)
	if err == nil {
		f.installed = true
	}
	return err
}

func (f *ChannelCurrentRenderFault) RestoreForTest(ctx context.Context) error {
	if f == nil || f.row == nil {
		return errors.New("channel render cleanup requires its original captured row")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.installed {
		return nil
	}
	err := runWorkflowProjectionFault(ctx, f.selected, f.row.RestoreTx)
	if err == nil {
		f.installed = false
	}
	return err
}
