package devscratch

import (
	"errors"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

// SelectedStore is the exact lifetime surface retained with an opened scratch
// epoch. Domain store capabilities are intentionally absent.
type SelectedStore interface {
	Activate(*worklifetime.Process) error
	CloseUnactivated() error
	CloseActivated(*worklifetime.ProcessJoinReceipt) error
}

// SelectedStoreLifecycle is the composite owner of the selected SQLite store
// and its project scratch epoch.
type SelectedStoreLifecycle struct {
	store SelectedStore
	epoch *storeEpoch
}

// BindOpenedStore consumes the prepared authority. After this call, no public
// operation can release the epoch before a successful selected-store close.
func (a *EpochAuthority) BindOpenedStore(store SelectedStore) (*SelectedStoreLifecycle, error) {
	if store == nil {
		return nil, errors.New("dev scratch selected store is required")
	}
	epoch, err := a.bindOpenedStore()
	if err != nil {
		return nil, err
	}
	return &SelectedStoreLifecycle{store: store, epoch: epoch}, nil
}

func (l *SelectedStoreLifecycle) Activate(process *worklifetime.Process) error {
	if l == nil || l.store == nil || l.epoch == nil {
		return errors.New("dev scratch selected-store lifecycle is incomplete")
	}
	return l.store.Activate(process)
}

func (l *SelectedStoreLifecycle) CloseUnactivated() error {
	if l == nil || l.store == nil || l.epoch == nil {
		return errors.New("dev scratch selected-store lifecycle is incomplete")
	}
	if err := l.store.CloseUnactivated(); err != nil {
		return err
	}
	return l.epoch.releaseAfterStoreClose()
}

func (l *SelectedStoreLifecycle) CloseActivated(receipt *worklifetime.ProcessJoinReceipt) error {
	if l == nil || l.store == nil || l.epoch == nil {
		return errors.New("dev scratch selected-store lifecycle is incomplete")
	}
	if err := l.store.CloseActivated(receipt); err != nil {
		return err
	}
	return l.epoch.releaseAfterStoreClose()
}
