package serveapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

const serveCapabilityReleaseRetryDelay = 100 * time.Millisecond

type activatedSelectedStore interface {
	Activate(*worklifetime.Process) error
	CloseActivated(*worklifetime.ProcessJoinReceipt) error
}

type selectedStoreLifecycle interface {
	activatedSelectedStore
	CloseUnactivated() error
}

type serveProcessCapability interface {
	Release(context.Context) error
	TerminalResult() (runtimestartupownership.TerminalResult, bool)
}

type activatedServeLifecycle struct {
	store      activatedSelectedStore
	process    *worklifetime.Process
	capability serveProcessCapability
}

func activateServeLifecycle(store activatedSelectedStore, process *worklifetime.Process) (*activatedServeLifecycle, error) {
	if store == nil || process == nil {
		return nil, errors.New("serve selected-store lifecycle requires store and process owners")
	}
	if err := store.Activate(process); err != nil {
		return nil, err
	}
	return &activatedServeLifecycle{store: store, process: process}, nil
}

func (l *activatedServeLifecycle) SetProcessCapability(capability serveProcessCapability) error {
	if l == nil || capability == nil {
		return errors.New("serve selected-store lifecycle requires a process capability")
	}
	if l.capability != nil {
		return errors.New("serve selected-store lifecycle process capability is already set")
	}
	l.capability = capability
	return nil
}

func (l *activatedServeLifecycle) Finalize(joinCtx context.Context, diagnostics error) error {
	if l == nil || l.store == nil || l.process == nil {
		return errors.Join(diagnostics, errors.New("serve selected-store lifecycle is incomplete"))
	}
	if joinCtx == nil {
		joinCtx = context.Background()
	}
	l.process.Retire()
	receipt, joinErr := l.process.Join(joinCtx)
	if joinErr != nil {
		diagnostics = errors.Join(diagnostics, fmt.Errorf("process work join exceeded shutdown budget: %w", joinErr))
		receipt, joinErr = l.process.Join(context.Background())
	}
	if joinErr != nil {
		return errors.Join(diagnostics, joinErr)
	}

	settled, releaseErr := releaseServeProcessCapability(l.capability)
	diagnostics = errors.Join(diagnostics, releaseErr)
	if !settled {
		return diagnostics
	}
	return errors.Join(diagnostics, closeActivatedServeStore(l.store, receipt))
}

func releaseServeProcessCapability(capability serveProcessCapability) (bool, error) {
	if capability == nil {
		return true, nil
	}
	var releaseErr error
	for {
		err := capability.Release(context.Background())
		if err == nil {
			return true, releaseErr
		}
		releaseErr = errors.Join(releaseErr, fmt.Errorf("release process capability: %w", err))
		if _, terminal := capability.TerminalResult(); terminal {
			return true, releaseErr
		}
		time.Sleep(serveCapabilityReleaseRetryDelay)
	}
}

func closeActivatedServeStore(store activatedSelectedStore, receipt *worklifetime.ProcessJoinReceipt) error {
	var closeErr error
	for {
		err := store.CloseActivated(receipt)
		if err == nil {
			return closeErr
		}
		closeErr = errors.Join(closeErr, fmt.Errorf("close activated selected store: %w", err))
		time.Sleep(serveCapabilityReleaseRetryDelay)
	}
}
