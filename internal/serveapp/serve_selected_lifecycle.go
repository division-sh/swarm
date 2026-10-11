package serveapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sessionprovider"
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
	store       activatedSelectedStore
	process     *worklifetime.Process
	capability  serveProcessCapability
	selected    selectedForkContextRetirement
	nativeState nativeInvocationRetirement
}

type nativeInvocationRetirement interface {
	Retire(*worklifetime.ProcessJoinReceipt) error
}

func (l *activatedServeLifecycle) SetNativeInvocationState(state *sessionprovider.InvocationState) error {
	if l == nil || state == nil || l.nativeState != nil {
		return errors.New("serve native invocation state must be installed exactly once")
	}
	l.nativeState = state
	return nil
}

type selectedForkContextRetirement interface {
	RetireSelectedContexts(context.Context) error
}

func (l *activatedServeLifecycle) SetSelectedForkSupervisor(supervisor *processLifecycleSupervisor) error {
	if l == nil || supervisor == nil || l.selected != nil {
		return errors.New("serve selected-fork retirement supervisor must be installed exactly once")
	}
	l.selected = supervisor
	return nil
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
	if l.selected != nil {
		if err := l.selected.RetireSelectedContexts(joinCtx); err != nil {
			diagnostics = errors.Join(diagnostics, fmt.Errorf("retire selected-fork contexts: %w", err))
			if err := l.selected.RetireSelectedContexts(context.Background()); err != nil {
				return errors.Join(diagnostics, err)
			}
		}
	}
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
	diagnostics = errors.Join(diagnostics, closeActivatedServeStore(l.store, receipt))
	if l.nativeState != nil {
		diagnostics = errors.Join(diagnostics, l.nativeState.Retire(receipt))
	}
	return diagnostics
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
