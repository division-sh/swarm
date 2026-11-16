package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

// PrepareInboundStop holds new publications while accepted target creation joins.
func (m *RuntimeContextManager) PrepareInboundStop() {
	m.inboundPhaseMu.Lock()
	defer m.inboundPhaseMu.Unlock()
	if !m.inboundStopping {
		m.inboundStopping = true
		if !m.inboundRequests.Closed() {
			m.inboundReady = make(chan struct{})
		}
	}
}

// SealInboundExecution joins final-set recovery without holding the phase lock.
// The process must also join pending-intent selection before exposing intake.
func (m *RuntimeContextManager) SealInboundExecution(ctx context.Context) error {
	m.PrepareInboundStop()
	m.inboundPhaseMu.Lock()
	if done := m.inboundSealDone; done != nil {
		m.inboundPhaseMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			m.inboundPhaseMu.RLock()
			defer m.inboundPhaseMu.RUnlock()
			return m.inboundSealErr
		}
	}
	m.inboundSealDone = make(chan struct{})
	m.inboundPhaseMu.Unlock()
	m.CloseFreshDispatch()
	m.mu.RLock()
	var runtimes []*Runtime
	for _, entry := range m.contexts {
		if runtimeContextEntryLoaded(entry) {
			runtimes = append(runtimes, entry.runtime)
		}
	}
	m.mu.RUnlock()
	var result error
	for _, rt := range runtimes {
		result = errors.Join(result, rt.Bus.SealPipelineRecovery(ctx))
	}
	m.inboundPhaseMu.Lock()
	m.inboundSealErr = result
	close(m.inboundSealDone)
	if result != nil {
		m.closeInboundReadyLocked()
	}
	m.inboundPhaseMu.Unlock()
	return result
}

func (m *RuntimeContextManager) OpenStoredInbound() error {
	m.inboundPhaseMu.Lock()
	defer m.inboundPhaseMu.Unlock()
	if !m.inboundStopping || m.inboundSealErr != nil || m.inboundRequests.Closed() {
		return errors.Join(worklifetime.ErrAdmissionFenced, m.inboundSealErr)
	}
	select {
	case <-m.inboundSealDone:
	default:
		return worklifetime.ErrAdmissionFenced
	}
	m.inboundStoreOnly = true
	m.closeInboundReadyLocked()
	return nil
}

func (m *RuntimeContextManager) closeInboundReadyLocked() {
	if m.inboundReady != nil {
		close(m.inboundReady)
		m.inboundReady = nil
	}
}

func (m *RuntimeContextManager) BeginInboundRequest(ctx context.Context) (context.Context, func(), bool) {
	return m.inboundRequests.BeginContext(ctx)
}

func (m *RuntimeContextManager) lockInboundPhase(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.inboundPhaseMu.RLock()
		if m.inboundReady == nil {
			return nil
		}
		ready := m.inboundReady
		m.inboundPhaseMu.RUnlock()
		select {
		case <-ready:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *RuntimeContextManager) CloseInboundRequests(ctx context.Context) error {
	m.inboundPhaseMu.Lock()
	m.inboundStoreOnly = false
	m.inboundRequests.Close()
	m.closeInboundReadyLocked()
	m.inboundPhaseMu.Unlock()
	err := m.inboundRequests.Wait(ctx)
	return errors.Join(err, m.inboundRequests.Wait(context.Background()))
}

func (m *RuntimeContextManager) InboundClaimAdmissionOpen() bool {
	m.inboundPhaseMu.RLock()
	defer m.inboundPhaseMu.RUnlock()
	return !m.inboundStopping
}

// InboundPublicationUse owns publication custody, never fresh execution rights.
// The ordinary case retains the existing execution use; stop intake does not.
type InboundPublicationUse struct {
	runtime  *Runtime
	context  BundleContext
	ctx      context.Context
	executed *RuntimeContextUse
	release  func()
	once     sync.Once
	result   error
}

func (u *InboundPublicationUse) Done() error {
	u.once.Do(func() {
		if u.executed != nil {
			u.result = u.executed.Done()
		}
		if u.release != nil {
			u.release()
		}
	})
	return u.result
}

func (m *RuntimeContextManager) AcquireInboundPublication(ctx context.Context, alias, provider string) (*InboundPublicationUse, RuntimeIngressContextLookup, error) {
	for {
		if err := m.lockInboundPhase(ctx); err != nil {
			return nil, RuntimeIngressContextLookup{}, err
		}
		m.inboundPhaseMu.RUnlock()
		lookup, err := m.loadSelectedIngressLanding(ctx, alias, provider)
		if err != nil {
			return nil, lookup, err
		}
		if !lookup.Loaded() {
			m.liveSelectionMu.RUnlock()
			return nil, lookup, nil
		}
		m.inboundPhaseMu.RLock()
		if m.inboundReady != nil {
			m.inboundPhaseMu.RUnlock()
			m.liveSelectionMu.RUnlock()
			continue
		}
		m.mu.RLock()
		use, err := m.acquireInboundPublicationLocked(ctx, lookup)
		m.mu.RUnlock()
		m.inboundPhaseMu.RUnlock()
		if err != nil || use == nil {
			m.liveSelectionMu.RUnlock()
			return nil, lookup, err
		}
		priorRelease := use.release
		use.release = func() {
			if priorRelease != nil {
				priorRelease()
			}
			m.liveSelectionMu.RUnlock()
		}
		return use, lookup, nil
	}
}

func (m *RuntimeContextManager) acquireInboundPublicationLocked(ctx context.Context, lookup RuntimeIngressContextLookup) (*InboundPublicationUse, error) {
	entry, err := m.ingressEntryLocked(lookup)
	if err != nil {
		return nil, err
	}
	if !m.inboundStopping {
		use, err := m.acquireEntryLocked(ctx, entry)
		if err != nil {
			return nil, err
		}
		use.Context = bindIngressLandingRun(use.Context, lookup.Target)
		return &InboundPublicationUse{runtime: use.runtime, context: use.Context, ctx: use.WorkContext(), executed: use}, nil
	}
	if !m.inboundStoreOnly {
		return nil, errors.Join(worklifetime.ErrAdmissionFenced, m.inboundSealErr)
	}
	definition := *entry.context
	definition.Runtime = entry.runtime
	ctx, err = rootedRuntimeContextUse(ctx, definition)
	if err == nil {
		ctx, err = entry.runtime.mutationContext(ctx)
	}
	if err != nil {
		return nil, err
	}
	ctx, release, admitted := m.inboundRequests.BeginContext(ctx)
	if !admitted {
		return nil, worklifetime.ErrAdmissionFenced
	}
	definition = bindIngressLandingRun(definition, lookup.Target)
	return &InboundPublicationUse{runtime: entry.runtime, context: definition, ctx: ctx, release: release}, nil
}

func (u *InboundPublicationUse) HandleResolvedWebhook(w http.ResponseWriter, request *http.Request, target InboundTarget) {
	if u == nil || u.runtime == nil || u.runtime.InboundGateway == nil {
		http.Error(w, "ingress target is unavailable", http.StatusServiceUnavailable)
		return
	}
	if !ownsInboundTarget(u.context, target) {
		http.Error(w, "ingress target is unavailable", http.StatusServiceUnavailable)
		return
	}
	target.storeOnly = u.executed == nil
	u.runtime.InboundGateway.HandleResolvedWebhook(w, request.WithContext(u.ctx), target, u.context.Source)
}

func (u *InboundPublicationUse) AdmitPublicIngressTarget(target InboundTarget) (InboundTarget, bool, error) {
	return admitPublicIngressTarget(u.runtime, u.context, target)
}
