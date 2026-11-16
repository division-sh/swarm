package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// SealInboundExecution serializes provider selection with recovery selection.
// The process must also join pending-intent selection before exposing intake.
func (m *RuntimeContextManager) SealInboundExecution(ctx context.Context) error {
	m.inboundPhaseMu.Lock()
	defer m.inboundPhaseMu.Unlock()
	if m.inboundStopping {
		return m.inboundSealErr
	}
	m.inboundStopping = true
	m.inboundReady = make(chan struct{})
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
	m.inboundSealErr = result
	return result
}

func (m *RuntimeContextManager) OpenStoredInbound() error {
	m.inboundPhaseMu.Lock()
	defer m.inboundPhaseMu.Unlock()
	if !m.inboundStopping || m.inboundSealErr != nil || m.inboundRequests.Closed() {
		return errors.Join(worklifetime.ErrAdmissionFenced, m.inboundSealErr)
	}
	m.inboundStoreOnly = true
	if m.inboundReady != nil {
		close(m.inboundReady)
		m.inboundReady = nil
	}
	return nil
}

func (m *RuntimeContextManager) BeginInboundRequest(ctx context.Context) (context.Context, func(), bool) {
	if err := m.lockInboundPhase(ctx); err != nil {
		return ctx, nil, false
	}
	defer m.inboundPhaseMu.RUnlock()
	return m.inboundRequests.BeginContext(ctx)
}

func (m *RuntimeContextManager) lockInboundPhase(ctx context.Context) error {
	for {
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
	if m.inboundReady != nil {
		close(m.inboundReady)
		m.inboundReady = nil
	}
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
}

func (u *InboundPublicationUse) Done() (err error) {
	u.once.Do(func() {
		if u.executed != nil {
			err = u.executed.Done()
		}
		if u.release != nil {
			u.release()
		}
	})
	return err
}

func (m *RuntimeContextManager) AcquireInboundPublication(ctx context.Context, alias, provider string) (*InboundPublicationUse, RuntimeIngressContextLookup, error) {
	if err := m.lockInboundPhase(ctx); err != nil {
		return nil, RuntimeIngressContextLookup{}, err
	}
	defer m.inboundPhaseMu.RUnlock()
	if !m.inboundStopping {
		use, lookup, err := m.AcquireIngress(ctx, alias, provider)
		if err != nil || use == nil {
			return nil, lookup, err
		}
		return &InboundPublicationUse{runtime: use.runtime, context: use.Context, ctx: use.WorkContext(), executed: use}, lookup, nil
	}
	if !m.inboundStoreOnly {
		return nil, m.LookupIngress(alias, provider), worklifetime.ErrAdmissionFenced
	}
	m.liveSelectionMu.RLock()
	lookup := m.LookupIngress(alias, provider)
	refuse := func(err error) (*InboundPublicationUse, RuntimeIngressContextLookup, error) {
		m.liveSelectionMu.RUnlock()
		return nil, lookup, err
	}
	if !lookup.Loaded() || lookup.Context == nil {
		return refuse(nil)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry := m.contexts[lookup.Context.BundleHash()]
	if !runtimeContextEntryLoaded(entry) || entry.runtime == nil || entry.context.PublicationGeneration != lookup.Context.PublicationGeneration || m.resetExecutionFenced {
		return refuse(worklifetime.ErrAdmissionFenced)
	}
	if m.landingRuns == nil {
		return refuse(fmt.Errorf("inbound requires the deployment landing-run reader"))
	}
	landing, found, err := m.landingRuns.LoadLandingRun(ctx, runlifecycle.LiveRunScope{FlowPath: lookup.Target.FlowPath})
	if err != nil {
		return refuse(err)
	}
	if !found || !landing.Source.Matches(entry.context.SourceArtifactFact) || (landing.State != runlifecycle.StateLive && landing.State != runlifecycle.StatePaused) {
		return refuse(fmt.Errorf("inbound landing run is unavailable"))
	}
	definition := *entry.context
	definition.Runtime = entry.runtime
	ctx, err = rootedRuntimeContextUse(ctx, definition)
	if err == nil {
		ctx, err = entry.runtime.mutationContext(ctx)
	}
	if err != nil {
		return refuse(err)
	}
	ctx, release, admitted := m.inboundRequests.BeginContext(ctx)
	if !admitted {
		return refuse(worklifetime.ErrAdmissionFenced)
	}
	lookup.Target.RunID = landing.RunID
	definition.StandingTargets = append([]StandingTarget(nil), definition.StandingTargets...)
	for index, target := range definition.StandingTargets {
		if target.Alias == alias && target.Provider == provider {
			definition.StandingTargets[index] = lookup.Target
		}
	}
	return &InboundPublicationUse{runtime: entry.runtime, context: definition, ctx: ctx, release: func() {
		release()
		m.liveSelectionMu.RUnlock()
	}}, lookup, nil
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
