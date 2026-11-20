package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

// InboundRuntimeStop reserves the exact final set through common intake cutoff.
// All other complete runtime stops and generation refreshes use these locks.
type InboundRuntimeStop struct {
	manager    *RuntimeContextManager
	entries    []*inboundRuntimeStopEntry
	ctx        context.Context
	grace      time.Duration
	openOnce   sync.Once
	drainOnce  sync.Once
	finishOnce sync.Once
	result     error
}

type inboundRuntimeStopEntry struct {
	entry        *runtimeContextEntry
	runtime      *Runtime
	completed    bool
	admissionErr error
	result       error
}

func (m *RuntimeContextManager) PrepareInboundRuntimeStop(ctx context.Context, grace time.Duration, additional ...*Runtime) *InboundRuntimeStop {
	stop := &InboundRuntimeStop{manager: m, ctx: ctx, grace: grace}
	stop.result = m.SealInboundExecution(ctx)
	m.sourceSetMu.Lock()
	if m.pendingSourceSetTransition != nil {
		m.pendingSourceSetTransition.terminate()
	}
	m.mu.RLock()
	for _, hash := range m.order {
		if entry := m.contexts[hash]; entry != nil && entry.runtime != nil {
			stop.entries = append(stop.entries, &inboundRuntimeStopEntry{entry: entry, runtime: entry.runtime})
		}
	}
	m.mu.RUnlock()
	m.sourceSetMu.Unlock()
	for _, rt := range additional {
		found := rt == nil
		for _, held := range stop.entries {
			found = found || held.runtime == rt
		}
		if !found {
			rt.CloseFreshDispatch()
			stop.result = errors.Join(stop.result, rt.Bus.SealPipelineRecovery(ctx))
			stop.entries = append(stop.entries, &inboundRuntimeStopEntry{runtime: rt})
		}
	}
	for _, held := range stop.entries {
		rt := held.runtime
		rt.generationMu.Lock()
		rt.lifecycleMu.Lock()
		held.completed = rt.replacementQuiesced && rt.startupGrant == nil
		rt.lifecycleMu.Unlock()
		if rt.Manager != nil {
			held.admissionErr = rt.Manager.CheckShutdownAdmission()
			stop.result = errors.Join(stop.result, held.admissionErr)
		}
		m.mu.Lock()
		if runtimeContextEntryLoaded(held.entry) && m.contexts[held.entry.context.BundleHash()] == held.entry && !held.completed && held.admissionErr == nil {
			m.inboundStopObservation = stop
			held.entry.stopObservation = stop
		}
		rt.CloseAdmission()
		m.mu.Unlock()
	}
	return stop
}

func (s *InboundRuntimeStop) Open(prior error) error {
	s.openOnce.Do(func() {
		s.result = errors.Join(s.result, prior)
		if s.result == nil {
			if !s.completedCutoff() {
				s.result = s.manager.OpenStoredInbound()
			}
		} else {
			s.manager.inboundPhaseMu.Lock()
			s.manager.inboundSealErr = s.result
			s.manager.closeInboundReadyLocked()
			s.manager.inboundPhaseMu.Unlock()
		}
	})
	return s.result
}

func (s *InboundRuntimeStop) completedCutoff() bool {
	m := s.manager
	m.inboundPhaseMu.RLock()
	defer m.inboundPhaseMu.RUnlock()
	if !m.inboundStopping || !m.inboundFinalSet || m.inboundSealErr != nil || m.inboundStoreOnly {
		return false
	}
	select {
	case <-m.inboundSealDone:
		return m.inboundRequests.closedAndDrained()
	default:
		return false
	}
}

func (s *InboundRuntimeStop) Drain() {
	s.drainOnce.Do(func() {
		for _, held := range s.entries {
			if !held.completed && held.admissionErr == nil {
				held.result = held.runtime.drainStopLocked(s.ctx, s.grace)
			}
		}
	})
}

func (s *InboundRuntimeStop) Finish() error {
	if !s.manager.inboundRequests.closedAndDrained() {
		return worklifetime.ErrAdmissionFenced
	}
	s.Drain()
	s.finishOnce.Do(func() {
		for _, held := range s.entries {
			rt := held.runtime
			if held.admissionErr == nil {
				if !held.completed {
					held.result = rt.finishStopLocked(s.ctx, held.result)
				} else {
					held.result = rt.stopResult
				}
				if held.entry != nil {
					result := RuntimeContextDeactivationResult{Cause: RuntimeContextCauseUnavailable}
					s.manager.finishGracefulContextVisibility(held.entry, &result)
					held.result = errors.Join(held.result, result.ShutdownErr)
				}
			}
			s.result = errors.Join(s.result, held.result)
			rt.generationMu.Unlock()
		}
	})
	return s.result
}
