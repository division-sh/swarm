package runtime

import (
	"context"
	"errors"
	"fmt"
	"maps"
)

const RuntimeContextCauseReset = "runtime_context_reset"

// StageResetRuntimeContexts replaces retired execution, not admitted sources.
// Publication exposes metadata for consumer convergence, not execution authority.
func (m *RuntimeContextManager) StageResetRuntimeContexts(contexts ...BundleContext) error {
	return m.installResetRuntimeContexts(false, false, contexts)
}

func (m *RuntimeContextManager) PublishResetRuntimeContexts(contexts ...BundleContext) error {
	return m.installResetRuntimeContexts(true, false, contexts)
}

// StageRecoveredRuntimeContexts installs the admitted reset source set into a
// new process manager. Retained process authority has already excluded and
// settled the predecessor; there are no old process-local entries to replace.
func (m *RuntimeContextManager) StageRecoveredRuntimeContexts(contexts ...BundleContext) error {
	return m.installResetRuntimeContexts(false, true, contexts)
}

// ReleaseResetExecution admits the exact published set after its consumers have
// converged. An old or partial candidate set cannot release a successor's fence.
func (m *RuntimeContextManager) ReleaseResetExecution(contexts ...BundleContext) error {
	if m == nil {
		return errors.New("reset requires runtime context manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.resetExecutionFenced || len(contexts) == 0 || len(contexts) != len(m.contexts) {
		return errors.New("reset execution release requires the complete fenced publication")
	}
	seen := make(map[string]bool, len(contexts))
	for _, candidate := range contexts {
		hash := candidate.BundleHash()
		entry := m.contexts[hash]
		if seen[hash] || !runtimeContextEntryLoaded(entry) || entry.runtime != candidate.Runtime || entry.workOwner != candidate.WorkOwner {
			return fmt.Errorf("reset execution release does not match published source %s", hash)
		}
		seen[hash] = true
	}
	m.resetExecutionFenced = false
	return nil
}

func (m *RuntimeContextManager) installResetRuntimeContexts(publish, recovered bool, contexts []BundleContext) error {
	if m == nil {
		return errors.New("reset requires runtime context manager")
	}
	m.sourceSetMu.Lock()
	defer m.sourceSetMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingSourceSetTransition != nil {
		return errors.New("reset cannot replace an unsettled source-set transition")
	}
	if len(contexts) == 0 || (recovered && len(m.contexts) != 0) || (!recovered && len(contexts) != len(m.contexts)) {
		return errors.New("reset must reconstruct the identical complete source set")
	}
	seen := make(map[string]bool, len(contexts))
	for _, candidate := range contexts {
		hash := candidate.BundleHash()
		old := m.contexts[hash]
		if seen[hash] || (!recovered && (old == nil || runtimeContextEntryLoaded(old))) {
			return fmt.Errorf("reset source %s is duplicate, changed, or still selectable", hash)
		}
		seen[hash] = true
		if candidate.Runtime == nil || candidate.WorkOwner == nil {
			return fmt.Errorf("reset source %s lacks execution ownership", hash)
		}
		if publish {
			if old.cause != RuntimeContextCauseReset || old.runtime != candidate.Runtime || old.workOwner != candidate.WorkOwner {
				return fmt.Errorf("reset source %s does not match staged execution", hash)
			}
		} else if !recovered {
			old.shutdownMu.Lock()
			retired := old.shutdownComplete
			old.shutdownMu.Unlock()
			if !retired || old.runtime == candidate.Runtime || old.workOwner == candidate.WorkOwner {
				return fmt.Errorf("reset source %s requires a joined predecessor and fresh execution", hash)
			}
		}
	}
	prepared := newRuntimeContextManagerState(m.availability)
	prepared.nextPublicationGeneration = m.nextPublicationGeneration
	prepared.suppressedStandingServices = maps.Clone(m.suppressedStandingServices)
	discardPrepared := func() {
		for _, entry := range prepared.contexts {
			for _, occurrence := range entry.standing {
				occurrence.Retire()
				_ = occurrence.Wait(context.Background())
			}
		}
	}
	for _, candidate := range contexts {
		if err := prepared.register(candidate, false); err != nil {
			// Registration owns only newly prepared standing occurrences here;
			// runtime disposal belongs to the reset supervisor on any failure.
			discardPrepared()
			return err
		}
		if publish {
			entry := prepared.contexts[candidate.BundleHash()]
			standing, err := prepared.newStandingOccurrencesLocked(candidate.WorkOwner, entry.context.StandingTargets)
			if err != nil {
				discardPrepared()
				return err
			}
			entry.standing = standing
		}
	}
	if !publish {
		for _, entry := range prepared.contexts {
			entry.state, entry.cause = RuntimeContextStateUnloaded, RuntimeContextCauseReset
		}
		if err := prepared.refreshCapabilitySubjectsLocked(); err != nil {
			return err
		}
	}
	m.contexts, m.order = prepared.contexts, prepared.order
	m.resetExecutionFenced = true
	m.nextPublicationGeneration = prepared.nextPublicationGeneration
	m.suppressedStandingServices = prepared.suppressedStandingServices
	m.setBaseCapabilitySubjectsLocked(prepared.capabilitySubjects)
	if publish {
		for _, candidate := range contexts {
			if candidate.Runtime.Bus != nil {
				candidate.Runtime.Bus.SetStandingRunWorkOwner(m)
			}
		}
	}
	return nil
}
