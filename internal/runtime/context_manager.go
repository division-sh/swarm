package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store/runbundle"
)

// RunBundleAvailabilityReader resolves persisted run identity before a request
// is dispatched to a bundle-bound runtime graph.
type RunBundleAvailabilityReader interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type BundleContext struct {
	BundleHash        string
	BundleSourceFact  runtimecorrelation.BundleSourceFact
	BundleIdentity    runtimecontracts.BundleIdentity
	Source            semanticview.Source
	ContractsRoot     string
	PlatformSpecPath  string
	Runtime           *Runtime
	WorkspaceScopeKey string
}

type RuntimeContextState string

const (
	RuntimeContextStateLoaded   RuntimeContextState = "loaded"
	RuntimeContextStateUnloaded RuntimeContextState = "unloaded"

	RuntimeContextCauseNotLoaded   = "runtime_context_not_loaded"
	RuntimeContextCauseUnavailable = "runtime_context_unavailable"
	RuntimeContextCauseUnloaded    = "runtime_context_unloaded"
)

func (c BundleContext) normalized() BundleContext {
	c.BundleHash = strings.TrimSpace(c.BundleHash)
	c.BundleSourceFact = c.BundleSourceFact.Normalized()
	if c.BundleHash == "" {
		c.BundleHash = strings.TrimSpace(c.BundleSourceFact.BundleHash)
	}
	c.ContractsRoot = strings.TrimSpace(c.ContractsRoot)
	c.PlatformSpecPath = strings.TrimSpace(c.PlatformSpecPath)
	c.WorkspaceScopeKey = strings.TrimSpace(c.WorkspaceScopeKey)
	return c
}

type runtimeContextEntry struct {
	context *BundleContext
	state   RuntimeContextState
	cause   string
}

type RuntimeContextLookup struct {
	Context *BundleContext
	State   RuntimeContextState
	Cause   string
	Found   bool
}

func (l RuntimeContextLookup) Loaded() bool {
	return l.Found && l.State == RuntimeContextStateLoaded && l.Context != nil
}

type RuntimeContextDeactivationResult struct {
	BundleHash  string
	State       RuntimeContextState
	Cause       string
	Found       bool
	Changed     bool
	ShutdownErr error
}

type RuntimeContextManager struct {
	mu           sync.RWMutex
	availability RunBundleAvailabilityReader
	contexts     map[string]*runtimeContextEntry
	order        []string
}

func NewRuntimeContextManager(availability RunBundleAvailabilityReader, contexts ...BundleContext) (*RuntimeContextManager, error) {
	manager := &RuntimeContextManager{
		availability: availability,
		contexts:     map[string]*runtimeContextEntry{},
	}
	for _, contextDef := range contexts {
		if err := manager.Register(contextDef); err != nil {
			return nil, err
		}
	}
	return manager, nil
}

func (m *RuntimeContextManager) Register(contextDef BundleContext) error {
	if m == nil {
		return fmt.Errorf("runtime context manager is required")
	}
	contextDef = contextDef.normalized()
	if err := runtimecontracts.ValidateBundleHash(contextDef.BundleHash); err != nil {
		return fmt.Errorf("runtime context bundle_hash: %w", err)
	}
	if contextDef.BundleSourceFact.BundleHash != "" && contextDef.BundleSourceFact.BundleHash != contextDef.BundleHash {
		return fmt.Errorf("runtime context source fact hash %q does not match bundle_hash %q", contextDef.BundleSourceFact.BundleHash, contextDef.BundleHash)
	}
	if contextDef.Source == nil {
		return fmt.Errorf("runtime context %s source is required", contextDef.BundleHash)
	}
	if contextDef.Runtime == nil {
		return fmt.Errorf("runtime context %s runtime is required", contextDef.BundleHash)
	}
	if contextDef.Runtime.Bus == nil {
		return fmt.Errorf("runtime context %s event bus is required", contextDef.BundleHash)
	}
	if contextDef.BundleSourceFact.BundleHash == "" {
		contextDef.BundleSourceFact.BundleHash = contextDef.BundleHash
	}
	contextDef.BundleSourceFact = contextDef.BundleSourceFact.Normalized()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.contexts == nil {
		m.contexts = map[string]*runtimeContextEntry{}
	}
	if _, exists := m.contexts[contextDef.BundleHash]; exists {
		return fmt.Errorf("duplicate runtime context bundle_hash %s", contextDef.BundleHash)
	}
	copied := contextDef
	m.contexts[contextDef.BundleHash] = &runtimeContextEntry{
		context: &copied,
		state:   RuntimeContextStateLoaded,
	}
	m.order = append(m.order, contextDef.BundleHash)
	sort.Strings(m.order)
	return nil
}

func (m *RuntimeContextManager) Len() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.contexts)
}

func (m *RuntimeContextManager) MultiContext() bool {
	return m.Len() > 1
}

func (m *RuntimeContextManager) BundleHashes() []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := append([]string(nil), m.order...)
	sort.Strings(out)
	return out
}

func (m *RuntimeContextManager) Primary() (*BundleContext, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.order) == 0 {
		return nil, false
	}
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if entry == nil || entry.state != RuntimeContextStateLoaded || entry.context == nil {
			continue
		}
		return entry.context, true
	}
	return nil, false
}

func (m *RuntimeContextManager) LookupBundleHash(bundleHash string) (*BundleContext, bool) {
	lookup := m.LookupBundleHashStatus(bundleHash)
	if !lookup.Loaded() {
		return nil, false
	}
	return lookup.Context, true
}

func (m *RuntimeContextManager) LookupBundleHashStatus(bundleHash string) RuntimeContextLookup {
	if m == nil {
		return RuntimeContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}
	}
	bundleHash = strings.TrimSpace(bundleHash)
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry := m.contexts[bundleHash]
	if entry == nil {
		return RuntimeContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}
	}
	state := entry.state
	if state == "" {
		state = RuntimeContextStateLoaded
	}
	cause := strings.TrimSpace(entry.cause)
	if cause == "" && state != RuntimeContextStateLoaded {
		cause = RuntimeContextCauseUnavailable
	}
	lookup := RuntimeContextLookup{
		State: state,
		Cause: cause,
		Found: true,
	}
	if state == RuntimeContextStateLoaded {
		lookup.Context = entry.context
	}
	return lookup
}

func (m *RuntimeContextManager) LookupRun(ctx context.Context, runID string) (*BundleContext, runbundle.Availability, bool, error) {
	lookup, availability, err := m.LookupRunStatus(ctx, runID)
	if err != nil {
		return nil, availability, false, err
	}
	return lookup.Context, availability, lookup.Loaded(), nil
}

func (m *RuntimeContextManager) LookupRunStatus(ctx context.Context, runID string) (RuntimeContextLookup, runbundle.Availability, error) {
	if m == nil {
		return RuntimeContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}, runbundle.Availability{}, nil
	}
	if m.availability == nil {
		return RuntimeContextLookup{}, runbundle.Availability{}, fmt.Errorf("run bundle availability reader is required")
	}
	availability, err := m.availability.LoadRunBundleAvailability(ctx, strings.TrimSpace(runID))
	if err != nil {
		return RuntimeContextLookup{}, runbundle.Availability{}, err
	}
	if strings.TrimSpace(availability.BundleHash) == "" {
		return RuntimeContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}, availability, nil
	}
	return m.LookupBundleHashStatus(availability.BundleHash), availability, nil
}

func (m *RuntimeContextManager) DeactivateBundleHash(bundleHash, cause string) RuntimeContextDeactivationResult {
	result := RuntimeContextDeactivationResult{
		BundleHash: strings.TrimSpace(bundleHash),
		State:      RuntimeContextStateUnloaded,
		Cause:      normalizeRuntimeContextDeactivationCause(cause),
	}
	if m == nil || result.BundleHash == "" {
		return result
	}
	var runtimeToShutdown *Runtime
	m.mu.Lock()
	entry := m.contexts[result.BundleHash]
	if entry == nil {
		m.mu.Unlock()
		return result
	}
	result.Found = true
	state := entry.state
	if state == "" {
		state = RuntimeContextStateLoaded
	}
	if state != RuntimeContextStateLoaded {
		result.State = state
		if strings.TrimSpace(entry.cause) != "" {
			result.Cause = strings.TrimSpace(entry.cause)
		}
		m.mu.Unlock()
		return result
	}
	entry.state = RuntimeContextStateUnloaded
	entry.cause = result.Cause
	result.Changed = true
	if entry.context != nil {
		runtimeToShutdown = entry.context.Runtime
	}
	m.mu.Unlock()
	if runtimeToShutdown != nil {
		result.ShutdownErr = runtimeToShutdown.Shutdown()
	}
	return result
}

func (m *RuntimeContextManager) DeactivateAll(cause string) []RuntimeContextDeactivationResult {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	hashes := append([]string(nil), m.order...)
	m.mu.RUnlock()
	results := make([]RuntimeContextDeactivationResult, 0, len(hashes))
	for _, bundleHash := range hashes {
		results = append(results, m.DeactivateBundleHash(bundleHash, cause))
	}
	return results
}

func normalizeRuntimeContextDeactivationCause(cause string) string {
	if cause = strings.TrimSpace(cause); cause != "" {
		return cause
	}
	return RuntimeContextCauseUnavailable
}
