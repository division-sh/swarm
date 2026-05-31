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

type RuntimeContextManager struct {
	mu           sync.RWMutex
	availability RunBundleAvailabilityReader
	contexts     map[string]*BundleContext
	order        []string
}

func NewRuntimeContextManager(availability RunBundleAvailabilityReader, contexts ...BundleContext) (*RuntimeContextManager, error) {
	manager := &RuntimeContextManager{
		availability: availability,
		contexts:     map[string]*BundleContext{},
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
		m.contexts = map[string]*BundleContext{}
	}
	if _, exists := m.contexts[contextDef.BundleHash]; exists {
		return fmt.Errorf("duplicate runtime context bundle_hash %s", contextDef.BundleHash)
	}
	copied := contextDef
	m.contexts[contextDef.BundleHash] = &copied
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
	ctx := m.contexts[m.order[0]]
	if ctx == nil {
		return nil, false
	}
	return ctx, true
}

func (m *RuntimeContextManager) LookupBundleHash(bundleHash string) (*BundleContext, bool) {
	if m == nil {
		return nil, false
	}
	bundleHash = strings.TrimSpace(bundleHash)
	m.mu.RLock()
	defer m.mu.RUnlock()
	ctx := m.contexts[bundleHash]
	if ctx == nil {
		return nil, false
	}
	return ctx, true
}

func (m *RuntimeContextManager) LookupRun(ctx context.Context, runID string) (*BundleContext, runbundle.Availability, bool, error) {
	if m == nil {
		return nil, runbundle.Availability{}, false, nil
	}
	if m.availability == nil {
		return nil, runbundle.Availability{}, false, fmt.Errorf("run bundle availability reader is required")
	}
	availability, err := m.availability.LoadRunBundleAvailability(ctx, strings.TrimSpace(runID))
	if err != nil {
		return nil, runbundle.Availability{}, false, err
	}
	if strings.TrimSpace(availability.BundleHash) == "" {
		return nil, availability, false, nil
	}
	contextDef, ok := m.LookupBundleHash(availability.BundleHash)
	return contextDef, availability, ok, nil
}
