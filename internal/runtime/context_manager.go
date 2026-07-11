package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/runbundle"
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
	StandingTargets   []StandingTarget
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
	if len(c.StandingTargets) > 0 {
		targets := make([]StandingTarget, 0, len(c.StandingTargets))
		for _, target := range c.StandingTargets {
			targets = append(targets, target.normalized())
		}
		c.StandingTargets = targets
	}
	return c
}

type runtimeContextEntry struct {
	context *BundleContext
	state   RuntimeContextState
	cause   string
}

type runtimeContextAgentSlugCollision struct {
	agentID  string
	existing BundleContext
	incoming BundleContext
}

type RuntimeContextLookup struct {
	Context *BundleContext
	State   RuntimeContextState
	Cause   string
	Found   bool
}

type RuntimeIngressContextLookup struct {
	Context    *BundleContext
	Target     StandingTarget
	State      RuntimeContextState
	Cause      string
	Found      bool
	AliasFound bool
}

func (l RuntimeIngressContextLookup) Loaded() bool {
	return l.Found && l.State == RuntimeContextStateLoaded && l.Context != nil
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

// ValidateRuntimeContextSet applies the manager's process-global collision
// rules without publishing any context as loaded.
func ValidateRuntimeContextSet(contexts ...BundleContext) error {
	_, err := NewRuntimeContextManager(nil, contexts...)
	return err
}

func (m *RuntimeContextManager) Register(contextDef BundleContext) error {
	if m == nil {
		return fmt.Errorf("runtime context manager is required")
	}
	contextDef, err := validateRuntimeContextDefinition(contextDef)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.contexts == nil {
		m.contexts = map[string]*runtimeContextEntry{}
	}
	if _, exists := m.contexts[contextDef.BundleHash]; exists {
		return fmt.Errorf("duplicate runtime context bundle_hash %s", contextDef.BundleHash)
	}
	if collision, ok := m.duplicateLoadedAgentSlugLocked(contextDef); ok {
		return fmt.Errorf(
			"duplicate runtime context agent_id %q across loaded BundleContexts: existing %s; incoming %s",
			collision.agentID,
			runtimeContextBundleLabel(collision.existing),
			runtimeContextBundleLabel(collision.incoming),
		)
	}
	if existing, incoming, alias, ok := m.duplicateLoadedIngressAliasLocked(contextDef); ok {
		return fmt.Errorf("duplicate standing ingress alias %q across loaded BundleContexts: existing %s; incoming %s; rename one package flow ingress alias", alias, runtimeContextBundleLabel(existing), runtimeContextBundleLabel(incoming))
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

func validateRuntimeContextDefinition(contextDef BundleContext) (BundleContext, error) {
	contextDef = contextDef.normalized()
	if err := runtimecontracts.ValidateBundleHash(contextDef.BundleHash); err != nil {
		return BundleContext{}, fmt.Errorf("runtime context bundle_hash: %w", err)
	}
	if contextDef.BundleSourceFact.BundleHash != "" && contextDef.BundleSourceFact.BundleHash != contextDef.BundleHash {
		return BundleContext{}, fmt.Errorf("runtime context source fact hash %q does not match bundle_hash %q", contextDef.BundleSourceFact.BundleHash, contextDef.BundleHash)
	}
	if contextDef.Source == nil {
		return BundleContext{}, fmt.Errorf("runtime context %s source is required", contextDef.BundleHash)
	}
	if contextDef.Runtime == nil {
		return BundleContext{}, fmt.Errorf("runtime context %s runtime is required", contextDef.BundleHash)
	}
	if contextDef.Runtime.Bus == nil {
		return BundleContext{}, fmt.Errorf("runtime context %s event bus is required", contextDef.BundleHash)
	}
	if err := validateRuntimeContextStandingTargets(contextDef); err != nil {
		return BundleContext{}, err
	}
	if contextDef.BundleSourceFact.BundleHash == "" {
		contextDef.BundleSourceFact.BundleHash = contextDef.BundleHash
	}
	contextDef.BundleSourceFact = contextDef.BundleSourceFact.Normalized()
	return contextDef, nil
}

func validateRuntimeContextStandingTargets(contextDef BundleContext) error {
	seen := map[string]string{}
	for _, target := range contextDef.StandingTargets {
		target = target.normalized()
		if target.BundleHash != contextDef.BundleHash {
			return fmt.Errorf("runtime context %s standing target %q/%q bundle_hash %q does not match context", contextDef.BundleHash, target.Alias, target.Provider, target.BundleHash)
		}
		if target.Alias == "" || target.Provider == "" || target.RunID == "" || target.FlowID == "" || target.FlowInstance == "" || target.EntityID == "" || target.SigningSecret == "" {
			return fmt.Errorf("runtime context %s standing target requires alias, provider, run_id, flow_id, flow_instance, entity_id, and signing_secret", contextDef.BundleHash)
		}
		key := target.Alias + "\x00" + target.Provider
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("runtime context %s duplicate standing target %s and %s for alias %q provider %q", contextDef.BundleHash, previous, target.SourcePath, target.Alias, target.Provider)
		}
		seen[key] = target.SourcePath
	}
	return nil
}

func (m *RuntimeContextManager) duplicateLoadedIngressAliasLocked(incoming BundleContext) (BundleContext, BundleContext, string, bool) {
	incomingAliases := map[string]struct{}{}
	for _, target := range incoming.StandingTargets {
		incomingAliases[target.normalized().Alias] = struct{}{}
	}
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		for _, target := range entry.context.StandingTargets {
			alias := target.normalized().Alias
			if _, ok := incomingAliases[alias]; ok {
				return *entry.context, incoming, alias, true
			}
		}
	}
	return BundleContext{}, BundleContext{}, "", false
}

func (m *RuntimeContextManager) duplicateLoadedAgentSlugLocked(incoming BundleContext) (runtimeContextAgentSlugCollision, bool) {
	incomingIDs := runtimeContextAgentIDs(incoming.Source)
	if len(incomingIDs) == 0 {
		return runtimeContextAgentSlugCollision{}, false
	}
	incomingSet := make(map[string]struct{}, len(incomingIDs))
	for _, agentID := range incomingIDs {
		incomingSet[agentID] = struct{}{}
	}
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		for _, existingAgentID := range runtimeContextAgentIDs(entry.context.Source) {
			if _, ok := incomingSet[existingAgentID]; !ok {
				continue
			}
			return runtimeContextAgentSlugCollision{
				agentID:  existingAgentID,
				existing: *entry.context,
				incoming: incoming,
			}, true
		}
	}
	return runtimeContextAgentSlugCollision{}, false
}

func runtimeContextEntryLoaded(entry *runtimeContextEntry) bool {
	if entry == nil || entry.context == nil {
		return false
	}
	state := entry.state
	if state == "" {
		state = RuntimeContextStateLoaded
	}
	return state == RuntimeContextStateLoaded
}

func runtimeContextAgentIDs(source semanticview.Source) []string {
	if source == nil {
		return nil
	}
	entries := source.AgentEntries()
	ids := make([]string, 0, len(entries))
	for key, entry := range entries {
		agentID := strings.TrimSpace(entry.ID)
		if agentID == "" {
			agentID = strings.TrimSpace(key)
		}
		if agentID == "" {
			continue
		}
		ids = append(ids, agentID)
	}
	sort.Strings(ids)
	return ids
}

func runtimeContextBundleLabel(contextDef BundleContext) string {
	contextDef = contextDef.normalized()
	parts := []string{}
	if contextDef.BundleHash != "" {
		parts = append(parts, "bundle_hash="+contextDef.BundleHash)
	}
	if source := strings.TrimSpace(contextDef.BundleSourceFact.BundleSource); source != "" {
		parts = append(parts, "bundle_source="+source)
	}
	if fingerprint := strings.TrimSpace(contextDef.BundleSourceFact.BundleFingerprint); fingerprint != "" {
		parts = append(parts, "bundle_fingerprint="+fingerprint)
	}
	workflowName := strings.TrimSpace(contextDef.BundleIdentity.WorkflowName)
	workflowVersion := strings.TrimSpace(contextDef.BundleIdentity.WorkflowVersion)
	switch {
	case workflowName != "" && workflowVersion != "":
		parts = append(parts, "workflow="+workflowName+"@"+workflowVersion)
	case workflowName != "":
		parts = append(parts, "workflow="+workflowName)
	}
	if len(parts) == 0 {
		return "bundle_context=<unknown>"
	}
	return strings.Join(parts, " ")
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

func (m *RuntimeContextManager) LoadedContexts() []BundleContext {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]BundleContext, 0, len(m.order))
	for _, hash := range m.order {
		entry := m.contexts[hash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		out = append(out, *entry.context)
	}
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

func (m *RuntimeContextManager) LookupIngress(alias, provider string) RuntimeIngressContextLookup {
	if m == nil {
		return RuntimeIngressContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}
	}
	alias = strings.Trim(strings.TrimSpace(alias), "/")
	provider = strings.TrimSpace(provider)
	m.mu.RLock()
	defer m.mu.RUnlock()
	aliasFound := false
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if entry == nil || entry.context == nil {
			continue
		}
		for _, target := range entry.context.StandingTargets {
			target = target.normalized()
			if target.Alias != alias {
				continue
			}
			aliasFound = true
			state := entry.state
			if state == "" {
				state = RuntimeContextStateLoaded
			}
			cause := strings.TrimSpace(entry.cause)
			if state != RuntimeContextStateLoaded && cause == "" {
				cause = RuntimeContextCauseUnavailable
			}
			out := RuntimeIngressContextLookup{State: state, Cause: cause, AliasFound: true}
			if target.Provider != provider {
				continue
			}
			out.Found = true
			out.Target = target
			if state == RuntimeContextStateLoaded {
				out.Context = entry.context
			}
			return out
		}
	}
	return RuntimeIngressContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded, AliasFound: aliasFound}
}

func (m *RuntimeContextManager) ReplaceSameBundle(contextDef BundleContext) error {
	return m.ReplaceBundleHash(contextDef.BundleHash, contextDef)
}

func (m *RuntimeContextManager) ValidateReplacement(existingHash string, contextDef BundleContext) error {
	if m == nil {
		return fmt.Errorf("runtime context manager is required")
	}
	contextDef, err := validateRuntimeContextDefinition(contextDef)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.validateReplacementLocked(strings.TrimSpace(existingHash), contextDef)
}

func (m *RuntimeContextManager) ReplaceBundleHash(existingHash string, contextDef BundleContext) error {
	if m == nil {
		return fmt.Errorf("runtime context manager is required")
	}
	contextDef, err := validateRuntimeContextDefinition(contextDef)
	if err != nil {
		return err
	}
	existingHash = strings.TrimSpace(existingHash)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.validateReplacementLocked(existingHash, contextDef); err != nil {
		return err
	}
	entry := m.contexts[existingHash]
	if entry.context != nil && entry.context.Runtime != nil {
		entry.context.Runtime.CloseAdmission()
	}
	if existingHash != contextDef.BundleHash {
		delete(m.contexts, existingHash)
		for i, bundleHash := range m.order {
			if bundleHash == existingHash {
				m.order = append(m.order[:i], m.order[i+1:]...)
				break
			}
		}
		entry = &runtimeContextEntry{}
		m.contexts[contextDef.BundleHash] = entry
		m.order = append(m.order, contextDef.BundleHash)
		sort.Strings(m.order)
	}
	copied := contextDef
	entry.context = &copied
	entry.state = RuntimeContextStateLoaded
	entry.cause = ""
	return nil
}

func (m *RuntimeContextManager) validateReplacementLocked(existingHash string, contextDef BundleContext) error {
	entry := m.contexts[existingHash]
	if entry == nil || !runtimeContextEntryLoaded(entry) {
		return fmt.Errorf("loaded runtime context %s is required for replacement", existingHash)
	}
	if existingHash != contextDef.BundleHash {
		if _, exists := m.contexts[contextDef.BundleHash]; exists {
			return fmt.Errorf("replacement runtime context bundle_hash %s is already registered", contextDef.BundleHash)
		}
	}
	if collision, ok := m.duplicateLoadedAgentSlugLockedExcluding(contextDef, existingHash); ok {
		return fmt.Errorf("duplicate runtime context agent_id %q across loaded BundleContexts: existing %s; incoming %s", collision.agentID, runtimeContextBundleLabel(collision.existing), runtimeContextBundleLabel(collision.incoming))
	}
	if existing, incoming, alias, ok := m.duplicateLoadedIngressAliasLockedExcluding(contextDef, existingHash); ok {
		return fmt.Errorf("duplicate standing ingress alias %q across loaded BundleContexts: existing %s; incoming %s; rename one package flow ingress alias", alias, runtimeContextBundleLabel(existing), runtimeContextBundleLabel(incoming))
	}
	return nil
}

func (m *RuntimeContextManager) duplicateLoadedAgentSlugLockedExcluding(incoming BundleContext, excludedHash string) (runtimeContextAgentSlugCollision, bool) {
	incomingSet := map[string]struct{}{}
	for _, agentID := range runtimeContextAgentIDs(incoming.Source) {
		incomingSet[agentID] = struct{}{}
	}
	for _, bundleHash := range m.order {
		if bundleHash == excludedHash {
			continue
		}
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		for _, agentID := range runtimeContextAgentIDs(entry.context.Source) {
			if _, ok := incomingSet[agentID]; ok {
				return runtimeContextAgentSlugCollision{agentID: agentID, existing: *entry.context, incoming: incoming}, true
			}
		}
	}
	return runtimeContextAgentSlugCollision{}, false
}

func (m *RuntimeContextManager) duplicateLoadedIngressAliasLockedExcluding(incoming BundleContext, excludedHash string) (BundleContext, BundleContext, string, bool) {
	incomingAliases := map[string]struct{}{}
	for _, target := range incoming.StandingTargets {
		incomingAliases[target.normalized().Alias] = struct{}{}
	}
	for _, bundleHash := range m.order {
		if bundleHash == excludedHash {
			continue
		}
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		for _, target := range entry.context.StandingTargets {
			alias := target.normalized().Alias
			if _, ok := incomingAliases[alias]; ok {
				return *entry.context, incoming, alias, true
			}
		}
	}
	return BundleContext{}, BundleContext{}, "", false
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
	return m.DeactivateBundleHashWithOptions(bundleHash, cause, DefaultShutdownOptions())
}

func (m *RuntimeContextManager) DeactivateBundleHashWithOptions(bundleHash, cause string, opts ShutdownOptions) RuntimeContextDeactivationResult {
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
		if runtimeToShutdown != nil {
			runtimeToShutdown.CloseAdmission()
		}
	}
	m.mu.Unlock()
	if runtimeToShutdown != nil {
		result.ShutdownErr = runtimeToShutdown.ShutdownWithOptions(opts)
	}
	return result
}

func (m *RuntimeContextManager) DeactivateAll(cause string) []RuntimeContextDeactivationResult {
	return m.DeactivateAllWithOptions(cause, DefaultShutdownOptions())
}

func (m *RuntimeContextManager) DeactivateAllWithOptions(cause string, opts ShutdownOptions) []RuntimeContextDeactivationResult {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	hashes := append([]string(nil), m.order...)
	m.mu.RUnlock()
	results := make([]RuntimeContextDeactivationResult, 0, len(hashes))
	for _, bundleHash := range hashes {
		results = append(results, m.DeactivateBundleHashWithOptions(bundleHash, cause, opts))
	}
	return results
}

func normalizeRuntimeContextDeactivationCause(cause string) string {
	if cause = strings.TrimSpace(cause); cause != "" {
		return cause
	}
	return RuntimeContextCauseUnavailable
}
