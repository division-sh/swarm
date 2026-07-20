package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
	WorkOwner         *worklifetime.RuntimeOccurrence
	WorkspaceScopeKey string
	StandingTargets   []StandingTarget
}

type RuntimeContextState string

const (
	RuntimeContextStateLoaded   RuntimeContextState = "loaded"
	RuntimeContextStateUnloaded RuntimeContextState = "unloaded"

	RuntimeContextCauseNotLoaded          = "runtime_context_not_loaded"
	RuntimeContextCauseUnavailable        = "runtime_context_unavailable"
	RuntimeContextCauseUnloaded           = "runtime_context_unloaded"
	RuntimeContextCauseReplacing          = "runtime_context_replacing"
	RuntimeContextCauseStandingSuppressed = "standing_service_suppressed"
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
	context          *BundleContext
	runtime          *Runtime
	workOwner        *worklifetime.RuntimeOccurrence
	standing         map[string]*worklifetime.StandingOccurrence
	state            RuntimeContextState
	cause            string
	shutdownMu       sync.Mutex
	shutdownComplete bool
}

// RuntimeContextUse is the only execution-bearing result produced by the
// runtime selector. Metadata lookups never expose a raw Runtime pointer.
type RuntimeContextUse struct {
	Context BundleContext
	runtime *Runtime
	lease   *worklifetime.Lease
	leases  []*worklifetime.Lease
	once    sync.Once
}

func (u *RuntimeContextUse) Runtime() *Runtime {
	if u == nil {
		return nil
	}
	return u.runtime
}

func (u *RuntimeContextUse) WorkContext() context.Context {
	if u == nil {
		return context.Background()
	}
	if len(u.leases) > 0 {
		return u.leases[len(u.leases)-1].Context()
	}
	if u.lease == nil {
		return context.Background()
	}
	return u.lease.Context()
}

func (u *RuntimeContextUse) Done() error {
	if u == nil {
		return nil
	}
	var err error
	u.once.Do(func() {
		leases := u.leases
		if len(leases) == 0 && u.lease != nil {
			leases = []*worklifetime.Lease{u.lease}
		}
		for i := len(leases) - 1; i >= 0; i-- {
			err = errors.Join(err, leases[i].Done())
		}
	})
	return err
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
	mu                         sync.RWMutex
	availability               RunBundleAvailabilityReader
	contexts                   map[string]*runtimeContextEntry
	order                      []string
	admissionGeneration        string
	installedTriggerSubjects   []packs.Subject
	capabilitySubjects         []packs.Subject
	suppressedStandingServices map[string]struct{}
}

type ProcessAdmissionState struct {
	GenerationID      string
	InstalledSubjects []packs.Subject
}

func NewRuntimeContextManager(availability RunBundleAvailabilityReader, contexts ...BundleContext) (*RuntimeContextManager, error) {
	return newRuntimeContextManager(availability, ProcessAdmissionState{}, contexts...)
}

func NewRuntimeContextManagerWithAdmission(availability RunBundleAvailabilityReader, state ProcessAdmissionState, contexts ...BundleContext) (*RuntimeContextManager, error) {
	return newRuntimeContextManager(availability, state, contexts...)
}

func newRuntimeContextManagerState(availability RunBundleAvailabilityReader, state ProcessAdmissionState) (*RuntimeContextManager, error) {
	installed, err := packs.NormalizeSubjects(state.InstalledSubjects)
	if err != nil {
		return nil, fmt.Errorf("normalize installed provider trigger subjects: %w", err)
	}
	return &RuntimeContextManager{
		availability:               availability,
		contexts:                   map[string]*runtimeContextEntry{},
		admissionGeneration:        strings.TrimSpace(state.GenerationID),
		installedTriggerSubjects:   installed,
		suppressedStandingServices: map[string]struct{}{},
	}, nil
}

func newRuntimeContextManager(availability RunBundleAvailabilityReader, state ProcessAdmissionState, contexts ...BundleContext) (*RuntimeContextManager, error) {
	manager, err := newRuntimeContextManagerState(availability, state)
	if err != nil {
		return nil, err
	}
	for _, contextDef := range contexts {
		if err := manager.Register(contextDef); err != nil {
			return nil, errors.Join(err, manager.quiesceConstructionFailure())
		}
	}
	if err := manager.refreshCapabilitySubjectsLocked(); err != nil {
		return nil, errors.Join(err, manager.quiesceConstructionFailure())
	}
	return manager, nil
}

func (m *RuntimeContextManager) quiesceConstructionFailure() error {
	if m == nil {
		return nil
	}
	var cleanupErr error
	for _, result := range m.DeactivateAll(RuntimeContextCauseUnavailable) {
		if result.ShutdownErr != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("shutdown partially registered runtime context %s: %w", result.BundleHash, result.ShutdownErr))
		}
	}
	return cleanupErr
}

// ValidateRuntimeContextSet applies the manager's process-global collision
// rules without publishing any context as loaded.
func ValidateRuntimeContextSet(contexts ...BundleContext) error {
	return ValidateRuntimeContextSetWithAdmission(ProcessAdmissionState{}, contexts...)
}

func ValidateRuntimeContextSetWithAdmission(state ProcessAdmissionState, contexts ...BundleContext) error {
	manager, err := newRuntimeContextManagerState(nil, state)
	if err != nil {
		return err
	}
	for _, contextDef := range contexts {
		if err := manager.register(contextDef, false); err != nil {
			return err
		}
	}
	return manager.refreshCapabilitySubjectsLocked()
}

func (m *RuntimeContextManager) Register(contextDef BundleContext) error {
	return m.register(contextDef, true)
}

func (m *RuntimeContextManager) register(contextDef BundleContext, activateOccurrences bool) error {
	if m == nil {
		return fmt.Errorf("runtime context manager is required")
	}
	contextDef, err := validateRuntimeContextDefinition(contextDef)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.validateAdmissionGenerationLocked(contextDef); err != nil {
		return err
	}
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
	runtimeOwner := copied.Runtime
	workOwner := copied.WorkOwner
	copied.Runtime = nil
	copied.WorkOwner = nil
	var standing map[string]*worklifetime.StandingOccurrence
	if activateOccurrences {
		standing, err = m.newStandingOccurrencesLocked(workOwner, copied.StandingTargets)
		if err != nil {
			return err
		}
	}
	m.contexts[contextDef.BundleHash] = &runtimeContextEntry{
		context:   &copied,
		runtime:   runtimeOwner,
		workOwner: workOwner,
		standing:  standing,
		state:     RuntimeContextStateLoaded,
	}
	m.order = append(m.order, contextDef.BundleHash)
	sort.Strings(m.order)
	if err := m.refreshCapabilitySubjectsLocked(); err != nil {
		delete(m.contexts, contextDef.BundleHash)
		for i, bundleHash := range m.order {
			if bundleHash == contextDef.BundleHash {
				m.order = append(m.order[:i], m.order[i+1:]...)
				break
			}
		}
		var retireErr error
		for serviceID, occurrence := range standing {
			if occurrence != nil {
				if occurrenceErr := occurrence.RetireAndWait(context.Background()); occurrenceErr != nil {
					retireErr = errors.Join(retireErr, fmt.Errorf("retire standing occurrence %s after registration failure: %w", serviceID, occurrenceErr))
				}
			}
		}
		return errors.Join(err, retireErr)
	}
	return nil
}

func (m *RuntimeContextManager) newStandingOccurrencesLocked(workOwner *worklifetime.RuntimeOccurrence, targets []StandingTarget) (map[string]*worklifetime.StandingOccurrence, error) {
	out := map[string]*worklifetime.StandingOccurrence{}
	if workOwner == nil {
		return nil, errors.New("runtime occurrence is required")
	}
	for _, raw := range targets {
		target := raw.normalized()
		if m.standingServiceSuppressedLocked(target.ServiceID) {
			continue
		}
		if _, exists := out[target.ServiceID]; exists {
			continue
		}
		if target.Generation <= 0 {
			return nil, fmt.Errorf("standing service %s has invalid durable generation %d", target.ServiceID, target.Generation)
		}
		occurrence, err := workOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{
			ServiceID: target.ServiceID, RunID: target.RunID, Generation: uint64(target.Generation),
		})
		if err != nil {
			for _, created := range out {
				_ = created.RetireAndWait(context.Background())
			}
			return nil, fmt.Errorf("create standing process occurrence: %w", err)
		}
		out[target.ServiceID] = occurrence
	}
	return out, nil
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
	if contextDef.WorkOwner == nil {
		return BundleContext{}, fmt.Errorf("runtime context %s work owner is required", contextDef.BundleHash)
	}
	if ownerHash := strings.TrimSpace(contextDef.WorkOwner.Identity().BundleHash); ownerHash != contextDef.BundleHash {
		return BundleContext{}, fmt.Errorf("runtime context %s work owner belongs to bundle %s", contextDef.BundleHash, ownerHash)
	}
	if runtimeOwner := contextDef.Runtime.WorkOccurrence(); runtimeOwner != nil && runtimeOwner != contextDef.WorkOwner {
		return BundleContext{}, fmt.Errorf("runtime context %s work owner does not belong to runtime", contextDef.BundleHash)
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
		if target.Alias == "" || target.Provider == "" || target.RunID == "" || target.Generation <= 0 || target.FlowID == "" || target.FlowInstance == "" || target.EntityID == "" || !target.AdmissionPlan.Valid() {
			return fmt.Errorf("runtime context %s standing target requires alias, provider, run_id, flow_id, flow_instance, entity_id, and compiled admission plan", contextDef.BundleHash)
		}
		if target.AdmissionPlan.RequiresSecret() != (target.SigningSecret != "") {
			return fmt.Errorf("runtime context %s standing target %q/%q signing_secret presence contradicts compiled %s request authentication", contextDef.BundleHash, target.Alias, target.Provider, target.AdmissionPlan.RequestAuthentication())
		}
		key := target.Alias + "\x00" + target.Provider
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("runtime context %s duplicate standing target %s and %s for alias %q provider %q", contextDef.BundleHash, previous, target.SourcePath, target.Alias, target.Provider)
		}
		seen[key] = target.SourcePath
	}
	return nil
}

func (m *RuntimeContextManager) validateAdmissionGenerationLocked(contextDef BundleContext) error {
	for _, target := range contextDef.StandingTargets {
		generation := target.AdmissionPlan.GenerationID()
		if m.admissionGeneration == "" {
			return fmt.Errorf("runtime context %s standing target %q/%q requires process admission catalog generation", contextDef.BundleHash, target.Alias, target.Provider)
		}
		if generation != m.admissionGeneration {
			return fmt.Errorf("runtime context %s standing target %q/%q admission generation %q does not match process generation %q", contextDef.BundleHash, target.Alias, target.Provider, generation, m.admissionGeneration)
		}
	}
	return nil
}

func (m *RuntimeContextManager) refreshCapabilitySubjectsLocked() error {
	subjects := append([]packs.Subject(nil), m.installedTriggerSubjects...)
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		for _, target := range entry.context.StandingTargets {
			if m.standingServiceSuppressedLocked(target.ServiceID) {
				continue
			}
			subject, err := target.CapabilitySubject()
			if err != nil {
				return fmt.Errorf("derive standing ingress capability subject: %w", err)
			}
			subjects = append(subjects, subject)
		}
	}
	normalized, err := packs.NormalizeSubjects(subjects)
	if err != nil {
		return fmt.Errorf("normalize process provider capability subjects: %w", err)
	}
	m.capabilitySubjects = normalized
	return nil
}

func (m *RuntimeContextManager) AdmissionState() ProcessAdmissionState {
	if m == nil {
		return ProcessAdmissionState{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return ProcessAdmissionState{GenerationID: m.admissionGeneration, InstalledSubjects: packs.CloneSubjects(m.installedTriggerSubjects)}
}

func (m *RuntimeContextManager) CapabilitySubjects() []packs.Subject {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return packs.CloneSubjects(m.capabilitySubjects)
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

func (m *RuntimeContextManager) acquireEntryLocked(ctx context.Context, entry *runtimeContextEntry) (*RuntimeContextUse, error) {
	if !runtimeContextEntryLoaded(entry) || entry.runtime == nil || entry.workOwner == nil {
		return nil, fmt.Errorf("runtime context is unavailable")
	}
	owner := entry.workOwner
	ctx = worklifetime.WithOccurrence(ctx, owner)
	lease, err := owner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit runtime context use: %w", err)
	}
	return &RuntimeContextUse{Context: *entry.context, runtime: entry.runtime, lease: lease}, nil
}

func (m *RuntimeContextManager) AcquireBundleHash(ctx context.Context, bundleHash string) (*RuntimeContextUse, RuntimeContextLookup, error) {
	if m == nil {
		return nil, RuntimeContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}, nil
	}
	bundleHash = strings.TrimSpace(bundleHash)
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry := m.contexts[bundleHash]
	lookup := runtimeContextLookupForEntry(entry)
	if !lookup.Loaded() {
		return nil, lookup, nil
	}
	use, err := m.acquireEntryLocked(ctx, entry)
	return use, lookup, err
}

// AcquireStandingService selects the one loaded runtime that declares the
// service without acquiring its potentially fenced standing occurrence.
// Lifecycle operations use this to create or retire that child occurrence.
func (m *RuntimeContextManager) AcquireStandingService(ctx context.Context, serviceID string) (*RuntimeContextUse, StandingTarget, error) {
	if m == nil {
		return nil, StandingTarget{}, errors.New("runtime context manager is required")
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return nil, StandingTarget{}, errors.New("standing service_id is required")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var selected *runtimeContextEntry
	var selectedTarget StandingTarget
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if entry == nil || entry.context == nil {
			continue
		}
		for _, target := range entry.context.StandingTargets {
			target = target.normalized()
			if target.ServiceID != serviceID {
				continue
			}
			if selected != nil && selected != entry {
				return nil, StandingTarget{}, fmt.Errorf("standing service %s has more than one runtime owner", serviceID)
			}
			selected, selectedTarget = entry, target
		}
	}
	if selected == nil {
		return nil, StandingTarget{}, &runtimepipeline.StandingServiceError{ServiceID: serviceID, Err: runtimepipeline.ErrStandingServiceNotFound}
	}
	if !runtimeContextEntryLoaded(selected) {
		return nil, selectedTarget, fmt.Errorf("standing service %s runtime context is unavailable", serviceID)
	}
	use, err := m.acquireEntryLocked(ctx, selected)
	return use, selectedTarget, err
}

func runtimeContextLookupForEntry(entry *runtimeContextEntry) RuntimeContextLookup {
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
	lookup := RuntimeContextLookup{State: state, Cause: cause, Found: true}
	if state == RuntimeContextStateLoaded {
		lookup.Context = entry.context
	}
	return lookup
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
	return runtimeContextLookupForEntry(entry)
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
			if target.Provider != provider {
				continue
			}
			if m.standingServiceSuppressedLocked(target.ServiceID) {
				return RuntimeIngressContextLookup{
					Target: target, State: RuntimeContextStateUnloaded,
					Cause: RuntimeContextCauseStandingSuppressed, Found: true, AliasFound: true,
				}
			}
			state := entry.state
			if state == "" {
				state = RuntimeContextStateLoaded
			}
			cause := strings.TrimSpace(entry.cause)
			if state != RuntimeContextStateLoaded && cause == "" {
				cause = RuntimeContextCauseUnavailable
			}
			out := RuntimeIngressContextLookup{State: state, Cause: cause, AliasFound: true}
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

func (m *RuntimeContextManager) standingServiceSuppressedLocked(serviceID string) bool {
	if m == nil || strings.TrimSpace(serviceID) == "" {
		return false
	}
	_, suppressed := m.suppressedStandingServices[strings.TrimSpace(serviceID)]
	return suppressed
}

// SuppressStandingServiceTargets withdraws process ingress before a lifecycle
// transition drains admitted work. Declaration targets remain in the context
// so alias collision authority and rollback/resume publication are preserved.
func (m *RuntimeContextManager) SuppressStandingServiceTargets(serviceID string) error {
	if m == nil {
		return nil
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return fmt.Errorf("standing service_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var fenced []*worklifetime.StandingOccurrence
	for _, entry := range m.contexts {
		if entry == nil || entry.standing == nil {
			continue
		}
		if occurrence := entry.standing[serviceID]; occurrence != nil {
			if err := occurrence.Fence(); err != nil {
				for _, prior := range fenced {
					_ = prior.Reopen()
				}
				return fmt.Errorf("fence standing service %s occurrence: %w", serviceID, err)
			}
			fenced = append(fenced, occurrence)
		}
	}
	if m.suppressedStandingServices == nil {
		m.suppressedStandingServices = map[string]struct{}{}
	}
	_, alreadySuppressed := m.suppressedStandingServices[serviceID]
	m.suppressedStandingServices[serviceID] = struct{}{}
	if err := m.refreshCapabilitySubjectsLocked(); err != nil {
		if !alreadySuppressed {
			delete(m.suppressedStandingServices, serviceID)
		}
		for _, occurrence := range fenced {
			_ = occurrence.Reopen()
		}
		return err
	}
	return nil
}

// RestoreStandingServiceTargets rolls back process-only suppression when the
// corresponding durable lifecycle transition did not commit.
func (m *RuntimeContextManager) RestoreStandingServiceTargets(serviceID string) error {
	if m == nil {
		return nil
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return fmt.Errorf("standing service_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, suppressed := m.suppressedStandingServices[serviceID]; !suppressed {
		return nil
	}
	delete(m.suppressedStandingServices, serviceID)
	if err := m.refreshCapabilitySubjectsLocked(); err != nil {
		m.suppressedStandingServices[serviceID] = struct{}{}
		return err
	}
	for _, entry := range m.contexts {
		if entry != nil && entry.standing != nil && entry.standing[serviceID] != nil {
			if err := entry.standing[serviceID].Reopen(); err != nil {
				m.suppressedStandingServices[serviceID] = struct{}{}
				_ = m.refreshCapabilitySubjectsLocked()
				return fmt.Errorf("reopen standing service %s occurrence: %w", serviceID, err)
			}
		}
	}
	return nil
}

func (m *RuntimeContextManager) WaitStandingServiceOccurrence(ctx context.Context, serviceID string) error {
	if m == nil {
		return nil
	}
	serviceID = strings.TrimSpace(serviceID)
	m.mu.RLock()
	var occurrences []*worklifetime.StandingOccurrence
	for _, entry := range m.contexts {
		if entry != nil && entry.standing != nil && entry.standing[serviceID] != nil {
			occurrences = append(occurrences, entry.standing[serviceID])
		}
	}
	m.mu.RUnlock()
	if len(occurrences) == 0 {
		return fmt.Errorf("standing service %s has no process occurrence", serviceID)
	}
	for _, occurrence := range occurrences {
		if err := occurrence.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *RuntimeContextManager) RetireStandingServiceOccurrence(ctx context.Context, serviceID string) error {
	if m == nil {
		return nil
	}
	serviceID = strings.TrimSpace(serviceID)
	m.mu.Lock()
	var occurrences []*worklifetime.StandingOccurrence
	for _, entry := range m.contexts {
		if entry != nil && entry.standing != nil && entry.standing[serviceID] != nil {
			occurrences = append(occurrences, entry.standing[serviceID])
			delete(entry.standing, serviceID)
		}
	}
	m.mu.Unlock()
	for _, occurrence := range occurrences {
		if err := occurrence.RetireAndWait(ctx); err != nil {
			return err
		}
	}
	return nil
}

// PublishStandingServiceTargets replaces stale run/generation/publication
// facts from committed reconciliation and makes that service visible.
func (m *RuntimeContextManager) PublishStandingServiceTargets(serviceID string, targets []StandingTarget) error {
	if m == nil {
		return nil
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return fmt.Errorf("standing service_id is required")
	}
	byBundleAndKey := map[string]StandingTarget{}
	for _, raw := range targets {
		target := raw.normalized()
		if target.ServiceID != serviceID {
			return fmt.Errorf("standing target service_id %s does not match %s", target.ServiceID, serviceID)
		}
		key := target.BundleHash + "\x00" + target.Alias + "\x00" + target.Provider
		if _, duplicate := byBundleAndKey[key]; duplicate {
			return fmt.Errorf("duplicate standing target publication for %s/%s", target.Alias, target.Provider)
		}
		byBundleAndKey[key] = target
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	replaced := 0
	planned := map[string]*BundleContext{}
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if entry == nil || entry.context == nil {
			continue
		}
		copied := *entry.context
		copied.StandingTargets = append([]StandingTarget(nil), entry.context.StandingTargets...)
		changed := false
		for i, existing := range copied.StandingTargets {
			if strings.TrimSpace(existing.ServiceID) != serviceID {
				continue
			}
			key := bundleHash + "\x00" + existing.normalized().Alias + "\x00" + existing.normalized().Provider
			published, ok := byBundleAndKey[key]
			if !ok {
				return fmt.Errorf("committed standing target publication omitted %s/%s", existing.Alias, existing.Provider)
			}
			copied.StandingTargets[i] = published
			delete(byBundleAndKey, key)
			replaced++
			changed = true
		}
		if changed {
			planned[bundleHash] = &copied
		}
	}
	if len(byBundleAndKey) != 0 {
		return fmt.Errorf("committed standing target publication has no loaded declaration owner")
	}
	if replaced == 0 && len(targets) > 0 {
		return fmt.Errorf("standing service %s has no loaded target owner", serviceID)
	}
	var newOccurrence *worklifetime.StandingOccurrence
	var occurrenceEntry *runtimeContextEntry
	for bundleHash, contextDef := range planned {
		entry := m.contexts[bundleHash]
		if entry == nil || entry.workOwner == nil || len(contextDef.StandingTargets) == 0 {
			continue
		}
		if entry.standing != nil && entry.standing[serviceID] != nil {
			return fmt.Errorf("standing service %s still owns an unretired process occurrence", serviceID)
		}
		for _, target := range contextDef.StandingTargets {
			if target.ServiceID != serviceID {
				continue
			}
			created, err := entry.workOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{
				ServiceID: serviceID, RunID: target.RunID, Generation: uint64(target.Generation),
			})
			if err != nil {
				return fmt.Errorf("publish fresh standing process occurrence: %w", err)
			}
			newOccurrence, occurrenceEntry = created, entry
			break
		}
	}
	oldContexts := map[string]*BundleContext{}
	for bundleHash, contextDef := range planned {
		oldContexts[bundleHash] = m.contexts[bundleHash].context
		m.contexts[bundleHash].context = contextDef
	}
	_, wasSuppressed := m.suppressedStandingServices[serviceID]
	delete(m.suppressedStandingServices, serviceID)
	if err := m.refreshCapabilitySubjectsLocked(); err != nil {
		for bundleHash, contextDef := range oldContexts {
			m.contexts[bundleHash].context = contextDef
		}
		if wasSuppressed {
			m.suppressedStandingServices[serviceID] = struct{}{}
		}
		if newOccurrence != nil {
			_ = newOccurrence.RetireAndWait(context.Background())
		}
		return err
	}
	if newOccurrence != nil {
		if occurrenceEntry.standing == nil {
			occurrenceEntry.standing = map[string]*worklifetime.StandingOccurrence{}
		}
		occurrenceEntry.standing[serviceID] = newOccurrence
	}
	return nil
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

// BeginBundleHashReplacement atomically withdraws the predecessor context
// after validating the candidate. The caller must publish either a started
// candidate or a freshly restored predecessor before the context is loaded
// again.
func (m *RuntimeContextManager) BeginBundleHashReplacement(ctx context.Context, existingHash string, contextDef BundleContext) (BundleContext, error) {
	if m == nil {
		return BundleContext{}, fmt.Errorf("runtime context manager is required")
	}
	contextDef, err := validateRuntimeContextDefinition(contextDef)
	if err != nil {
		return BundleContext{}, err
	}
	existingHash = strings.TrimSpace(existingHash)
	m.mu.Lock()
	if err := m.validateReplacementLocked(existingHash, contextDef); err != nil {
		m.mu.Unlock()
		return BundleContext{}, err
	}
	entry := m.contexts[existingHash]
	predecessor := *entry.context
	var standing []*worklifetime.StandingOccurrence
	for _, occurrence := range entry.standing {
		if err := occurrence.Fence(); err != nil {
			for _, prior := range standing {
				_ = prior.Reopen()
			}
			m.mu.Unlock()
			return BundleContext{}, fmt.Errorf("fence predecessor standing occurrence: %w", err)
		}
		standing = append(standing, occurrence)
	}
	if entry.workOwner != nil {
		if err := entry.workOwner.Fence(); err != nil {
			for _, prior := range standing {
				_ = prior.Reopen()
			}
			m.mu.Unlock()
			return BundleContext{}, fmt.Errorf("fence predecessor runtime occurrence: %w", err)
		}
	}
	entry.state = RuntimeContextStateUnloaded
	entry.cause = RuntimeContextCauseReplacing
	m.mu.Unlock()
	for _, occurrence := range standing {
		if err := occurrence.Wait(ctx); err != nil {
			for _, prior := range standing {
				_ = prior.Reopen()
			}
			if entry.workOwner != nil {
				_ = entry.workOwner.Reopen()
			}
			m.mu.Lock()
			entry.state, entry.cause = RuntimeContextStateLoaded, ""
			m.mu.Unlock()
			return BundleContext{}, fmt.Errorf("drain predecessor standing occurrence: %w", err)
		}
	}
	for _, occurrence := range standing {
		if err := occurrence.RetireAndWait(ctx); err != nil {
			return BundleContext{}, fmt.Errorf("retire predecessor standing occurrence: %w", err)
		}
	}
	m.mu.Lock()
	entry.standing = nil
	m.mu.Unlock()
	return predecessor, nil
}

// PublishBundleHashReplacement publishes a replacement that carries no
// admission targets. Admission-bearing reloads use the process-wide API.
func (m *RuntimeContextManager) PublishBundleHashReplacement(existingHash string, contextDef BundleContext) error {
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
	entry := m.contexts[existingHash]
	if entry == nil || entry.state != RuntimeContextStateUnloaded || entry.cause != RuntimeContextCauseReplacing {
		return fmt.Errorf("runtime context %s is not unavailable for replacement", existingHash)
	}
	if m.admissionGeneration != "" && (len(entry.context.StandingTargets) > 0 || len(contextDef.StandingTargets) > 0) {
		return fmt.Errorf("runtime context %s carries compiled admission targets; publish through PublishBundleHashReplacementWithAdmission", existingHash)
	}
	return m.publishBundleHashReplacementLocked(existingHash, contextDef, entry)
}

// PublishRestoredBundleHashReplacement restores the withdrawn predecessor
// against the already-published catalog generation after candidate failure.
func (m *RuntimeContextManager) PublishRestoredBundleHashReplacement(existingHash string, contextDef BundleContext) error {
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
	entry := m.contexts[existingHash]
	if entry == nil || entry.state != RuntimeContextStateUnloaded || entry.cause != RuntimeContextCauseReplacing || entry.context == nil {
		return fmt.Errorf("runtime context %s is not unavailable for predecessor restoration", existingHash)
	}
	if err := validateTargetsGeneration(contextDef, m.admissionGeneration); err != nil {
		return err
	}
	if err := validateRestoredAdmissionAuthority(*entry.context, contextDef); err != nil {
		return err
	}
	subjects, err := m.replacementCapabilitySubjectsLocked(existingHash, contextDef)
	if err != nil {
		return err
	}
	if err := m.publishBundleHashReplacementLocked(existingHash, contextDef, entry); err != nil {
		return err
	}
	m.capabilitySubjects = subjects
	return nil
}

func (m *RuntimeContextManager) publishBundleHashReplacementLocked(existingHash string, contextDef BundleContext, entry *runtimeContextEntry) error {
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
	runtimeOwner := copied.Runtime
	workOwner := copied.WorkOwner
	copied.Runtime = nil
	copied.WorkOwner = nil
	standing, err := m.newStandingOccurrencesLocked(workOwner, copied.StandingTargets)
	if err != nil {
		return err
	}
	entry.context = &copied
	entry.runtime = runtimeOwner
	entry.workOwner = workOwner
	entry.standing = standing
	entry.state = RuntimeContextStateLoaded
	entry.cause = ""
	return nil
}

func validateRestoredAdmissionAuthority(predecessor, restored BundleContext) error {
	if len(predecessor.StandingTargets) != len(restored.StandingTargets) {
		return fmt.Errorf("restored runtime context %s changed standing admission target count from %d to %d", predecessor.BundleHash, len(predecessor.StandingTargets), len(restored.StandingTargets))
	}
	want := map[string]StandingTarget{}
	for _, target := range predecessor.StandingTargets {
		target = target.normalized()
		want[target.Alias+"\x00"+target.Provider] = target
	}
	for _, target := range restored.StandingTargets {
		target = target.normalized()
		previous, ok := want[target.Alias+"\x00"+target.Provider]
		if !ok || previous.SigningSecret != target.SigningSecret ||
			previous.AdmissionPlan.GenerationID() != target.AdmissionPlan.GenerationID() ||
			previous.AdmissionPlan.PolicySource() != target.AdmissionPlan.PolicySource() ||
			previous.AdmissionPlan.RequestAuthentication() != target.AdmissionPlan.RequestAuthentication() {
			return fmt.Errorf("restored runtime context %s changed standing admission authority for %q/%q", predecessor.BundleHash, target.Alias, target.Provider)
		}
		delete(want, target.Alias+"\x00"+target.Provider)
	}
	if len(want) != 0 {
		return fmt.Errorf("restored runtime context %s omitted predecessor standing admission targets", predecessor.BundleHash)
	}
	return nil
}

func (m *RuntimeContextManager) replacementCapabilitySubjectsLocked(existingHash string, contextDef BundleContext) ([]packs.Subject, error) {
	subjects := packs.CloneSubjects(m.installedTriggerSubjects)
	for _, bundleHash := range m.order {
		if bundleHash == existingHash {
			continue
		}
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		for _, target := range entry.context.StandingTargets {
			subject, err := target.CapabilitySubject()
			if err != nil {
				return nil, err
			}
			subjects = append(subjects, subject)
		}
	}
	for _, target := range contextDef.StandingTargets {
		subject, err := target.CapabilitySubject()
		if err != nil {
			return nil, err
		}
		subjects = append(subjects, subject)
	}
	return packs.NormalizeSubjects(subjects)
}

func (m *RuntimeContextManager) ValidateProcessAdmissionReplacement(existingHash string, contextDef BundleContext, survivingTargets map[string][]StandingTarget, state ProcessAdmissionState) error {
	if m == nil {
		return fmt.Errorf("runtime context manager is required")
	}
	contextDef, err := validateRuntimeContextDefinition(contextDef)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.validateReplacementLocked(strings.TrimSpace(existingHash), contextDef); err != nil {
		return err
	}
	_, _, err = m.validateProcessAdmissionCandidateLocked(strings.TrimSpace(existingHash), contextDef, survivingTargets, state)
	return err
}

// PublishBundleHashReplacementWithAdmission is the sole authority transition
// for a runtime replacement and its process-global provider-trigger catalog.
func (m *RuntimeContextManager) PublishBundleHashReplacementWithAdmission(existingHash string, contextDef BundleContext, survivingTargets map[string][]StandingTarget, state ProcessAdmissionState) error {
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
	entry := m.contexts[existingHash]
	if entry == nil || entry.state != RuntimeContextStateUnloaded || entry.cause != RuntimeContextCauseReplacing {
		return fmt.Errorf("runtime context %s is not unavailable for replacement", existingHash)
	}
	installed, subjects, err := m.validateProcessAdmissionCandidateLocked(existingHash, contextDef, survivingTargets, state)
	if err != nil {
		return err
	}
	if existingHash != contextDef.BundleHash {
		if _, exists := m.contexts[contextDef.BundleHash]; exists {
			return fmt.Errorf("replacement runtime context bundle_hash %s is already registered", contextDef.BundleHash)
		}
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
	for bundleHash, targets := range survivingTargets {
		if surviving := m.contexts[strings.TrimSpace(bundleHash)]; surviving != nil && runtimeContextEntryLoaded(surviving) {
			copied := *surviving.context
			copied.StandingTargets = append([]StandingTarget(nil), targets...)
			surviving.context = &copied
		}
	}
	copied := contextDef
	entry.context = &copied
	entry.state = RuntimeContextStateLoaded
	entry.cause = ""
	m.admissionGeneration = strings.TrimSpace(state.GenerationID)
	m.installedTriggerSubjects = installed
	m.capabilitySubjects = subjects
	return nil
}

func (m *RuntimeContextManager) validateProcessAdmissionCandidateLocked(existingHash string, contextDef BundleContext, survivingTargets map[string][]StandingTarget, state ProcessAdmissionState) ([]packs.Subject, []packs.Subject, error) {
	generation := strings.TrimSpace(state.GenerationID)
	if generation == "" {
		return nil, nil, fmt.Errorf("candidate provider-trigger catalog generation is required")
	}
	installed, err := packs.NormalizeSubjects(state.InstalledSubjects)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize candidate installed provider trigger subjects: %w", err)
	}
	contexts := make([]BundleContext, 0, len(m.contexts))
	seenUpdates := map[string]struct{}{}
	for _, bundleHash := range m.order {
		if bundleHash == existingHash {
			continue
		}
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		targets, ok := survivingTargets[bundleHash]
		if !ok {
			return nil, nil, fmt.Errorf("candidate provider-trigger catalog generation %q did not recompile loaded runtime context %s", generation, bundleHash)
		}
		seenUpdates[bundleHash] = struct{}{}
		copied := *entry.context
		copied.StandingTargets = append([]StandingTarget(nil), targets...)
		if err := validateRuntimeContextStandingTargets(copied); err != nil {
			return nil, nil, err
		}
		if err := validateTargetsGeneration(copied, generation); err != nil {
			return nil, nil, err
		}
		contexts = append(contexts, copied)
	}
	for bundleHash := range survivingTargets {
		if _, ok := seenUpdates[strings.TrimSpace(bundleHash)]; !ok {
			return nil, nil, fmt.Errorf("candidate provider-trigger target update names non-surviving runtime context %s", bundleHash)
		}
	}
	if err := validateTargetsGeneration(contextDef, generation); err != nil {
		return nil, nil, err
	}
	contexts = append(contexts, contextDef)
	if err := validateContextSetCollisions(contexts); err != nil {
		return nil, nil, err
	}
	subjects := append([]packs.Subject(nil), installed...)
	for _, candidateContext := range contexts {
		for _, target := range candidateContext.StandingTargets {
			subject, err := target.CapabilitySubject()
			if err != nil {
				return nil, nil, fmt.Errorf("derive candidate standing ingress capability subject: %w", err)
			}
			subjects = append(subjects, subject)
		}
	}
	normalized, err := packs.NormalizeSubjects(subjects)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize candidate process provider capability subjects: %w", err)
	}
	return installed, normalized, nil
}

func validateTargetsGeneration(contextDef BundleContext, generation string) error {
	for _, target := range contextDef.StandingTargets {
		if target.AdmissionPlan.GenerationID() != generation {
			return fmt.Errorf("runtime context %s standing target %q/%q admission generation %q does not match candidate process generation %q", contextDef.BundleHash, target.Alias, target.Provider, target.AdmissionPlan.GenerationID(), generation)
		}
	}
	return nil
}

func validateContextSetCollisions(contexts []BundleContext) error {
	seenAlias := map[string]string{}
	for _, contextDef := range contexts {
		for _, target := range contextDef.StandingTargets {
			if previous, ok := seenAlias[target.Alias]; ok {
				return fmt.Errorf("duplicate standing ingress alias %q across loaded BundleContexts: existing %s; incoming %s; rename one package flow ingress alias", target.Alias, previous, contextDef.BundleHash)
			}
			seenAlias[target.Alias] = contextDef.BundleHash
		}
	}
	return nil
}

func (m *RuntimeContextManager) ReplaceBundleHash(existingHash string, contextDef BundleContext) error {
	if _, err := m.BeginBundleHashReplacement(context.Background(), existingHash, contextDef); err != nil {
		return err
	}
	return m.PublishBundleHashReplacement(existingHash, contextDef)
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

func (m *RuntimeContextManager) AcquireRun(ctx context.Context, runID string) (*RuntimeContextUse, RuntimeContextLookup, runbundle.Availability, error) {
	if m == nil {
		lookup := RuntimeContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}
		return nil, lookup, runbundle.Availability{}, nil
	}
	if m.availability == nil {
		return nil, RuntimeContextLookup{}, runbundle.Availability{}, fmt.Errorf("run bundle availability reader is required")
	}
	availability, err := m.availability.LoadRunBundleAvailability(ctx, strings.TrimSpace(runID))
	if err != nil {
		return nil, RuntimeContextLookup{}, runbundle.Availability{}, err
	}
	if strings.TrimSpace(availability.BundleHash) == "" {
		lookup := RuntimeContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}
		return nil, lookup, availability, nil
	}
	use, lookup, err := m.AcquireBundleHash(ctx, availability.BundleHash)
	return use, lookup, availability, err
}

func (m *RuntimeContextManager) AcquireIngress(ctx context.Context, alias, provider string) (*RuntimeContextUse, RuntimeIngressContextLookup, error) {
	if m == nil {
		return nil, RuntimeIngressContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}, nil
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
		for _, raw := range entry.context.StandingTargets {
			target := raw.normalized()
			if target.Alias != alias {
				continue
			}
			aliasFound = true
			if target.Provider != provider {
				continue
			}
			lookup := RuntimeIngressContextLookup{Target: target, Found: true, AliasFound: true, State: entry.state, Cause: entry.cause}
			if lookup.State == "" {
				lookup.State = RuntimeContextStateLoaded
			}
			if m.standingServiceSuppressedLocked(target.ServiceID) {
				lookup.State = RuntimeContextStateUnloaded
				lookup.Cause = RuntimeContextCauseStandingSuppressed
				return nil, lookup, nil
			}
			if lookup.State != RuntimeContextStateLoaded {
				if strings.TrimSpace(lookup.Cause) == "" {
					lookup.Cause = RuntimeContextCauseUnavailable
				}
				return nil, lookup, nil
			}
			lookup.Context = entry.context
			use, err := m.acquireEntryLocked(ctx, entry)
			if err != nil {
				return nil, lookup, err
			}
			standing := entry.standing[target.ServiceID]
			if standing == nil {
				_ = use.Done()
				return nil, lookup, fmt.Errorf("standing service %s has no process occurrence", target.ServiceID)
			}
			standingLease, err := standing.Begin(use.WorkContext())
			if err != nil {
				_ = use.Done()
				return nil, lookup, fmt.Errorf("admit standing process occurrence: %w", err)
			}
			use.leases = []*worklifetime.Lease{use.lease, standingLease}
			use.lease = nil
			return use, lookup, nil
		}
	}
	return nil, RuntimeIngressContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded, AliasFound: aliasFound}, nil
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
	var (
		entry             *runtimeContextEntry
		runtimeToShutdown *Runtime
		standingToRetire  []*worklifetime.StandingOccurrence
	)
	m.mu.Lock()
	entry = m.contexts[result.BundleHash]
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
	} else {
		entry.state = RuntimeContextStateUnloaded
		entry.cause = result.Cause
		result.Changed = true
		if entry.context != nil {
			runtimeToShutdown = entry.runtime
			for _, occurrence := range entry.standing {
				occurrence.Retire()
				standingToRetire = append(standingToRetire, occurrence)
			}
			entry.standing = nil
			if runtimeToShutdown != nil {
				runtimeToShutdown.CloseAdmission()
			}
		}
	}
	if runtimeToShutdown == nil && entry.context != nil {
		runtimeToShutdown = entry.runtime
	}
	m.mu.Unlock()
	for _, occurrence := range standingToRetire {
		if err := occurrence.RetireAndWait(context.Background()); err != nil {
			result.ShutdownErr = errors.Join(result.ShutdownErr, fmt.Errorf("retire standing process occurrence: %w", err))
		}
	}
	if runtimeToShutdown == nil {
		return result
	}
	entry.shutdownMu.Lock()
	defer entry.shutdownMu.Unlock()
	if entry.shutdownComplete {
		return result
	}
	result.ShutdownErr = errors.Join(result.ShutdownErr, runtimeToShutdown.ShutdownWithOptions(opts))
	if result.ShutdownErr == nil {
		entry.shutdownComplete = true
	}
	return result
}

func (m *RuntimeContextManager) QuiesceBundleRuntime(_ context.Context, bundleHash string) error {
	result := m.DeactivateBundleHash(bundleHash, RuntimeContextCauseUnloaded)
	if result.ShutdownErr != nil {
		return fmt.Errorf("shutdown runtime context for bundle %s: %w", strings.TrimSpace(bundleHash), result.ShutdownErr)
	}
	return nil
}

func (m *RuntimeContextManager) QuiesceAllRuntimeContexts(_ context.Context) error {
	var quiesceErr error
	for _, result := range m.DeactivateAll(RuntimeContextCauseUnloaded) {
		if result.ShutdownErr != nil {
			quiesceErr = errors.Join(quiesceErr, fmt.Errorf("shutdown runtime context for bundle %s: %w", result.BundleHash, result.ShutdownErr))
		}
	}
	return quiesceErr
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
