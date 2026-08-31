package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/google/uuid"
)

// RunBundleAvailabilityReader resolves persisted run identity before a request
// is dispatched to a bundle-bound runtime graph.
type RunBundleAvailabilityReader interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type BundleContext struct {
	SourceArtifactFact          runtimecorrelation.SourceArtifactFact
	BundleIdentity              runtimecontracts.BundleIdentity
	RuntimeInstanceID           string
	PublicationGeneration       uint64
	Source                      semanticview.Source
	PlatformSpecPath            string
	Runtime                     *Runtime
	WorkOwner                   *worklifetime.RuntimeOccurrence
	WorkspaceScopeKey           string
	StandingTargets             []StandingTarget
	ProviderTriggerGeneration   triggergeneration.Generation
	InstalledTriggerSubjects    []packs.Subject
	PackInventoryDigest         string
	ChannelPlans                []packs.SatisfactionPlan
	DeclaredChannelPublication  channelonboarding.ChannelActivationPublication
	ChannelActivationGeneration channelonboarding.ChannelActivationGeneration
	EffectiveSourceIdentity     scenarioexecution.EffectiveSourceIdentity
	ScenarioProfileCatalog      *scenarioexecution.Catalog
}

type RuntimeContextState string

const (
	RuntimeContextStateLoaded   RuntimeContextState = "loaded"
	RuntimeContextStateUnloaded RuntimeContextState = "unloaded"

	RuntimeContextCauseNotLoaded           = "runtime_context_not_loaded"
	RuntimeContextCauseUnavailable         = "runtime_context_unavailable"
	RuntimeContextCauseUnloaded            = "runtime_context_unloaded"
	RuntimeContextCauseSourceSetTransition = "runtime_context_source_set_transition"
	RuntimeContextCauseStandingSuppressed  = "standing_service_suppressed"
)

func (c BundleContext) normalized() BundleContext {
	c.RuntimeInstanceID = strings.TrimSpace(c.RuntimeInstanceID)
	c.PlatformSpecPath = strings.TrimSpace(c.PlatformSpecPath)
	c.WorkspaceScopeKey = strings.TrimSpace(c.WorkspaceScopeKey)
	c.PackInventoryDigest = strings.TrimSpace(c.PackInventoryDigest)
	c.InstalledTriggerSubjects = packs.CloneSubjects(c.InstalledTriggerSubjects)
	c.ChannelPlans = append([]packs.SatisfactionPlan(nil), c.ChannelPlans...)
	if len(c.StandingTargets) > 0 {
		targets := make([]StandingTarget, 0, len(c.StandingTargets))
		for _, target := range c.StandingTargets {
			targets = append(targets, target.normalized())
		}
		c.StandingTargets = targets
	}
	return c
}

func (c BundleContext) BundleHash() string {
	return c.SourceArtifactFact.BundleHash()
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

type runtimeSourceSetTransitionEntry struct {
	bundleHash string
	entry      *runtimeContextEntry
	prepared   *PreparedSourceSetGenerationRefresh
	fresh      bool
}

type runtimeSourceSetTransitionAdmission struct {
	mu           sync.RWMutex
	id           string
	revision     string
	predecessors map[string]runtimemanager.ProcessExecutionBinding
	done         chan struct{}
	completed    bool
}

func newRuntimeSourceSetTransitionAdmission(revision string) (*runtimeSourceSetTransitionAdmission, error) {
	revision = strings.TrimSpace(revision)
	if revision == "" {
		return nil, errors.New("runtime source-set transition revision is required")
	}
	return &runtimeSourceSetTransitionAdmission{
		id: uuid.NewString(), revision: revision,
		predecessors: make(map[string]runtimemanager.ProcessExecutionBinding), done: make(chan struct{}),
	}, nil
}

func (a *runtimeSourceSetTransitionAdmission) TransitionID() string {
	if a == nil {
		return ""
	}
	return a.id
}

func (a *runtimeSourceSetTransitionAdmission) SourceSetRevision() string {
	if a == nil {
		return ""
	}
	return a.revision
}

func (a *runtimeSourceSetTransitionAdmission) RecordPredecessorProcessBinding(binding runtimemanager.ProcessExecutionBinding) error {
	if a == nil {
		return errors.New("runtime source-set transition admission is required")
	}
	if err := binding.Validate(); err != nil {
		return fmt.Errorf("runtime source-set predecessor binding: %w", err)
	}
	key := sourceSetTransitionRuntimeKey(binding)
	a.mu.Lock()
	defer a.mu.Unlock()
	if previous, exists := a.predecessors[key]; exists {
		if !previous.Equal(binding) {
			return fmt.Errorf("runtime source-set predecessor binding changed for %s", key)
		}
		return nil
	}
	a.predecessors[key] = binding
	return nil
}

func (a *runtimeSourceSetTransitionAdmission) PredecessorProcessBinding(current runtimemanager.ProcessExecutionBinding) (runtimemanager.ProcessExecutionBinding, bool) {
	if a == nil {
		return runtimemanager.ProcessExecutionBinding{}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	binding, ok := a.predecessors[sourceSetTransitionRuntimeKey(current)]
	return binding, ok
}

func sourceSetTransitionRuntimeKey(binding runtimemanager.ProcessExecutionBinding) string {
	return strings.Join([]string{
		strings.TrimSpace(binding.BundleHash),
		strings.TrimSpace(binding.RuntimeInstanceID),
	}, "\x00")
}

func (a *runtimeSourceSetTransitionAdmission) Done() <-chan struct{} {
	if a == nil {
		return nil
	}
	return a.done
}

func (a *runtimeSourceSetTransitionAdmission) complete() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.completed {
		a.completed = true
		close(a.done)
	}
}

// PreparedRuntimeSourceSetTransition owns the temporary admission fence for
// every surviving loaded runtime while process composition changes the global
// complete source set.
type PreparedRuntimeSourceSetTransition struct {
	mu               sync.Mutex
	manager          *RuntimeContextManager
	entries          []runtimeSourceSetTransitionEntry
	admission        *runtimeSourceSetTransitionAdmission
	createdAdmission bool
	done             bool
	locked           bool
}

// PreparedStandingServicePublication owns one fresh, unselectable standing
// occurrence while canonical activation and timer producers build its local
// projections. Publish installs the exact occurrence and targets atomically.
type PreparedStandingServicePublication struct {
	mu         sync.Mutex
	manager    *RuntimeContextManager
	serviceID  string
	entry      *runtimeContextEntry
	identity   worklifetime.StandingIdentity
	occurrence *worklifetime.StandingOccurrence
	published  bool
	discarded  bool
}

func (p *PreparedStandingServicePublication) WorkContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || p.occurrence == nil {
		return ctx
	}
	return worklifetime.WithOccurrence(ctx, p.occurrence)
}

func (p *PreparedStandingServicePublication) Publish(targets []StandingTarget) error {
	if p == nil {
		return errors.New("prepared standing service publication is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.published {
		return nil
	}
	if p.discarded || p.manager == nil || p.occurrence == nil {
		return errors.New("prepared standing service publication is no longer active")
	}
	if err := p.manager.publishStandingServiceTargets(p.serviceID, targets, p); err != nil {
		return err
	}
	p.published = true
	return nil
}

func (p *PreparedStandingServicePublication) Discard() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.published {
		return errors.New("published standing service occurrence cannot be discarded")
	}
	if p.discarded {
		return nil
	}
	p.discarded = true
	return p.occurrence.RetireAndWait(context.Background())
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
	sourceSetMu                sync.Mutex
	availability               RunBundleAvailabilityReader
	contexts                   map[string]*runtimeContextEntry
	order                      []string
	primaryBundleHash          string
	capabilitySubjects         []packs.Subject
	capabilityRevision         uint64
	nextPublicationGeneration  uint64
	suppressedStandingServices map[string]struct{}
	pendingSourceSetTransition *runtimeSourceSetTransitionAdmission
}

// RuntimeContextPublicationSnapshot is the current public identity of the
// manager-owned loaded context set. PrimaryBundle identifies the semantic
// primary slot; SourceArtifactFacts contains every currently selectable source.
type RuntimeContextPublicationSnapshot struct {
	PrimaryBundle       runtimecontracts.BundleIdentity
	SourceArtifactFacts []runtimecorrelation.SourceArtifactFact
}

type runtimeContextVisibilityUpdate struct {
	entry *runtimeContextEntry
	state RuntimeContextState
	cause string
}

func NewRuntimeContextManager(availability RunBundleAvailabilityReader, contexts ...BundleContext) (*RuntimeContextManager, error) {
	return newRuntimeContextManager(availability, contexts...)
}

func newRuntimeContextManagerState(availability RunBundleAvailabilityReader) *RuntimeContextManager {
	return &RuntimeContextManager{
		availability:               availability,
		contexts:                   map[string]*runtimeContextEntry{},
		suppressedStandingServices: map[string]struct{}{},
	}
}

func newRuntimeContextManager(availability RunBundleAvailabilityReader, contexts ...BundleContext) (*RuntimeContextManager, error) {
	manager := newRuntimeContextManagerState(availability)
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

// ValidateRuntimeContextSet applies the manager's cross-bundle collision
// rules without publishing any context as loaded.
func ValidateRuntimeContextSet(contexts ...BundleContext) error {
	manager := newRuntimeContextManagerState(nil)
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
	if m.contexts == nil {
		m.contexts = map[string]*runtimeContextEntry{}
	}
	if _, exists := m.contexts[contextDef.BundleHash()]; exists {
		return fmt.Errorf("duplicate runtime context bundle_hash %s", contextDef.BundleHash())
	}
	collision, duplicateAgent, err := m.duplicateLoadedAgentSlugLocked(contextDef)
	if err != nil {
		return fmt.Errorf("resolve runtime context declared agent names: %w", err)
	}
	if duplicateAgent {
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
	m.nextPublicationGeneration++
	contextDef.PublicationGeneration = m.nextPublicationGeneration
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
	m.contexts[contextDef.BundleHash()] = &runtimeContextEntry{
		context:   &copied,
		runtime:   runtimeOwner,
		workOwner: workOwner,
		standing:  standing,
		state:     RuntimeContextStateUnloaded,
		cause:     RuntimeContextCauseNotLoaded,
	}
	m.order = append(m.order, contextDef.BundleHash())
	sort.Strings(m.order)
	if err := m.publishRuntimeContextVisibilityLocked(runtimeContextVisibilityUpdate{
		entry: m.contexts[contextDef.BundleHash()], state: RuntimeContextStateLoaded,
	}); err != nil {
		delete(m.contexts, contextDef.BundleHash())
		for i, bundleHash := range m.order {
			if bundleHash == contextDef.BundleHash() {
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
	if activateOccurrences && runtimeOwner != nil && runtimeOwner.Bus != nil {
		runtimeOwner.Bus.SetStandingRunWorkOwner(m)
	}
	if m.primaryBundleHash == "" {
		m.primaryBundleHash = contextDef.BundleHash()
	}
	return nil
}

func (m *RuntimeContextManager) BeginStandingRunRecovery(
	ctx context.Context,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) (*worklifetime.Lease, error) {
	if m == nil {
		return nil, errors.New("runtime context manager is required")
	}
	runID = strings.TrimSpace(runID)
	if origin.Kind() != runtimerunlifecycle.OriginStandingGeneration {
		return nil, fmt.Errorf("standing recovery requires standing_generation origin, got %s", origin.Kind())
	}
	if err := origin.Validate(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var selected *worklifetime.StandingOccurrence
	for _, entry := range m.contexts {
		if !runtimeContextEntryLoaded(entry) || entry.standing == nil {
			continue
		}
		occurrence := entry.standing[origin.ServiceID()]
		if occurrence == nil {
			continue
		}
		identity := occurrence.Identity()
		if identity.RunID != runID || identity.Generation != uint64(origin.Generation()) {
			return nil, fmt.Errorf(
				"standing recovery origin %s/%d run %s conflicts with loaded occurrence run %s generation %d",
				origin.ServiceID(), origin.Generation(), runID, identity.RunID, identity.Generation,
			)
		}
		if selected != nil && selected != occurrence {
			return nil, fmt.Errorf("standing recovery run %s has more than one process-local owner", runID)
		}
		selected = occurrence
	}
	if selected == nil {
		return nil, fmt.Errorf(
			"standing recovery origin %s/%d run %s has no loaded process-local owner",
			origin.ServiceID(), origin.Generation(), runID,
		)
	}
	return selected.Begin(ctx)
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
	if err := contextDef.SourceArtifactFact.Validate(); err != nil {
		return BundleContext{}, fmt.Errorf("runtime context bundle source fact: %w", err)
	}
	bundleHash := contextDef.BundleHash()
	if contextDef.Source == nil {
		return BundleContext{}, fmt.Errorf("runtime context %s source is required", bundleHash)
	}
	if contextDef.Runtime == nil {
		return BundleContext{}, fmt.Errorf("runtime context %s runtime is required", bundleHash)
	}
	runtimeInstanceID := strings.TrimSpace(contextDef.Runtime.Options.RuntimeInstanceID)
	if contextDef.RuntimeInstanceID != "" && contextDef.RuntimeInstanceID != runtimeInstanceID {
		return BundleContext{}, fmt.Errorf("runtime context %s runtime instance does not belong to runtime", bundleHash)
	}
	contextDef.RuntimeInstanceID = runtimeInstanceID
	runtimeIdentity := contextDef.Runtime.EffectiveSourceIdentity
	if runtimeIdentity.Validate() == nil {
		if err := contextDef.EffectiveSourceIdentity.Validate(); err == nil && !contextDef.EffectiveSourceIdentity.Equal(runtimeIdentity) {
			return BundleContext{}, fmt.Errorf("runtime context %s effective source identity does not belong to runtime", bundleHash)
		}
		contextDef.EffectiveSourceIdentity = runtimeIdentity
		if contextDef.Runtime.ScenarioProfileCatalog == nil {
			return BundleContext{}, fmt.Errorf("runtime context %s scenario profile catalog is required", bundleHash)
		}
		if contextDef.ScenarioProfileCatalog != nil && !contextDef.ScenarioProfileCatalog.EffectiveSourceIdentity().Equal(runtimeIdentity) {
			return BundleContext{}, fmt.Errorf("runtime context %s scenario profile catalog does not belong to runtime", bundleHash)
		}
		contextDef.ScenarioProfileCatalog = contextDef.Runtime.ScenarioProfileCatalog
	}
	if contextDef.Runtime.Bus == nil {
		return BundleContext{}, fmt.Errorf("runtime context %s event bus is required", bundleHash)
	}
	if !equalRuntimeContextSlice(contextDef.ChannelPlans, contextDef.Runtime.Options.ChannelPlans) {
		return BundleContext{}, fmt.Errorf("runtime context %s channel plans do not belong to runtime", bundleHash)
	}
	runtimeDeclaredPublication := contextDef.Runtime.Options.DeclaredChannelPublication
	if !runtimeDeclaredPublication.Generation().Valid() {
		emptyPublication, emptyErr := channelonboarding.NewDeclaredOnlyChannelActivationPublication(nil)
		if emptyErr != nil {
			return BundleContext{}, fmt.Errorf("runtime context %s empty declared-only channel publication: %w", bundleHash, emptyErr)
		}
		runtimeDeclaredPublication = emptyPublication
	}
	if !contextDef.DeclaredChannelPublication.Generation().Valid() {
		contextDef.DeclaredChannelPublication = runtimeDeclaredPublication
	}
	if err := contextDef.DeclaredChannelPublication.Validate(); err != nil {
		return BundleContext{}, fmt.Errorf("runtime context %s declared-only channel publication is invalid: %w", bundleHash, err)
	}
	if contextDef.DeclaredChannelPublication.Mode() != channelonboarding.ChannelActivationPublicationDeclaredOnly {
		return BundleContext{}, fmt.Errorf("runtime context %s channel declaration publication must use declared-only mode", bundleHash)
	}
	if !contextDef.DeclaredChannelPublication.Generation().Equal(runtimeDeclaredPublication.Generation()) {
		return BundleContext{}, fmt.Errorf("runtime context %s declared-only channel publication does not belong to runtime", bundleHash)
	}
	if contextDef.WorkOwner == nil {
		return BundleContext{}, fmt.Errorf("runtime context %s work owner is required", bundleHash)
	}
	if ownerHash := strings.TrimSpace(contextDef.WorkOwner.Identity().BundleHash); ownerHash != bundleHash {
		return BundleContext{}, fmt.Errorf("runtime context %s work owner belongs to bundle %s", bundleHash, ownerHash)
	}
	if runtimeOwner := contextDef.Runtime.WorkOccurrence(); runtimeOwner != nil && runtimeOwner != contextDef.WorkOwner {
		return BundleContext{}, fmt.Errorf("runtime context %s work owner does not belong to runtime", bundleHash)
	}
	if err := validateRuntimeContextStandingTargets(contextDef); err != nil {
		return BundleContext{}, err
	}
	normalizedSubjects, err := packs.NormalizeSubjects(contextDef.InstalledTriggerSubjects)
	if err != nil {
		return BundleContext{}, fmt.Errorf("runtime context %s installed provider trigger subjects: %w", bundleHash, err)
	}
	contextDef.InstalledTriggerSubjects = normalizedSubjects
	if bundle, ok := semanticview.Bundle(contextDef.Source); ok && bundle != nil && bundle.PackInventory != nil {
		inventoryDigest := bundle.PackInventory.Digest()
		if contextDef.PackInventoryDigest == "" || contextDef.PackInventoryDigest != inventoryDigest {
			return BundleContext{}, fmt.Errorf("runtime context %s pack inventory digest %q does not match bundle inventory %q", bundleHash, contextDef.PackInventoryDigest, inventoryDigest)
		}
		projection, err := packadmission.FromBundle(bundle)
		if err != nil {
			return BundleContext{}, fmt.Errorf("runtime context %s load admitted pack projection: %w", bundleHash, err)
		}
		catalog := projection.ProviderTriggers
		if !contextDef.ProviderTriggerGeneration.Equal(catalog.Generation()) {
			return BundleContext{}, fmt.Errorf("runtime context %s provider-trigger generation %q does not match bundle inventory generation %q", bundleHash, contextDef.ProviderTriggerGeneration.Diagnostic(), catalog.Generation().Diagnostic())
		}
		expectedSubjects, err := catalog.InstalledCapabilitySubjects()
		if err != nil {
			return BundleContext{}, fmt.Errorf("runtime context %s derive installed provider-trigger subjects: %w", bundleHash, err)
		}
		expectedSubjects, err = packs.NormalizeSubjects(expectedSubjects)
		if err != nil {
			return BundleContext{}, err
		}
		if !reflect.DeepEqual(contextDef.InstalledTriggerSubjects, expectedSubjects) {
			return BundleContext{}, fmt.Errorf("runtime context %s installed provider-trigger subjects do not match its bundle inventory", bundleHash)
		}
	}
	if err := validateTargetsGeneration(contextDef, contextDef.ProviderTriggerGeneration); err != nil {
		return BundleContext{}, err
	}
	return contextDef, nil
}

func equalRuntimeContextSlice[T any](left, right []T) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	return reflect.DeepEqual(left, right)
}

func validateRuntimeContextStandingTargets(contextDef BundleContext) error {
	bundleHash := contextDef.BundleHash()
	seen := map[string]string{}
	for _, target := range contextDef.StandingTargets {
		target = target.normalized()
		if target.BundleHash != bundleHash {
			return fmt.Errorf("runtime context %s standing target %q/%q bundle_hash %q does not match context", bundleHash, target.Alias, target.Provider, target.BundleHash)
		}
		if target.Alias == "" || target.Provider == "" || target.RunID == "" || target.Generation <= 0 || target.FlowPath == "" || target.FlowInstance == "" || target.EntityID == "" || !target.AdmissionPlan.Valid() {
			return fmt.Errorf("runtime context %s standing target requires alias, provider, run_id, flow_path, flow_instance, entity_id, and compiled admission plan", bundleHash)
		}
		if target.AdmissionPlan.RequiresSecret() != (target.SigningSecret != "") {
			return fmt.Errorf("runtime context %s standing target %q/%q signing_secret presence contradicts compiled %s request authentication", bundleHash, target.Alias, target.Provider, target.AdmissionPlan.RequestAuthentication())
		}
		key := target.Alias + "\x00" + target.Provider
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("runtime context %s duplicate standing target %s and %s for alias %q provider %q", bundleHash, previous, target.SourcePath, target.Alias, target.Provider)
		}
		seen[key] = target.SourcePath
	}
	return nil
}

func (m *RuntimeContextManager) refreshCapabilitySubjectsLocked() error {
	var installed []bundleScopedInstalledSubject
	var subjects []packs.Subject
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		for _, subject := range entry.context.InstalledTriggerSubjects {
			installed = append(installed, bundleScopedInstalledSubject{
				subject: subject, bundleHash: bundleHash,
				inventoryDigest: entry.context.PackInventoryDigest,
				generation:      entry.context.ProviderTriggerGeneration.Diagnostic(),
			})
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
	projected, err := projectBundleScopedInstalledSubjects(installed)
	if err != nil {
		return fmt.Errorf("project installed provider capability subjects: %w", err)
	}
	subjects = append(projected, subjects...)
	normalized, err := packs.NormalizeSubjects(subjects)
	if err != nil {
		return fmt.Errorf("normalize process provider capability subjects: %w", err)
	}
	m.setBaseCapabilitySubjectsLocked(normalized)
	return nil
}

type bundleScopedInstalledSubject struct {
	subject         packs.Subject
	bundleHash      string
	inventoryDigest string
	generation      string
}

func projectBundleScopedInstalledSubjects(scoped []bundleScopedInstalledSubject) ([]packs.Subject, error) {
	groups := map[string][]bundleScopedInstalledSubject{}
	var keys []string
	for _, item := range scoped {
		normalized, err := packs.NormalizeSubjects([]packs.Subject{item.subject})
		if err != nil {
			return nil, err
		}
		item.subject = normalized[0]
		key := string(item.subject.Kind) + "\x00" + item.subject.ID
		if _, exists := groups[key]; !exists {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], item)
	}
	sort.Strings(keys)
	var out []packs.Subject
	for _, key := range keys {
		items := groups[key]
		unique := make([]bundleScopedInstalledSubject, 0, len(items))
		for _, item := range items {
			duplicate := false
			for _, existing := range unique {
				if reflect.DeepEqual(existing.subject, item.subject) {
					duplicate = true
					break
				}
			}
			if !duplicate {
				unique = append(unique, item)
			}
		}
		if len(unique) == 1 {
			out = append(out, unique[0].subject)
			continue
		}
		sort.Slice(unique, func(i, j int) bool { return unique[i].bundleHash < unique[j].bundleHash })
		for _, item := range unique {
			subject := item.subject
			subject.ID += "@" + item.bundleHash
			subject.Evidence = append(subject.Evidence, packs.Evidence{Kind: "bundle_pack_generation", Fields: map[string]string{
				"bundle_hash": item.bundleHash, "inventory_digest": item.inventoryDigest, "catalog_generation": item.generation,
			}})
			out = append(out, subject)
		}
	}
	return out, nil
}

func (m *RuntimeContextManager) setBaseCapabilitySubjectsLocked(subjects []packs.Subject) {
	m.capabilitySubjects = packs.CloneSubjects(subjects)
	m.capabilityRevision++
}

func (m *RuntimeContextManager) publishRuntimeContextVisibilityLocked(updates ...runtimeContextVisibilityUpdate) error {
	type priorVisibility struct {
		entry *runtimeContextEntry
		state RuntimeContextState
		cause string
	}
	previous := make([]priorVisibility, 0, len(updates))
	seen := make(map[*runtimeContextEntry]struct{}, len(updates))
	for _, update := range updates {
		if update.entry == nil {
			return errors.New("runtime context visibility update requires an entry")
		}
		if _, duplicate := seen[update.entry]; duplicate {
			return errors.New("runtime context visibility update contains a duplicate entry")
		}
		seen[update.entry] = struct{}{}
	}
	for _, update := range updates {
		previous = append(previous, priorVisibility{entry: update.entry, state: update.entry.state, cause: update.entry.cause})
		update.entry.state = update.state
		update.entry.cause = strings.TrimSpace(update.cause)
	}
	if err := m.refreshCapabilitySubjectsLocked(); err != nil {
		for _, prior := range previous {
			prior.entry.state = prior.state
			prior.entry.cause = prior.cause
		}
		restoreErr := m.refreshCapabilitySubjectsLocked()
		return errors.Join(fmt.Errorf("publish runtime context visibility: %w", err), restoreErr)
	}
	return nil
}

func (m *RuntimeContextManager) BaseCapabilitySubjects() []packs.Subject {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return packs.CloneSubjects(m.capabilitySubjects)
}

func (m *RuntimeContextManager) EvaluatedCapabilitySubjects(ctx context.Context, owner *runtimecredentials.SnapshotOwner) ([]packs.Subject, error) {
	if m == nil {
		return nil, fmt.Errorf("runtime context manager is required")
	}
	m.mu.RLock()
	revision := m.capabilityRevision
	base := packs.CloneSubjects(m.capabilitySubjects)
	targets := make(map[string]StandingTarget)
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		activationSigning, err := currentChannelActivationSigningKeys(entry)
		if err != nil {
			m.mu.RUnlock()
			return nil, err
		}
		for _, target := range entry.context.StandingTargets {
			if m.standingServiceSuppressedLocked(target.ServiceID) {
				continue
			}
			selector := fmt.Sprintf("ingress:%s:%s", target.FlowPath, target.Provider)
			if signingKey, found := activationSigning[selector]; found {
				target.SigningSecret = signingKey
			}
			subject, err := target.CapabilitySubject()
			if err != nil {
				m.mu.RUnlock()
				return nil, fmt.Errorf("derive standing ingress capability subject: %w", err)
			}
			targets[subject.ID] = target
		}
	}
	m.mu.RUnlock()

	projection := owner.BeginSecretBindingProjection()
	evaluated := make([]packs.Subject, 0, len(base))
	for _, subject := range base {
		if subject.Kind != packs.SubjectProviderTrigger || subject.Applicability != "effective" {
			evaluated = append(evaluated, subject)
			continue
		}
		target, ok := targets[subject.ID]
		if !ok {
			return nil, fmt.Errorf("effective provider trigger subject %q has no current standing target", subject.ID)
		}
		activationBackedSubject, err := target.CapabilitySubject()
		if err != nil {
			return nil, err
		}
		if activationBackedSubject.ID != subject.ID {
			return nil, fmt.Errorf("activation-backed provider trigger subject changed identity from %q to %q", subject.ID, activationBackedSubject.ID)
		}
		current, err := evaluateStandingIngressCapabilitySubject(ctx, target, activationBackedSubject, projection)
		if err != nil {
			return nil, err
		}
		evaluated = append(evaluated, current)
	}
	normalized, err := packs.NormalizeSubjects(evaluated)
	if err != nil {
		return nil, err
	}
	if err := projection.ValidateCurrent(ctx); err != nil {
		return nil, err
	}
	m.mu.RLock()
	currentRevision := m.capabilityRevision
	m.mu.RUnlock()
	if currentRevision != revision {
		return nil, fmt.Errorf("provider ingress capability projection became stale while credentials were observed")
	}
	return normalized, nil
}

func currentChannelActivationSigningKeys(entry *runtimeContextEntry) (map[string]string, error) {
	out := map[string]string{}
	if !runtimeContextEntryLoaded(entry) || entry.context == nil || entry.runtime == nil || entry.runtime.ChannelActivations == nil {
		return out, nil
	}
	lease, available := entry.runtime.ChannelActivations.AcquirePresentation()
	if !available {
		return nil, fmt.Errorf("runtime context %s channel activation publication is unavailable", entry.context.SourceArtifactFact.BundleHash())
	}
	defer lease.Release()
	for _, activation := range lease.Activations() {
		selector := strings.TrimSpace(activation.Plan.RegistrationTarget())
		if selector == "" {
			continue
		}
		if activation.Coordinate.BundleHash != entry.context.SourceArtifactFact.BundleHash() ||
			activation.Coordinate.RuntimeInstanceID != entry.context.RuntimeInstanceID ||
			activation.Coordinate.ContextPublicationGeneration != entry.context.PublicationGeneration {
			return nil, fmt.Errorf("channel activation registration target %q contradicts its runtime context", selector)
		}
		target, err := packs.ParseChannelRegistrationTarget(selector)
		if err != nil {
			return nil, err
		}
		matched := false
		for _, standing := range entry.context.StandingTargets {
			if standing.FlowPath == target.FlowPath && standing.Provider == target.Provider {
				if uint64(standing.Generation) != activation.Coordinate.TargetGeneration {
					return nil, fmt.Errorf("channel activation registration target %q has a contradictory target generation", selector)
				}
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("channel activation registration target %q has no exact standing target", selector)
		}
		registration, ok := activation.Plan.Registration()
		if !ok {
			return nil, fmt.Errorf("channel activation registration target %q has no compiled registration", selector)
		}
		signingKey := strings.TrimSpace(activation.Plan.CredentialStoreKeys()[registration.SigningCredential()])
		if signingKey == "" {
			return nil, fmt.Errorf("channel activation registration target %q has no signing credential key", selector)
		}
		if existing, duplicate := out[selector]; duplicate && existing != signingKey {
			return nil, fmt.Errorf("channel activation registration target %q has contradictory signing credential keys", selector)
		}
		out[selector] = signingKey
	}
	return out, nil
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

func (m *RuntimeContextManager) duplicateLoadedAgentSlugLocked(incoming BundleContext) (runtimeContextAgentSlugCollision, bool, error) {
	incomingIDs, err := runtimeContextAgentIDs(incoming.Source)
	if err != nil {
		return runtimeContextAgentSlugCollision{}, false, err
	}
	if len(incomingIDs) == 0 {
		return runtimeContextAgentSlugCollision{}, false, nil
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
		existingAgentIDs, err := runtimeContextAgentIDs(entry.context.Source)
		if err != nil {
			return runtimeContextAgentSlugCollision{}, false, err
		}
		for _, existingAgentID := range existingAgentIDs {
			if _, ok := incomingSet[existingAgentID]; !ok {
				continue
			}
			return runtimeContextAgentSlugCollision{
				agentID:  existingAgentID,
				existing: *entry.context,
				incoming: incoming,
			}, true, nil
		}
	}
	return runtimeContextAgentSlugCollision{}, false, nil
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

func runtimeContextAgentIDs(source semanticview.Source) ([]string, error) {
	if source == nil {
		return nil, nil
	}
	plans, err := semanticview.AgentNamePlans(source)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(plans))
	for _, plan := range plans {
		ids = append(ids, plan.AgentID)
	}
	sort.Strings(ids)
	return ids, nil
}

func runtimeContextBundleLabel(contextDef BundleContext) string {
	contextDef = contextDef.normalized()
	parts := []string{}
	if bundleHash := contextDef.BundleHash(); bundleHash != "" {
		parts = append(parts, "bundle_hash="+bundleHash)
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

func (m *RuntimeContextManager) ChannelActivationPublication(bundleHash string, publicationGeneration uint64) (channelonboarding.ChannelActivationPublication, bool, error) {
	lease, current, err := m.AcquireChannelActivationPublication(bundleHash, publicationGeneration)
	if err != nil || !current {
		return channelonboarding.ChannelActivationPublication{}, current, err
	}
	defer lease.Release()
	publication, err := channelonboarding.NewChannelActivationPublication(lease.Activations())
	return publication, err == nil, err
}

func (m *RuntimeContextManager) AcquireChannelActivationPublication(bundleHash string, publicationGeneration uint64) (*runtimechannelactivation.Lease, bool, error) {
	if m == nil || strings.TrimSpace(bundleHash) == "" || publicationGeneration == 0 {
		return nil, false, fmt.Errorf("runtime context channel activation coordinate is required")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry := m.contexts[strings.TrimSpace(bundleHash)]
	if !runtimeContextEntryLoaded(entry) || entry.context == nil || entry.runtime == nil || entry.runtime.ChannelActivations == nil {
		return nil, false, nil
	}
	if entry.context.PublicationGeneration != publicationGeneration {
		return nil, false, nil
	}
	lease, available := entry.runtime.ChannelActivations.AcquirePresentation()
	if !available {
		return nil, false, nil
	}
	if !entry.context.ChannelActivationGeneration.Equal(lease.Generation()) {
		lease.Release()
		return nil, false, fmt.Errorf("runtime context %s channel activation generation contradicts its owner", bundleHash)
	}
	return lease, true, nil
}

// ReplaceChannelActivations publishes an exact executable activation snapshot
// only to the still-current runtime-context publication that owns it.
func (m *RuntimeContextManager) ReplaceChannelActivations(bundleHash string, publicationGeneration uint64, publication channelonboarding.ChannelActivationPublication) error {
	return m.ReplaceChannelActivationsContext(context.Background(), bundleHash, publicationGeneration, publication)
}

func (m *RuntimeContextManager) ReplaceChannelActivationsContext(ctx context.Context, bundleHash string, publicationGeneration uint64, publication channelonboarding.ChannelActivationPublication) error {
	if m == nil || strings.TrimSpace(bundleHash) == "" || publicationGeneration == 0 {
		return fmt.Errorf("runtime context channel activation coordinate is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.contexts[strings.TrimSpace(bundleHash)]
	if entry == nil || entry.context == nil || entry.runtime == nil || entry.state != RuntimeContextStateLoaded {
		return fmt.Errorf("runtime context %s is not loaded for channel activation replacement", bundleHash)
	}
	if entry.context.PublicationGeneration != publicationGeneration {
		return fmt.Errorf("runtime context %s publication changed before channel activation replacement", bundleHash)
	}
	for _, activation := range publication.Activations() {
		if activation.Coordinate.BundleHash != entry.context.BundleHash() ||
			activation.Coordinate.RuntimeInstanceID != entry.context.RuntimeInstanceID ||
			activation.Coordinate.ContextPublicationGeneration != entry.context.PublicationGeneration {
			return fmt.Errorf("runtime context %s channel activation occurrence contradicts current publication", bundleHash)
		}
	}
	if err := entry.runtime.ReplaceChannelActivationsContext(ctx, publication); err != nil {
		return err
	}
	entry.context.ChannelActivationGeneration = publication.Generation()
	return nil
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
	if m.primaryBundleHash == "" {
		return nil, false
	}
	entry := m.contexts[m.primaryBundleHash]
	if !runtimeContextEntryLoaded(entry) {
		return nil, false
	}
	return entry.context, true
}

func (m *RuntimeContextManager) CurrentPublication() (RuntimeContextPublicationSnapshot, error) {
	if m == nil {
		return RuntimeContextPublicationSnapshot{}, errors.New("runtime context manager is required")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	primary := m.contexts[m.primaryBundleHash]
	if !runtimeContextEntryLoaded(primary) {
		return RuntimeContextPublicationSnapshot{}, errors.New("primary runtime context is unavailable")
	}
	identity := primary.context.BundleIdentity
	identity.BundleHash = primary.context.BundleHash()
	snapshot := RuntimeContextPublicationSnapshot{
		PrimaryBundle:       identity,
		SourceArtifactFacts: make([]runtimecorrelation.SourceArtifactFact, 0, len(m.order)),
	}
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		fact := entry.context.SourceArtifactFact
		if err := fact.Validate(); err != nil {
			return RuntimeContextPublicationSnapshot{}, fmt.Errorf("loaded runtime context %s source fact: %w", bundleHash, err)
		}
		snapshot.SourceArtifactFacts = append(snapshot.SourceArtifactFacts, fact)
	}
	return snapshot, nil
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

type standingOccurrenceTransition struct {
	entry      *runtimeContextEntry
	occurrence *worklifetime.StandingOccurrence
	scheduler  *runtimepipeline.Scheduler
	parked     *runtimepipeline.ParkedOccurrence
}

// StandingServiceTransition is the sole process-local owner for a standing
// suspend/reset transition. It parks exact-owner timer projections before
// active descendants drain, then either restores the same unretired occurrence
// or retires it after the durable transition commits.
type StandingServiceTransition struct {
	mu          sync.Mutex
	manager     *RuntimeContextManager
	serviceID   string
	occurrences []standingOccurrenceTransition
	settled     bool
}

func (m *RuntimeContextManager) BeginStandingServiceTransition(ctx context.Context, serviceID string) (*StandingServiceTransition, error) {
	if m == nil {
		return nil, errors.New("runtime context manager is required")
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return nil, errors.New("standing service_id is required")
	}
	transition := &StandingServiceTransition{manager: m, serviceID: serviceID}
	m.mu.Lock()
	for _, entry := range m.contexts {
		if entry == nil || entry.standing == nil || entry.standing[serviceID] == nil {
			continue
		}
		occurrence := entry.standing[serviceID]
		if err := occurrence.Fence(); err != nil {
			for _, prior := range transition.occurrences {
				_ = prior.occurrence.Reopen()
			}
			m.mu.Unlock()
			return nil, fmt.Errorf("fence standing service %s occurrence: %w", serviceID, err)
		}
		var scheduler *runtimepipeline.Scheduler
		if entry.runtime != nil {
			scheduler = entry.runtime.Scheduler
		}
		transition.occurrences = append(transition.occurrences, standingOccurrenceTransition{
			entry: entry, occurrence: occurrence, scheduler: scheduler,
		})
	}
	if len(transition.occurrences) == 0 {
		m.mu.Unlock()
		return nil, fmt.Errorf("standing service %s has no process occurrence", serviceID)
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
		for _, prior := range transition.occurrences {
			_ = prior.occurrence.Reopen()
		}
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Unlock()

	for i := range transition.occurrences {
		item := &transition.occurrences[i]
		if item.scheduler == nil {
			continue
		}
		parked, err := item.scheduler.ParkOccurrence(ctx, item.occurrence)
		item.parked = parked
		if err != nil {
			return nil, errors.Join(err, transition.Restore(context.Background()))
		}
	}
	return transition, nil
}

func (t *StandingServiceTransition) Wait(ctx context.Context) error {
	if t == nil {
		return errors.New("standing service transition is required")
	}
	for _, item := range t.occurrences {
		if err := item.occurrence.Wait(ctx); err != nil {
			return fmt.Errorf("drain standing service %s occurrence: %w", t.serviceID, err)
		}
	}
	return nil
}

func (t *StandingServiceTransition) Restore(ctx context.Context) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.settled {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, item := range t.occurrences {
		if err := item.occurrence.Reopen(); err != nil {
			return fmt.Errorf("reopen standing service %s occurrence: %w", t.serviceID, err)
		}
	}
	for _, item := range t.occurrences {
		if item.scheduler == nil || item.parked == nil {
			continue
		}
		if err := item.parked.RestoreOriginal(ctx); err != nil {
			for _, fenced := range t.occurrences {
				_ = fenced.occurrence.Fence()
			}
			return fmt.Errorf("restore standing service %s schedules: %w", t.serviceID, err)
		}
	}
	t.manager.mu.Lock()
	delete(t.manager.suppressedStandingServices, t.serviceID)
	if err := t.manager.refreshCapabilitySubjectsLocked(); err != nil {
		t.manager.suppressedStandingServices[t.serviceID] = struct{}{}
		t.manager.mu.Unlock()
		for _, fenced := range t.occurrences {
			_ = fenced.occurrence.Fence()
		}
		return err
	}
	t.manager.mu.Unlock()
	t.settled = true
	return nil
}

func (t *StandingServiceTransition) Retire(ctx context.Context) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.settled {
		return nil
	}
	t.manager.mu.Lock()
	for _, item := range t.occurrences {
		if item.entry != nil && item.entry.standing != nil && item.entry.standing[t.serviceID] == item.occurrence {
			delete(item.entry.standing, t.serviceID)
		}
	}
	t.manager.mu.Unlock()
	for _, item := range t.occurrences {
		if err := item.occurrence.RetireAndWait(ctx); err != nil {
			return fmt.Errorf("retire standing service %s occurrence: %w", t.serviceID, err)
		}
	}
	t.settled = true
	return nil
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
func (m *RuntimeContextManager) PrepareStandingServicePublication(serviceID, runID string, generation int64) (*PreparedStandingServicePublication, error) {
	if m == nil {
		return nil, errors.New("runtime context manager is required")
	}
	serviceID = strings.TrimSpace(serviceID)
	runID = strings.TrimSpace(runID)
	if serviceID == "" || runID == "" || generation <= 0 {
		return nil, errors.New("prepared standing publication requires service_id, run_id, and positive generation")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, suppressed := m.suppressedStandingServices[serviceID]; !suppressed {
		return nil, fmt.Errorf("standing service %s must remain suppressed during successor preparation", serviceID)
	}
	var selected *runtimeContextEntry
	for _, entry := range m.contexts {
		if entry == nil || entry.context == nil {
			continue
		}
		for _, target := range entry.context.StandingTargets {
			if target.normalized().ServiceID != serviceID {
				continue
			}
			if selected != nil && selected != entry {
				return nil, fmt.Errorf("standing service %s has more than one runtime owner", serviceID)
			}
			selected = entry
		}
	}
	if selected == nil || !runtimeContextEntryLoaded(selected) || selected.workOwner == nil {
		return nil, fmt.Errorf("standing service %s has no loaded runtime owner", serviceID)
	}
	if selected.standing != nil && selected.standing[serviceID] != nil {
		return nil, fmt.Errorf("standing service %s predecessor process occurrence is still live", serviceID)
	}
	identity := worklifetime.StandingIdentity{ServiceID: serviceID, RunID: runID, Generation: uint64(generation)}
	occurrence, err := selected.workOwner.NewStanding(context.Background(), identity)
	if err != nil {
		return nil, fmt.Errorf("prepare standing service %s successor occurrence: %w", serviceID, err)
	}
	return &PreparedStandingServicePublication{
		manager: m, serviceID: serviceID, entry: selected, identity: identity, occurrence: occurrence,
	}, nil
}

func (m *RuntimeContextManager) PublishStandingServiceTargets(serviceID string, targets []StandingTarget) error {
	return m.publishStandingServiceTargets(serviceID, targets, nil)
}

func (m *RuntimeContextManager) publishStandingServiceTargets(serviceID string, targets []StandingTarget, prepared *PreparedStandingServicePublication) error {
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
			if prepared != nil {
				if prepared.manager != m || prepared.entry != entry || prepared.serviceID != serviceID ||
					prepared.identity.RunID != target.RunID || prepared.identity.Generation != uint64(target.Generation) {
					return fmt.Errorf("prepared standing service %s occurrence does not match committed target identity", serviceID)
				}
				newOccurrence = prepared.occurrence
			} else {
				created, err := entry.workOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{
					ServiceID: serviceID, RunID: target.RunID, Generation: uint64(target.Generation),
				})
				if err != nil {
					return fmt.Errorf("publish fresh standing process occurrence: %w", err)
				}
				newOccurrence = created
			}
			occurrenceEntry = entry
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

func validateTargetsGeneration(contextDef BundleContext, generation triggergeneration.Generation) error {
	if (len(contextDef.StandingTargets) > 0 || len(contextDef.InstalledTriggerSubjects) > 0) && !generation.Valid() {
		return fmt.Errorf("runtime context %s provider-trigger catalog generation is required", contextDef.BundleHash())
	}
	for _, target := range contextDef.StandingTargets {
		if !target.AdmissionPlan.Generation().Equal(generation) {
			return fmt.Errorf("runtime context %s standing target %q/%q admission generation %q does not match its bundle generation %q", contextDef.BundleHash(), target.Alias, target.Provider, target.AdmissionPlan.Generation().Diagnostic(), generation.Diagnostic())
		}
	}
	return nil
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

// PrepareSourceSetTransition fences every surviving loaded runtime before the
// selected-store source-set head changes. A prior failed post-commit refresh
// is adopted so replay can complete it without reopening stale authority.
func (m *RuntimeContextManager) PrepareSourceSetTransition(
	ctx context.Context,
	plan runtimeagenttopology.SourceSetPlan,
) (_ *PreparedRuntimeSourceSetTransition, prepareErr error) {
	return m.prepareSourceSetTransition(ctx, plan, false)
}

// PreparePendingSourceSetTransition adopts only survivors left fenced by an
// earlier committed source-set mutation. An ordinary duplicate with no
// pending transition is a no-op and does not rotate loaded generations.
func (m *RuntimeContextManager) PreparePendingSourceSetTransition(
	ctx context.Context,
	plan runtimeagenttopology.SourceSetPlan,
) (_ *PreparedRuntimeSourceSetTransition, prepareErr error) {
	return m.prepareSourceSetTransition(ctx, plan, true)
}

func (m *RuntimeContextManager) prepareSourceSetTransition(
	ctx context.Context,
	plan runtimeagenttopology.SourceSetPlan,
	pendingOnly bool,
) (_ *PreparedRuntimeSourceSetTransition, prepareErr error) {
	if m == nil {
		return nil, errors.New("runtime context manager is required")
	}
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("runtime source-set transition plan: %w", err)
	}
	m.sourceSetMu.Lock()
	keepLock := false
	defer func() {
		if !keepLock {
			m.sourceSetMu.Unlock()
		}
	}()
	wanted := make(map[string]struct{}, len(plan.Sources))
	for _, source := range plan.Sources {
		wanted[source.Normalize().Key()] = struct{}{}
	}
	transition := &PreparedRuntimeSourceSetTransition{manager: m, locked: true}
	m.mu.Lock()
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if entry == nil || entry.context == nil || entry.runtime == nil || entry.workOwner == nil {
			continue
		}
		state := entry.state
		if state == "" {
			state = RuntimeContextStateLoaded
		}
		loaded := state == RuntimeContextStateLoaded
		pending := state == RuntimeContextStateUnloaded && entry.cause == RuntimeContextCauseSourceSetTransition
		if (!loaded && !pending) || (pendingOnly && !pending) {
			continue
		}
		coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: entry.context.SourceArtifactFact.BundleHash()}.Normalize()
		if _, survives := wanted[coordinate.Key()]; !survives {
			m.mu.Unlock()
			return nil, fmt.Errorf("loaded runtime context %s is absent from successor source set", bundleHash)
		}
		transition.entries = append(transition.entries, runtimeSourceSetTransitionEntry{
			bundleHash: bundleHash, entry: entry, fresh: loaded,
		})
	}
	if len(transition.entries) == 0 {
		m.mu.Unlock()
		if m.pendingSourceSetTransition != nil {
			return nil, errors.New("runtime source-set transition admission has no fenced survivor contexts")
		}
		return nil, nil
	}
	visibilityUpdates := make([]runtimeContextVisibilityUpdate, 0, len(transition.entries))
	for i := range transition.entries {
		item := &transition.entries[i]
		if !item.fresh {
			continue
		}
		visibilityUpdates = append(visibilityUpdates, runtimeContextVisibilityUpdate{
			entry: item.entry, state: RuntimeContextStateUnloaded, cause: RuntimeContextCauseSourceSetTransition,
		})
	}
	if len(visibilityUpdates) > 0 {
		if err := m.publishRuntimeContextVisibilityLocked(visibilityUpdates...); err != nil {
			m.mu.Unlock()
			return nil, err
		}
	}
	m.mu.Unlock()
	admission := m.pendingSourceSetTransition
	if admission != nil {
		if admission.SourceSetRevision() != plan.Revision {
			return nil, errors.Join(
				fmt.Errorf("pending runtime source-set transition targets revision %s, not %s", admission.SourceSetRevision(), plan.Revision),
				transition.abortFresh(),
			)
		}
	} else {
		for _, item := range transition.entries {
			if !item.fresh {
				return nil, errors.Join(
					errors.New("fenced runtime source-set survivor is missing its aggregate transition admission"),
					transition.abortFresh(),
				)
			}
		}
		var err error
		admission, err = newRuntimeSourceSetTransitionAdmission(plan.Revision)
		if err != nil {
			return nil, errors.Join(err, transition.abortFresh())
		}
		m.pendingSourceSetTransition = admission
		transition.createdAdmission = true
	}
	transition.admission = admission
	resuming := !transition.createdAdmission
	for i := range transition.entries {
		item := &transition.entries[i]
		prepared, err := item.entry.runtime.PrepareSourceSetGenerationRefresh(
			ctx, plan, item.entry.context.Source, admission, resuming,
		)
		if err != nil {
			for j := i - 1; j >= 0; j-- {
				transition.entries[j].prepared.Abort()
			}
			prepareErr := fmt.Errorf("prepare surviving runtime context %s: %w", item.bundleHash, err)
			if transition.createdAdmission {
				prepareErr = errors.Join(prepareErr, transition.abortFresh())
				admission.complete()
				m.pendingSourceSetTransition = nil
			}
			return nil, prepareErr
		}
		item.prepared = prepared
	}
	keepLock = true
	return transition, nil
}

// RetainForRetry releases local preparation locks while preserving aggregate
// invisibility and the transition admission after the source-set head commits.
func (p *PreparedRuntimeSourceSetTransition) RetainForRetry() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return
	}
	p.done = true
	for i := len(p.entries) - 1; i >= 0; i-- {
		if p.entries[i].prepared != nil {
			p.entries[i].prepared.RetainForRetry()
		}
	}
	p.release()
}

func (p *PreparedRuntimeSourceSetTransition) abortFresh() error {
	if p == nil || p.manager == nil {
		return nil
	}
	p.manager.mu.Lock()
	defer p.manager.mu.Unlock()
	updates := make([]runtimeContextVisibilityUpdate, 0, len(p.entries))
	for _, item := range p.entries {
		if !item.fresh || item.entry.state != RuntimeContextStateUnloaded || item.entry.cause != RuntimeContextCauseSourceSetTransition {
			continue
		}
		updates = append(updates, runtimeContextVisibilityUpdate{entry: item.entry, state: RuntimeContextStateLoaded})
	}
	if len(updates) == 0 {
		return nil
	}
	return p.manager.publishRuntimeContextVisibilityLocked(updates...)
}

func (p *PreparedRuntimeSourceSetTransition) release() {
	if p == nil || p.manager == nil || !p.locked {
		return
	}
	p.locked = false
	p.manager.sourceSetMu.Unlock()
}

// Abort restores only contexts fenced by this pre-commit attempt. Contexts
// adopted from an earlier committed-but-incomplete refresh remain fenced.
func (p *PreparedRuntimeSourceSetTransition) Abort() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return nil
	}
	p.done = true
	for i := len(p.entries) - 1; i >= 0; i-- {
		if p.entries[i].prepared != nil {
			p.entries[i].prepared.Abort()
		}
	}
	err := p.abortFresh()
	if p.createdAdmission {
		p.admission.complete()
		p.manager.pendingSourceSetTransition = nil
	}
	p.release()
	return err
}

// Commit refreshes every survivor to the committed plan and publishes all of
// them together. Any failure leaves the complete survivor set unavailable.
func (p *PreparedRuntimeSourceSetTransition) Commit(
	ctx context.Context,
	capability runtimestartupownership.ProcessCapability,
) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return errors.New("prepared runtime source-set transition is already settled")
	}
	if capability == nil {
		p.done = true
		for i := len(p.entries) - 1; i >= 0; i-- {
			if p.entries[i].prepared != nil {
				p.entries[i].prepared.RetainForRetry()
			}
		}
		p.release()
		return errors.New("runtime source-set transition requires process capability")
	}
	p.done = true
	defer p.release()
	for i := range p.entries {
		item := &p.entries[i]
		if err := item.prepared.Commit(ctx, capability); err != nil {
			for j := len(p.entries) - 1; j > i; j-- {
				p.entries[j].prepared.RetainForRetry()
			}
			return fmt.Errorf("refresh surviving runtime context %s: %w", item.bundleHash, err)
		}
	}
	if err := p.publishCommittedVisibility(); err != nil {
		return err
	}
	p.admission.complete()
	p.manager.pendingSourceSetTransition = nil
	return nil
}

func (p *PreparedRuntimeSourceSetTransition) publishCommittedVisibility() error {
	if p == nil || p.manager == nil {
		return nil
	}
	p.manager.mu.Lock()
	defer p.manager.mu.Unlock()
	for _, item := range p.entries {
		if item.entry.state != RuntimeContextStateUnloaded || item.entry.cause != RuntimeContextCauseSourceSetTransition {
			return fmt.Errorf("surviving runtime context %s changed during source-set transition", item.bundleHash)
		}
	}
	updates := make([]runtimeContextVisibilityUpdate, 0, len(p.entries))
	for _, item := range p.entries {
		updates = append(updates, runtimeContextVisibilityUpdate{entry: item.entry, state: RuntimeContextStateLoaded})
	}
	return p.manager.publishRuntimeContextVisibilityLocked(updates...)
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
		if err := m.publishRuntimeContextVisibilityLocked(runtimeContextVisibilityUpdate{
			entry: entry, state: RuntimeContextStateUnloaded, cause: result.Cause,
		}); err != nil {
			result.State = RuntimeContextStateLoaded
			result.Cause = strings.TrimSpace(entry.cause)
			result.ShutdownErr = err
			m.mu.Unlock()
			return result
		}
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
