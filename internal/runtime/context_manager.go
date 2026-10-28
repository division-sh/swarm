package runtime

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

// RunBundleAvailabilityReader resolves persisted run identity before a request
// is dispatched to a bundle-bound runtime graph.
type RunBundleAvailabilityReader interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type BundleContext struct {
	ProviderTriggerCatalog      *providertriggers.CatalogSnapshot
	SourceArtifactFact          runtimecorrelation.SourceArtifactFact
	RootAdmission               sourceartifact.RootAdmission `json:"-"`
	BundleIdentity              runtimecontracts.BundleIdentity
	RuntimeInstanceID           string
	PublicationGeneration       uint64
	Source                      semanticview.Source
	Runtime                     *Runtime
	WorkOwner                   *worklifetime.RuntimeOccurrence
	WorkspaceScopeKey           string
	StandingTargets             []StandingTarget
	StandingActivations         []StandingActivation
	ProviderTriggerGeneration   triggergeneration.Generation
	InstalledTriggerSubjects    []packs.Subject
	PackInventoryDigest         string
	ChannelPlans                []packs.SatisfactionPlan
	DeclaredChannelPublication  channelonboarding.ChannelActivationPublication
	ChannelActivationGeneration channelonboarding.ChannelActivationGeneration
	EffectiveSourceIdentity     scenarioexecution.EffectiveSourceIdentity
	ScenarioProfileCatalog      *scenarioexecution.Catalog
	deploymentTriggers          map[string]config.TriggerConfig
}

type RuntimeContextState string

const (
	RuntimeContextStateLoaded   RuntimeContextState = "loaded"
	RuntimeContextStateUnloaded RuntimeContextState = "unloaded"

	RuntimeContextCauseNotLoaded           = "runtime_context_not_loaded"
	RuntimeContextCauseUnavailable         = "runtime_context_unavailable"
	RuntimeContextCauseUnloaded            = "runtime_context_unloaded"
	RuntimeContextCauseSourceSetTransition = "runtime_context_source_set_transition"
)

func (c BundleContext) normalized() BundleContext {
	if c.Runtime != nil {
		c.deploymentTriggers = maps.Clone(c.Runtime.Options.TriggerEntries)
	}
	c.RuntimeInstanceID = strings.TrimSpace(c.RuntimeInstanceID)
	c.WorkspaceScopeKey = strings.TrimSpace(c.WorkspaceScopeKey)
	c.PackInventoryDigest = strings.TrimSpace(c.PackInventoryDigest)
	c.InstalledTriggerSubjects = packs.CloneSubjects(c.InstalledTriggerSubjects)
	c.ChannelPlans = append([]packs.SatisfactionPlan(nil), c.ChannelPlans...)
	c.StandingActivations = append([]StandingActivation(nil), c.StandingActivations...)
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

func (c BundleContext) triggerEntries() map[string]config.TriggerConfig {
	if c.Runtime == nil {
		return c.deploymentTriggers
	}
	return c.Runtime.Options.TriggerEntries
}

type runtimeContextEntry struct {
	context          *BundleContext
	runtime          *Runtime
	workOwner        *worklifetime.RuntimeOccurrence
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
	mu            sync.RWMutex
	id            string
	revision      string
	predecessors  map[string]runtimemanager.ProcessExecutionBinding
	done          chan struct{}
	terminalDrain chan struct{}
	completed     bool
	terminating   bool
}

func newRuntimeSourceSetTransitionAdmission(revision string) (*runtimeSourceSetTransitionAdmission, error) {
	revision = strings.TrimSpace(revision)
	if revision == "" {
		return nil, errors.New("runtime source-set transition revision is required")
	}
	return &runtimeSourceSetTransitionAdmission{
		id: uuid.NewString(), revision: revision,
		predecessors: make(map[string]runtimemanager.ProcessExecutionBinding), done: make(chan struct{}), terminalDrain: make(chan struct{}),
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

func (a *runtimeSourceSetTransitionAdmission) TerminalDrain() <-chan struct{} {
	return a.terminalDrain
}

// Only aggregate shutdown, after fencing the complete set, may release drain
// waiters. The refresh remains incomplete and cannot admit execution or retry.
func (a *runtimeSourceSetTransitionAdmission) terminate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.terminating {
		a.terminating = true
		close(a.terminalDrain)
	}
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

// RuntimeContextUse is the only execution-bearing result produced by the
// runtime selector. Metadata lookups never expose a raw Runtime pointer.
type RuntimeContextUse struct {
	Context          BundleContext
	runtime          *Runtime
	lease            *worklifetime.Lease
	leases           []*worklifetime.Lease
	grant            *runtimestartupownership.GrantEvidence
	releaseAdmission func()
	once             sync.Once
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
	ctx := context.Background()
	if len(u.leases) > 0 {
		ctx = u.leases[len(u.leases)-1].Context()
	} else if u.lease != nil {
		ctx = u.lease.Context()
	}
	if u.grant != nil {
		ctx = runtimestartupownership.WithGenerationGrantEvidence(ctx, *u.grant)
	}
	return ctx
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
		if u.releaseAdmission != nil {
			u.releaseAdmission()
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
	Context       *BundleContext
	Target        StandingTarget
	State         RuntimeContextState
	Cause         string
	Found         bool
	AliasFound    bool
	authorIngress bool
}

// DeclaresAuthorIngress is a route-classification hint, never execution
// admission. Publication still requires the one acquired context and its fence.
func (l RuntimeIngressContextLookup) DeclaresAuthorIngress() bool {
	return l.Found && l.authorIngress
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
	liveSelectionMu            sync.RWMutex
	availability               RunBundleAvailabilityReader
	landingRuns                runtimerunlifecycle.LandingRunReader
	contexts                   map[string]*runtimeContextEntry
	order                      []string
	primaryBundleHash          string
	capabilitySubjects         []packs.Subject
	capabilityRevision         uint64
	nextPublicationGeneration  uint64
	pendingSourceSetTransition *runtimeSourceSetTransitionAdmission
	resetExecutionFenced       bool
	channelSessions            *channelSessionAdmission
}

// The platform installs one original native issuer, not a provider registry.
func (m *RuntimeContextManager) BindChannelSessionAdmissionOwner(owner operatorchannel.SessionAdmissionOwner) error {
	if m == nil || owner == nil {
		return fmt.Errorf("channel session admission owner is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.channelSessions != nil {
		return fmt.Errorf("channel session admission owner is already installed")
	}
	binding := &channelSessionAdmission{owner: owner}
	for _, entry := range m.contexts {
		if entry.runtime == nil || entry.runtime.channelSessions.Load() != nil {
			return fmt.Errorf("runtime context has another channel session admission owner")
		}
	}
	for _, entry := range m.contexts {
		if err := entry.runtime.bindChannelSessionAdmission(binding); err != nil {
			return err
		}
	}
	m.channelSessions = binding
	return nil
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
	landing, _ := availability.(runtimerunlifecycle.LandingRunReader)
	return &RuntimeContextManager{
		availability: availability,
		landingRuns:  landing,
		contexts:     map[string]*runtimeContextEntry{},
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
		if err := manager.register(contextDef, false, false); err != nil {
			return err
		}
	}
	return manager.refreshCapabilitySubjectsLocked()
}

func (m *RuntimeContextManager) Register(contextDef BundleContext) error {
	if m == nil {
		return fmt.Errorf("runtime context manager is required")
	}
	m.sourceSetMu.Lock()
	defer m.sourceSetMu.Unlock()
	return m.register(contextDef, true, false)
}

// RegisterLiveReplacement publishes the sole bundle after withdrawal. Returning
// to a prior bundle also requires its exact previous occurrence to have joined.
func (m *RuntimeContextManager) RegisterLiveReplacement(contextDef BundleContext) error {
	if m == nil {
		return errors.New("runtime context manager is required")
	}
	m.sourceSetMu.Lock()
	defer m.sourceSetMu.Unlock()
	return m.register(contextDef, true, true)
}

func (m *RuntimeContextManager) register(contextDef BundleContext, activateOccurrences, replaceJoined bool) error {
	if m == nil {
		return fmt.Errorf("runtime context manager is required")
	}
	contextDef, err := validateRuntimeContextDefinition(contextDef, activateOccurrences)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.contexts == nil {
		m.contexts = map[string]*runtimeContextEntry{}
	}
	previous, err := m.registrationPredecessorLocked(contextDef, replaceJoined)
	if err != nil {
		return err
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
	if collision := m.duplicateLoadedIngressAliasLocked(contextDef); collision != nil {
		return collision
	}
	m.nextPublicationGeneration++
	contextDef.PublicationGeneration = m.nextPublicationGeneration
	copied := contextDef
	runtimeOwner := copied.Runtime
	workOwner := copied.WorkOwner
	copied.Runtime = nil
	copied.WorkOwner = nil
	m.contexts[contextDef.BundleHash()] = &runtimeContextEntry{
		context:   &copied,
		runtime:   runtimeOwner,
		workOwner: workOwner,
		state:     RuntimeContextStateUnloaded,
		cause:     RuntimeContextCauseNotLoaded,
	}
	if previous == nil {
		m.order = append(m.order, contextDef.BundleHash())
		sort.Strings(m.order)
	}
	if err := m.publishRuntimeContextVisibilityLocked(runtimeContextVisibilityUpdate{
		entry: m.contexts[contextDef.BundleHash()], state: RuntimeContextStateLoaded,
	}); err != nil {
		m.rollbackRegistrationLocked(contextDef.BundleHash(), previous)
		return err
	}
	if m.primaryBundleHash == "" || replaceJoined {
		m.primaryBundleHash = contextDef.BundleHash()
	}
	return nil
}

func (m *RuntimeContextManager) rollbackRegistrationLocked(hash string, previous *runtimeContextEntry) {
	if previous != nil {
		m.contexts[hash] = previous
	} else {
		delete(m.contexts, hash)
		for i, bundleHash := range m.order {
			if bundleHash == hash {
				m.order = append(m.order[:i], m.order[i+1:]...)
				break
			}
		}
	}
}

func (m *RuntimeContextManager) registrationPredecessorLocked(contextDef BundleContext, replaceJoined bool) (*runtimeContextEntry, error) {
	if replaceJoined {
		for _, entry := range m.contexts {
			if runtimeContextEntryLoaded(entry) || !entry.shutdownMu.TryLock() {
				return nil, errors.New("bundle replacement requires every predecessor to be withdrawn and joined")
			}
			joined := entry.shutdownComplete
			entry.shutdownMu.Unlock()
			if !joined {
				return nil, errors.New("bundle replacement requires every predecessor to be joined")
			}
		}
	}
	previous := m.contexts[contextDef.BundleHash()]
	if previous != nil {
		if !replaceJoined || previous.state != RuntimeContextStateUnloaded || previous.runtime == contextDef.Runtime || !previous.shutdownMu.TryLock() {
			return nil, fmt.Errorf("duplicate runtime context bundle_hash %s", contextDef.BundleHash())
		}
		joined := previous.shutdownComplete
		previous.shutdownMu.Unlock()
		if !joined {
			return nil, fmt.Errorf("runtime context %s has not joined", contextDef.BundleHash())
		}
	}
	return previous, nil
}

func validateRuntimeContextDefinition(contextDef BundleContext, executable bool) (BundleContext, error) {
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
	if err := validateRuntimeContextRoot(contextDef); err != nil {
		return BundleContext{}, err
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
	if err := validateRuntimeContextStandingTargets(contextDef, executable); err != nil {
		return BundleContext{}, err
	}
	if err := validateRuntimeContextStandingActivations(contextDef); err != nil {
		return BundleContext{}, err
	}
	normalizedSubjects, err := packs.NormalizeSubjects(contextDef.InstalledTriggerSubjects)
	if err != nil {
		return BundleContext{}, fmt.Errorf("runtime context %s installed provider trigger subjects: %w", bundleHash, err)
	}
	contextDef.InstalledTriggerSubjects = normalizedSubjects
	if err := validateRuntimeContextProviderCatalog(contextDef); err != nil {
		return BundleContext{}, err
	}
	if err := validateTargetsGeneration(contextDef, contextDef.ProviderTriggerGeneration); err != nil {
		return BundleContext{}, err
	}
	return contextDef, nil
}

func validateRuntimeContextProviderCatalog(contextDef BundleContext) error {
	bundle, present := semanticview.Bundle(contextDef.Source)
	if !present || bundle == nil {
		return fmt.Errorf("runtime context provider catalog requires its compiled source")
	}
	if bundle.PackInventory != nil && contextDef.PackInventoryDigest != bundle.PackInventory.Digest() {
		return fmt.Errorf("runtime context %s pack inventory digest does not match its captured inventory", contextDef.BundleHash())
	}
	catalog := contextDef.ProviderTriggerCatalog
	if catalog == nil {
		return fmt.Errorf("runtime context %s requires its captured provider catalog", contextDef.BundleHash())
	}
	if !contextDef.ProviderTriggerGeneration.Equal(catalog.Generation()) {
		return fmt.Errorf("runtime context %s provider-trigger generation does not match its captured catalog", contextDef.BundleHash())
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return err
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		return err
	}
	matched := false
	for _, flow := range providers.FlowPaths() {
		scope, err := providers.Scope(flow)
		if err != nil {
			return err
		}
		expected, err := scope.TriggerCatalog()
		if err != nil {
			return err
		}
		if !catalog.Generation().Equal(expected.Generation()) || len(catalog.Entries()) != len(expected.Entries()) {
			continue
		}
		matched = true
		for _, entry := range catalog.Entries() {
			owner, found := expected.EntryByProvider(entry.Manifest.Provider())
			if !found || owner.Identity != entry.Identity || owner.SourcePath != entry.SourcePath {
				matched = false
				break
			}
		}
		if matched {
			break
		}
	}
	if !matched {
		return fmt.Errorf("runtime context %s provider catalog is not an exact captured flow scope", contextDef.BundleHash())
	}
	expectedSubjects, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(contextDef.InstalledTriggerSubjects, expectedSubjects) {
		return fmt.Errorf("runtime context %s installed provider-trigger subjects do not match its captured catalog", contextDef.BundleHash())
	}
	return nil
}

func equalRuntimeContextSlice[T any](left, right []T) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	return reflect.DeepEqual(left, right)
}

func validateRuntimeContextStandingTargets(contextDef BundleContext, executable bool) error {
	bundleHash := contextDef.BundleHash()
	seen := map[string]string{}
	aliasOwners := map[string]StandingTarget{}
	for _, target := range contextDef.StandingTargets {
		target = target.normalized()
		if err := runtimecontracts.ValidateIngressAlias(target.Alias); err != nil {
			return err
		}
		if previous, present := aliasOwners[target.Alias]; present && previous.FlowPath != target.FlowPath {
			return fmt.Errorf("duplicate enabled ingress alias %q: flow %q (%s) and flow %q (%s)", target.Alias, previous.FlowPath, previous.SourcePath, target.FlowPath, target.SourcePath)
		}
		aliasOwners[target.Alias] = target
		if target.BundleHash != bundleHash {
			return fmt.Errorf("runtime context %s standing target %q/%q bundle_hash %q does not match context", bundleHash, target.Alias, target.Provider, target.BundleHash)
		}
		if target.Alias == "" || target.Provider == "" || target.FlowPath == "" || !target.AdmissionPlan.Valid() {
			return fmt.Errorf("runtime context %s ingress target requires its exact declaration, alias, provider and compiled admission plan", bundleHash)
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
		executableSubjects := map[string]struct{}{}
		for _, target := range entry.context.StandingTargets {
			subject, err := target.CapabilitySubject()
			if err != nil {
				return err
			}
			executableSubjects[subject.ID] = struct{}{}
			subjects = append(subjects, subject)
		}
		declared, err := declaredStandingCapabilitySubjects(entry.context, executableSubjects)
		if err != nil {
			return err
		}
		subjects = append(subjects, declared...)
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

func validateStandingTargetDeclarationOwner(contextDef *BundleContext, runtimeOwner *Runtime, target StandingTarget) error {
	declarations, err := ResolveStandingTargetDeclarations(contextDef.Source, contextDef.ProviderTriggerCatalog, contextDef.triggerEntries())
	if err != nil {
		return err
	}
	owned := false
	for _, declaration := range declarations {
		if declaration.FlowPath != target.FlowPath || declaration.Alias != target.Alias {
			continue
		}
		for _, binding := range declaration.Ingress {
			if binding.Provider != target.Provider || !binding.AdmissionPlan.Generation().Equal(target.AdmissionPlan.Generation()) {
				continue
			}
			signingSecret := binding.SigningSecret
			if runtimeOwner != nil {
				_, _, _, err := runtimeOwner.standingIngressCredentialScope(InboundTarget{
					BundleHash: target.BundleHash, FlowPath: target.FlowPath, Alias: target.Alias,
					Provider: target.Provider, SigningSecret: target.SigningSecret, AdmissionPlan: target.AdmissionPlan,
				})
				if err != nil {
					return err
				}
				signingSecret = target.SigningSecret
			}
			if signingSecret != target.SigningSecret {
				continue
			}
			expected, err := binding.AdmissionPlan.EffectiveCapabilitySubject(providertriggers.EffectiveSubjectRequest{
				BundleHash: contextDef.BundleHash(), FlowPath: declaration.FlowPath, Alias: declaration.Alias,
				SigningSecret: signingSecret, SourcePath: declaration.SourcePath,
			})
			if err != nil {
				return err
			}
			actual, err := target.CapabilitySubject()
			if err != nil {
				return err
			}
			owned = reflect.DeepEqual(actual, expected)
		}
	}
	if !owned {
		return fmt.Errorf("committed standing target %s/%s has no exact declaration owner", target.Alias, target.Provider)
	}
	return nil
}

func declaredStandingCapabilitySubjects(contextDef *BundleContext, executableSubjects map[string]struct{}) ([]packs.Subject, error) {
	declarations, err := ResolveStandingTargetDeclarations(contextDef.Source, contextDef.ProviderTriggerCatalog, contextDef.triggerEntries())
	if err != nil {
		return nil, err
	}
	var subjects []packs.Subject
	for _, declaration := range declarations {
		for _, binding := range declaration.Ingress {
			subject, err := binding.AdmissionPlan.EffectiveCapabilitySubject(providertriggers.EffectiveSubjectRequest{
				BundleHash: contextDef.BundleHash(), FlowPath: declaration.FlowPath, Alias: declaration.Alias, SigningSecret: binding.SigningSecret, SourcePath: declaration.SourcePath,
			})
			if err != nil {
				return nil, fmt.Errorf("derive standing ingress capability subject: %w", err)
			}
			if _, executable := executableSubjects[subject.ID]; !executable {
				subjects = append(subjects, subject)
			}
		}
	}
	return subjects, nil
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
	if m.channelSessions != nil {
		for _, update := range updates {
			if update.state == RuntimeContextStateLoaded {
				if err := update.entry.runtime.bindChannelSessionAdmission(m.channelSessions); err != nil {
					return err
				}
			}
		}
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
	credentialOwners := make(map[string]*Runtime)
	ineligible := make(map[string]standingIngressReadback)
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		if entry.runtime != nil {
			blocked, err := entry.runtime.ineligibleStandingCapabilitySubjects()
			if err != nil {
				m.mu.RUnlock()
				return nil, err
			}
			for id, subject := range blocked {
				ineligible[id] = subject
			}
		}
		activationSigning, err := currentChannelActivationSigningKeys(entry)
		if err != nil {
			m.mu.RUnlock()
			return nil, err
		}
		for _, target := range entry.context.StandingTargets {
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
			credentialOwners[subject.ID] = entry.runtime
		}
	}
	m.mu.RUnlock()

	projection := owner.BeginSecretBindingProjection()
	var validateAdmissions []func(context.Context) error
	evaluated := make([]packs.Subject, 0, len(base))
	for _, subject := range base {
		if subject.Kind != packs.SubjectProviderTrigger || subject.Applicability != "effective" {
			evaluated = append(evaluated, subject)
			continue
		}
		target, ok := targets[subject.ID]
		if !ok {
			blocked, blockedPresent := ineligible[subject.ID]
			current, err := evaluateIneligibleStandingSubject(ctx, subject, blocked, blockedPresent, projection)
			if err != nil {
				return nil, err
			}
			evaluated = append(evaluated, current)
			continue
		}
		activationBackedSubject, err := target.CapabilitySubject()
		if err != nil {
			return nil, err
		}
		if activationBackedSubject.ID != subject.ID {
			return nil, fmt.Errorf("activation-backed provider trigger subject changed identity from %q to %q", subject.ID, activationBackedSubject.ID)
		}
		credentialOwner := credentialOwners[subject.ID]
		if credentialOwner == nil {
			return nil, fmt.Errorf("standing ingress subject %q has no runtime credential admission owner", subject.ID)
		}
		current, validate, err := credentialOwner.evaluateStandingIngressAdmission(ctx, target, activationBackedSubject, projection)
		if err != nil {
			return nil, err
		}
		if validate != nil {
			validateAdmissions = append(validateAdmissions, validate)
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
	for _, validate := range validateAdmissions {
		if err := validate(ctx); err != nil {
			return nil, err
		}
	}
	m.mu.RLock()
	currentRevision := m.capabilityRevision
	m.mu.RUnlock()
	if currentRevision != revision {
		return nil, fmt.Errorf("provider ingress capability projection became stale while credentials were observed")
	}
	return normalized, nil
}

func evaluateIneligibleStandingSubject(ctx context.Context, subject packs.Subject, blocked standingIngressReadback, blockedPresent bool, projection *runtimecredentials.SecretBindingProjection) (packs.Subject, error) {
	current := subject
	if blockedPresent {
		current = blocked.subject
	}
	if !blockedPresent || blocked.observeSigning {
		var err error
		current, err = evaluateProviderTriggerCapabilitySubject(ctx, current, projection)
		if err != nil {
			return packs.Subject{}, err
		}
	}
	enabled := false
	current.Status = ""
	current.TriggerAdmission.BindingEnabled = &enabled
	normalized, err := packs.NormalizeSubjects([]packs.Subject{current})
	if err != nil {
		return packs.Subject{}, err
	}
	return normalized[0], nil
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
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("channel activation registration target %q has no exact standing target", selector)
		}
		if activation.Plan.Transport() == packs.ChannelTransportSession {
			continue
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

func (m *RuntimeContextManager) duplicateLoadedIngressAliasLocked(incoming BundleContext) error {
	incomingAliases := map[string]string{}
	for _, target := range incoming.StandingTargets {
		incomingAliases[target.Alias] = target.FlowPath
	}
	return m.duplicateLoadedIngressAliasesLocked(incoming, incomingAliases)
}

func (m *RuntimeContextManager) duplicateLoadedIngressAliasesLocked(incoming BundleContext, incomingAliases map[string]string) error {
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if !runtimeContextEntryLoaded(entry) {
			continue
		}
		for _, target := range entry.context.StandingTargets {
			flowPath, collision := incomingAliases[target.Alias]
			if collision && (bundleHash != incoming.BundleHash() || flowPath != target.FlowPath) {
				return fmt.Errorf("duplicate enabled ingress alias %q: %s flow %q (%s) and %s flow %q", target.Alias, runtimeContextBundleLabel(*entry.context), target.FlowPath, target.SourcePath, runtimeContextBundleLabel(incoming), flowPath)
			}
		}
	}
	return nil
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

func (m *RuntimeContextManager) ReplaceChannelActivationsContext(ctx context.Context, bundleHash string, publicationGeneration uint64, publication channelonboarding.ChannelActivationPublication) (bool, error) {
	if m == nil || strings.TrimSpace(bundleHash) == "" || publicationGeneration == 0 {
		return false, fmt.Errorf("runtime context channel activation coordinate is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.contexts[strings.TrimSpace(bundleHash)]
	if entry == nil || entry.context == nil || entry.runtime == nil || entry.state != RuntimeContextStateLoaded {
		return false, fmt.Errorf("runtime context %s is not loaded for channel activation replacement", bundleHash)
	}
	if entry.context.PublicationGeneration != publicationGeneration {
		return false, fmt.Errorf("runtime context %s publication changed before channel activation replacement", bundleHash)
	}
	for _, activation := range publication.Activations() {
		if activation.Coordinate.BundleHash != entry.context.BundleHash() ||
			activation.Coordinate.RuntimeInstanceID != entry.context.RuntimeInstanceID ||
			activation.Coordinate.ContextPublicationGeneration != entry.context.PublicationGeneration {
			return false, fmt.Errorf("runtime context %s channel activation occurrence contradicts current publication", bundleHash)
		}
	}
	if err := entry.runtime.ReplaceChannelActivationsContext(ctx, publication); err != nil {
		return false, err
	}
	changed := !entry.context.ChannelActivationGeneration.Equal(publication.Generation())
	entry.context.ChannelActivationGeneration = publication.Generation()
	return changed, nil
}

func (m *RuntimeContextManager) acquireEntryLocked(ctx context.Context, entry *runtimeContextEntry) (*RuntimeContextUse, error) {
	if m.resetExecutionFenced {
		return nil, worklifetime.ErrAdmissionFenced
	}
	if !runtimeContextEntryLoaded(entry) || entry.runtime == nil || entry.workOwner == nil {
		return nil, fmt.Errorf("runtime context is unavailable")
	}
	definition := *entry.context
	definition.Runtime = entry.runtime
	ctx, err := rootedRuntimeContextUse(ctx, definition)
	if err == nil {
		ctx, err = entry.runtime.mutationContext(ctx)
	}
	if err != nil {
		return nil, err
	}
	owner := entry.workOwner
	ctx, releaseAdmission, admitted := entry.runtime.shutdownGate.BeginContext(ctx)
	if !admitted {
		return nil, worklifetime.ErrAdmissionFenced
	}
	ctx = worklifetime.WithOccurrence(ctx, owner)
	lease, err := owner.Begin(ctx)
	if err != nil {
		releaseAdmission()
		return nil, fmt.Errorf("admit runtime context use: %w", err)
	}
	use := &RuntimeContextUse{Context: *entry.context, runtime: entry.runtime, lease: lease, releaseAdmission: releaseAdmission}
	entry.runtime.lifecycleMu.Lock()
	grant := entry.runtime.startupGrant
	entry.runtime.lifecycleMu.Unlock()
	if grant != nil {
		evidence, err := grant.Evidence()
		if err != nil {
			return nil, errors.Join(err, use.Done())
		}
		use.grant = &evidence
	}
	return use, nil
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
		lookup.Context = runtimeContextMetadata(entry.context)
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
	return runtimeContextMetadata(entry.context), true
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
			authorIngress := declaredAuthorIngress(entry.runtime, entry.context, target)
			state := entry.state
			if state == "" {
				state = RuntimeContextStateLoaded
			}
			cause := strings.TrimSpace(entry.cause)
			if state != RuntimeContextStateLoaded && cause == "" {
				cause = RuntimeContextCauseUnavailable
			}
			out := RuntimeIngressContextLookup{State: state, Cause: cause, AliasFound: true, authorIngress: authorIngress}
			out.Found = true
			out.Target = target
			if state == RuntimeContextStateLoaded {
				out.Context = runtimeContextMetadata(entry.context)
			}
			return out
		}
	}
	return RuntimeIngressContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded, AliasFound: aliasFound}
}

// Admission and publication share source-set ownership. Alias refusal must
// precede the durable standing mutation, not merely route publication.
func (m *RuntimeContextManager) AdmitChannelStandingTarget(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate, session operatorchannel.ProviderAuthority, barrier channelonboarding.TestLifecycleBarrier) error {
	if m == nil {
		return errors.New("runtime context manager is required")
	}
	if err := candidate.ValidateDeclaration(); err != nil {
		return err
	}
	m.sourceSetMu.Lock()
	defer m.sourceSetMu.Unlock()
	m.mu.RLock()
	entry := m.contexts[candidate.Coordinate.BundleHash]
	if !runtimeContextEntryLoaded(entry) || entry.runtime == nil || entry.context.RuntimeInstanceID != candidate.Coordinate.RuntimeInstanceID || entry.context.PublicationGeneration != candidate.Coordinate.ContextPublicationGeneration {
		m.mu.RUnlock()
		return fmt.Errorf("%w: channel target runtime context is no longer current", channelonboarding.ErrRevisionConflict)
	}
	declarations, err := ResolveStandingTargetDeclarations(entry.context.Source, entry.context.ProviderTriggerCatalog, entry.context.triggerEntries())
	if err != nil {
		m.mu.RUnlock()
		return err
	}
	owned := false
	for _, declaration := range declarations {
		if declaration.FlowPath != candidate.Target.FlowPath || declaration.Alias != candidate.Target.Alias {
			continue
		}
		for _, ingress := range declaration.Ingress {
			if ingress.Provider == candidate.Target.Provider && ingress.AdmissionPlan.Generation().Equal(candidate.Target.AdmissionGeneration) {
				owned = true
			}
		}
	}
	if !owned {
		m.mu.RUnlock()
		return fmt.Errorf("channel target %s has no exact declaration owner", candidate.Target.Selector)
	}
	aliases := map[string]string{candidate.Target.Alias: candidate.Target.FlowPath}
	if collision := m.duplicateLoadedIngressAliasesLocked(*entry.context, aliases); collision != nil {
		m.mu.RUnlock()
		return collision
	}
	use, err := m.acquireEntryLocked(ctx, entry)
	m.mu.RUnlock()
	if err != nil {
		return err
	}
	defer func() { _ = use.Done() }()
	targets, activations, err := use.Runtime().AdmitChannelStandingTarget(use.WorkContext(), op, candidate, session)
	if err != nil {
		return err
	}
	if barrier != nil {
		if err := barrier(channelonboarding.TestAfterStandingTargetReconciliation, op.OperationID); err != nil {
			return err
		}
	}
	if err := use.Runtime().ValidateStandingIngressCredentials(use.WorkContext()); err != nil {
		return err
	}
	m.mu.RLock()
	current := m.contexts[candidate.Coordinate.BundleHash]
	stillCurrent := current == entry && runtimeContextEntryLoaded(current) && current.context.PublicationGeneration == candidate.Coordinate.ContextPublicationGeneration && use.WorkContext().Err() == nil
	m.mu.RUnlock()
	if !stillCurrent {
		return fmt.Errorf("%w: channel target runtime ownership changed before publication", channelonboarding.ErrRevisionConflict)
	}
	if err := m.publishFlowTargets(candidate.Target.FlowPath, targets, activations, use); err != nil {
		return err
	}
	if barrier != nil {
		return barrier(channelonboarding.TestAfterStandingTargetPublication, op.OperationID)
	}
	return nil
}

func (m *RuntimeContextManager) publishFlowTargets(flowPath string, targets []StandingTarget, activations []StandingActivation, admitted *RuntimeContextUse) error {
	if m == nil {
		return nil
	}
	flowPath = strings.TrimSpace(flowPath)
	if flowPath == "" {
		return fmt.Errorf("flow path is required")
	}
	byBundleAndKey := map[string]StandingTarget{}
	for _, raw := range targets {
		target := raw.normalized()
		if target.FlowPath != flowPath {
			return fmt.Errorf("ingress flow %s does not match %s", target.FlowPath, flowPath)
		}
		key := target.BundleHash + "\x00" + target.Alias + "\x00" + target.Provider
		if _, duplicate := byBundleAndKey[key]; duplicate {
			return fmt.Errorf("duplicate standing target publication for %s/%s", target.Alias, target.Provider)
		}
		byBundleAndKey[key] = target
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if admitted != nil {
		if err := m.validateAdmittedStandingPublicationLocked(admitted); err != nil {
			return err
		}
	}
	replaced := 0
	planned := map[string]*BundleContext{}
	for _, bundleHash := range m.order {
		entry := m.contexts[bundleHash]
		if entry == nil || entry.context == nil {
			continue
		}
		copied := *entry.context
		copied.StandingTargets = nil
		publishedActivations, added, err := standingActivationsForPublication(entry.context.StandingActivations, activations, flowPath, bundleHash)
		if err != nil {
			return err
		}
		copied.StandingActivations = publishedActivations
		replaced += added
		changed := added > 0
		for _, existing := range entry.context.StandingTargets {
			if strings.TrimSpace(existing.FlowPath) != flowPath {
				copied.StandingTargets = append(copied.StandingTargets, existing)
				continue
			}
			key := bundleHash + "\x00" + existing.normalized().Alias + "\x00" + existing.normalized().Provider
			published, ok := byBundleAndKey[key]
			if !ok {
				changed = true
				continue
			}
			copied.StandingTargets = append(copied.StandingTargets, published)
			delete(byBundleAndKey, key)
			replaced++
			changed = true
		}
		for key, target := range byBundleAndKey {
			if target.BundleHash != bundleHash {
				continue
			}
			if err := validateStandingTargetDeclarationOwner(&copied, entry.runtime, target); err != nil {
				return err
			}
			copied.StandingTargets = append(copied.StandingTargets, target)
			delete(byBundleAndKey, key)
			replaced++
			changed = true
		}
		if changed {
			if err := validateRuntimeContextStandingTargets(copied, true); err != nil {
				return err
			}
			if err := validateRuntimeContextStandingActivations(copied); err != nil {
				return err
			}
			if collision := m.duplicateLoadedIngressAliasLocked(copied); collision != nil {
				return collision
			}
			planned[bundleHash] = &copied
		}
	}
	if len(byBundleAndKey) != 0 {
		return fmt.Errorf("committed standing target publication has no loaded declaration owner")
	}
	if replaced == 0 && (len(targets) > 0 || len(activations) > 0) {
		return fmt.Errorf("flow %s has no loaded target owner", flowPath)
	}
	oldContexts := map[string]*BundleContext{}
	for bundleHash, contextDef := range planned {
		oldContexts[bundleHash] = m.contexts[bundleHash].context
		m.contexts[bundleHash].context = contextDef
	}
	if err := m.refreshCapabilitySubjectsLocked(); err != nil {
		for bundleHash, contextDef := range oldContexts {
			m.contexts[bundleHash].context = contextDef
		}
		return err
	}
	return nil
}

func (m *RuntimeContextManager) validateAdmittedStandingPublicationLocked(admitted *RuntimeContextUse) error {
	current := m.contexts[admitted.Context.BundleHash()]
	if !runtimeContextEntryLoaded(current) || current.runtime != admitted.Runtime() || current.context.RuntimeInstanceID != admitted.Context.RuntimeInstanceID || current.context.PublicationGeneration != admitted.Context.PublicationGeneration || !current.context.RootAdmission.Equal(admitted.Context.RootAdmission) || admitted.WorkContext().Err() != nil || m.resetExecutionFenced {
		return fmt.Errorf("%w: channel target runtime ownership changed at publication", channelonboarding.ErrRevisionConflict)
	}
	return admitted.Runtime().ValidateStandingIngressCredentials(admitted.WorkContext())
}

func validateTargetsGeneration(contextDef BundleContext, generation triggergeneration.Generation) error {
	if (len(contextDef.StandingTargets) > 0 || len(contextDef.InstalledTriggerSubjects) > 0) && !generation.Valid() {
		return fmt.Errorf("runtime context %s provider-trigger catalog generation is required", contextDef.BundleHash())
	}
	for _, target := range contextDef.StandingTargets {
		if err := validateStandingTargetDeclarationOwner(&contextDef, contextDef.Runtime, target); err != nil {
			return err
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, lookup, err := m.runContextEntryLocked(availability)
	return lookup, availability, err
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
	if admission := m.pendingSourceSetTransition; admission != nil {
		select {
		case <-admission.TerminalDrain():
			return nil, errors.New("runtime source-set transition is terminally draining")
		default:
		}
	}
	wanted := make(map[string]runtimeagenttopology.SourceCoordinate, len(plan.Sources))
	for _, source := range plan.Sources {
		wanted[source.Normalize().Key()] = source.Normalize()
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
		if err := validateRuntimeContextSourceSetMember(entry.context, wanted); err != nil {
			m.mu.Unlock()
			return nil, err
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

// Fork preparation and selection are exclusive with inbound publication.
func (m *RuntimeContextManager) BeginLiveRunSelection() func() {
	m.liveSelectionMu.Lock()
	return m.liveSelectionMu.Unlock
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, lookup, err := m.runContextEntryLocked(availability)
	if err != nil || !lookup.Loaded() {
		return nil, lookup, availability, err
	}
	use, err := m.acquireEntryLocked(ctx, entry)
	return use, lookup, availability, err
}

func (m *RuntimeContextManager) AcquireIngress(ctx context.Context, alias, provider string) (*RuntimeContextUse, RuntimeIngressContextLookup, error) {
	if m == nil {
		return nil, RuntimeIngressContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded}, nil
	}
	m.liveSelectionMu.RLock()
	releaseSelection := true
	defer func() {
		if releaseSelection {
			m.liveSelectionMu.RUnlock()
		}
	}()
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
			lookup.authorIngress = declaredAuthorIngress(entry.runtime, entry.context, target)
			if lookup.State == "" {
				lookup.State = RuntimeContextStateLoaded
			}
			if lookup.State != RuntimeContextStateLoaded {
				if strings.TrimSpace(lookup.Cause) == "" {
					lookup.Cause = RuntimeContextCauseUnavailable
				}
				return nil, lookup, nil
			}
			lookup.Context = runtimeContextMetadata(entry.context)
			if m.landingRuns == nil {
				return nil, lookup, fmt.Errorf("inbound requires the deployment landing-run reader")
			}
			landing, found, err := m.landingRuns.LoadLandingRun(ctx, runtimerunlifecycle.LiveRunScope{FlowPath: target.FlowPath})
			if err != nil {
				return nil, lookup, err
			}
			if !found || !landing.Source.Matches(entry.context.SourceArtifactFact) ||
				(landing.State != runtimerunlifecycle.StateLive && landing.State != runtimerunlifecycle.StatePaused) {
				lookup.State, lookup.Cause = RuntimeContextStateUnloaded, RuntimeContextCauseUnavailable
				return nil, lookup, nil
			}
			lookup.Target.RunID = landing.RunID
			use, err := m.acquireEntryLocked(ctx, entry)
			if err != nil {
				return nil, lookup, err
			}
			use.Context.StandingTargets = append([]StandingTarget(nil), use.Context.StandingTargets...)
			for i, owned := range use.Context.StandingTargets {
				if owned.Alias == target.Alias && owned.Provider == target.Provider {
					use.Context.StandingTargets[i] = lookup.Target
				}
			}
			priorRelease := use.releaseAdmission
			use.releaseAdmission = func() {
				if priorRelease != nil {
					priorRelease()
				}
				m.liveSelectionMu.RUnlock()
			}
			releaseSelection = false
			return use, lookup, nil
		}
	}
	return nil, RuntimeIngressContextLookup{State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseNotLoaded, AliasFound: aliasFound}, nil
}

func (m *RuntimeContextManager) DeactivateBundleHash(bundleHash, cause string) RuntimeContextDeactivationResult {
	return m.DeactivateBundleHashWithOptions(bundleHash, cause, DefaultShutdownOptions())
}

func (m *RuntimeContextManager) DeactivateBundleHashWithOptions(bundleHash, cause string, opts ShutdownOptions) RuntimeContextDeactivationResult {
	if m == nil {
		return RuntimeContextDeactivationResult{BundleHash: strings.TrimSpace(bundleHash), State: RuntimeContextStateUnloaded, Cause: normalizeRuntimeContextDeactivationCause(cause)}
	}
	m.sourceSetMu.Lock()
	if admission := m.pendingSourceSetTransition; admission != nil {
		select {
		case <-admission.TerminalDrain():
		default:
			m.sourceSetMu.Unlock()
			return RuntimeContextDeactivationResult{BundleHash: strings.TrimSpace(bundleHash), State: RuntimeContextStateUnloaded, Cause: RuntimeContextCauseSourceSetTransition,
				ShutdownErr: errors.New("pending runtime source-set transition requires aggregate shutdown")}
		}
	}
	// Withdrawal and admission fencing serialize with source-set preparation.
	// Retirement joins below cannot retain that lock.
	return m.deactivateBundleHashWithOptions(bundleHash, cause, opts, m.sourceSetMu.Unlock)
}

func (m *RuntimeContextManager) deactivateBundleHashWithOptions(bundleHash, cause string, opts ShutdownOptions, releasePreparation func()) RuntimeContextDeactivationResult {
	defer func() {
		if releasePreparation != nil {
			releasePreparation()
		}
	}()
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
	}
	if entry.context != nil {
		runtimeToShutdown = entry.runtime
		if runtimeToShutdown != nil {
			runtimeToShutdown.CloseAdmission()
			if runtimeToShutdown.shutdownGate.closedAndDrained() {
				runtimeToShutdown.deliveryContinuations.BeginRetirement()
			}
		}
	}
	m.mu.Unlock()
	if releasePreparation != nil {
		releasePreparation()
		releasePreparation = nil
	}
	if runtimeToShutdown == nil {
		return result
	}
	entry.shutdownMu.Lock()
	defer entry.shutdownMu.Unlock()
	if entry.shutdownComplete {
		return result
	}
	remaining, drainErr := runtimeToShutdown.drainIngressForContextWithdrawal(opts)
	result.ShutdownErr = errors.Join(result.ShutdownErr, drainErr)
	if remaining.Grace <= 0 {
		// Even invalid shutdown options must join admitted producers.
		result.ShutdownErr = errors.Join(result.ShutdownErr, runtimeToShutdown.drainAdmittedIngress(context.Background(), 0))
	}
	// Return continuation carriers before joining their runtime occurrence.
	if runtimeToShutdown.deliveryContinuations != nil {
		result.ShutdownErr = errors.Join(result.ShutdownErr, runtimeToShutdown.deliveryContinuations.Retire(context.Background()))
	}
	if remaining.Grace <= 0 {
		return result
	}
	result.ShutdownErr = errors.Join(result.ShutdownErr, runtimeToShutdown.ShutdownWithOptions(remaining))
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
	m.sourceSetMu.Lock()
	m.mu.Lock()
	hashes := append([]string(nil), m.order...)
	updates := make([]runtimeContextVisibilityUpdate, 0, len(hashes))
	changed := make(map[string]bool, len(hashes))
	for _, hash := range hashes {
		entry := m.contexts[hash]
		if runtimeContextEntryLoaded(entry) {
			updates = append(updates, runtimeContextVisibilityUpdate{entry: entry, state: RuntimeContextStateUnloaded, cause: normalizeRuntimeContextDeactivationCause(cause)})
			changed[hash] = true
		}
	}
	if err := m.publishRuntimeContextVisibilityLocked(updates...); err != nil {
		m.mu.Unlock()
		m.sourceSetMu.Unlock()
		return []RuntimeContextDeactivationResult{{ShutdownErr: err}}
	}
	// Withdraw the whole selectable set and fence every occurrence before the
	// first join. Draining one context must not leave a sibling admitting work.
	for _, hash := range hashes {
		entry := m.contexts[hash]
		if entry == nil {
			continue
		}
		if entry.runtime != nil {
			entry.runtime.CloseAdmission()
			if entry.runtime.shutdownGate.closedAndDrained() {
				entry.runtime.deliveryContinuations.BeginRetirement()
			}
		}
	}
	m.mu.Unlock()
	if m.pendingSourceSetTransition != nil {
		m.pendingSourceSetTransition.terminate()
	}
	m.sourceSetMu.Unlock()
	results := make([]RuntimeContextDeactivationResult, 0, len(hashes))
	for _, bundleHash := range hashes {
		result := m.deactivateBundleHashWithOptions(bundleHash, cause, opts, nil)
		result.Changed = result.Changed || changed[bundleHash]
		results = append(results, result)
	}
	return results
}

func normalizeRuntimeContextDeactivationCause(cause string) string {
	if cause = strings.TrimSpace(cause); cause != "" {
		return cause
	}
	return RuntimeContextCauseUnavailable
}
