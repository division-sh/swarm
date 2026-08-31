package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const runtimeTestBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

type runtimeTestWorkFixture struct {
	process *worklifetime.Process
}

var runtimeTestWorkFixtures sync.Map
var runtimeTestEventBusOwners sync.Map

type runtimeTestCandidateOwner struct{}

type runtimeTestCandidateRegistration struct{}

type runtimeTestUnavailableDecisionCards struct{ decisioncard.Store }

type runtimeTestRetainedSession struct {
	mu              sync.Mutex
	authority       runtimestartupownership.Authority
	plan            runtimeagenttopology.SourceSetPlan
	agents          map[string]runtimemanager.PersistedAgent
	callback        func(runtimestartupownership.TerminalResult)
	grantTransition func(*runtimestartupownership.GrantEvidence, runtimestartupownership.GrantEvidence)
	released        bool
}

type runtimeTestRetainedSessionProvider interface {
	runtimeTestStartupSession() *runtimeTestRetainedSession
}

type runtimeTestProcessBindingSeeder interface {
	seedRuntimeTestProcessBinding(runtimemanager.ProcessExecutionBinding)
}

func (s *runtimeTestRetainedSession) Authority() (runtimestartupownership.Authority, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return runtimestartupownership.Authority{}, errors.New("runtime test retained session is released")
	}
	return s.authority, nil
}

func (s *runtimeTestRetainedSession) ProveCurrent(context.Context) error {
	_, err := s.Authority()
	return err
}

func (s *runtimeTestRetainedSession) MonitorProveCurrent(ctx context.Context, _ time.Duration) error {
	return s.ProveCurrent(ctx)
}

func (s *runtimeTestRetainedSession) InstallTerminalOwner(owner runtimestartupownership.SessionTerminalOwner, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.callback != nil || owner == nil {
		return errors.New("runtime test retained session terminal callback is invalid")
	}
	s.callback = owner.SelectedStoreSessionTerminal
	return nil
}

func (s *runtimeTestRetainedSession) RecordGenerationGrantTransition(_ context.Context, previous *runtimestartupownership.GrantEvidence, next runtimestartupownership.GrantEvidence) error {
	s.mu.Lock()
	observe := s.grantTransition
	s.mu.Unlock()
	if observe != nil {
		observe(previous, next)
	}
	return nil
}

func (s *runtimeTestRetainedSession) LoadSourceSet(context.Context) (runtimeagenttopology.SourceSetPlan, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.plan, s.plan.Revision != "", nil
}

func (s *runtimeTestRetainedSession) CommitSourceSet(_ context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.plan.Revision
	s.plan = req.Plan
	return runtimeagenttopology.SourceSetCommitResult{Operation: req.Operation, OperationID: req.OperationID, PreviousRevision: previous, CurrentRevision: req.Plan.Revision}, nil
}

func (*runtimeTestRetainedSession) ApplyBundleDeleteFinalMutation(context.Context, runtimebundledelete.FinalMutationRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimebundledelete.FinalMutationResult, error) {
	return runtimebundledelete.FinalMutationResult{}, errors.New("runtime test retained session does not own bundle deletion")
}

func (*runtimeTestRetainedSession) ReplayBundleDeleteResult(context.Context, runtimebundledelete.FinalMutationRequest) (runtimebundledelete.Result, error) {
	return runtimebundledelete.Result{}, errors.New("runtime test retained session does not own bundle deletion replay")
}

func (*runtimeTestRetainedSession) ApplyDestructiveResetCleanup(context.Context, runtimedestructivereset.CleanupRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error) {
	return runtimedestructivereset.CleanupResult{}, errors.New("runtime test retained session does not own destructive reset")
}

func (s *runtimeTestRetainedSession) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	result := runtimemanager.AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: uuid.NewString(), Identity: req.Identity, AgentID: req.AgentID,
		PreviousEpoch: req.ExpectedEpoch, RuntimeEpoch: req.TargetEpoch, PreviousGeneration: req.ExpectedGeneration,
		Generation: req.TargetGeneration, PreviousPhase: req.ExpectedPhase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode, Topology: req.Topology,
		ProcessBinding: req.ProcessBinding,
	}
	key, err := req.Identity.Normalize().Fingerprint()
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.agents == nil {
		s.agents = map[string]runtimemanager.PersistedAgent{}
	}
	rec, exists := s.agents[key]
	if req.Agent != nil {
		rec = *req.Agent
		exists = true
	}
	if !exists {
		return runtimemanager.AgentLifecycleTransitionResult{}, errors.New("runtime test lifecycle transition has no agent projection")
	}
	rec.Topology = req.Topology
	rec.LifecycleEpoch = req.TargetEpoch
	rec.LifecycleGeneration = req.TargetGeneration
	rec.LifecyclePhase = req.TargetPhase
	rec.LifecycleRunMode = req.RunMode
	rec.ProcessBinding = req.ProcessBinding
	if req.TargetPhase == runtimemanager.AgentLifecycleTerminated {
		rec.Status = "terminated"
	}
	s.agents[key] = rec
	return result, nil
}

func (s *runtimeTestRetainedSession) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]runtimemanager.PersistedAgent, 0, len(s.agents))
	for _, rec := range s.agents {
		if rec.LifecyclePhase == runtimemanager.AgentLifecycleTerminated {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func (s *runtimeTestRetainedSession) LoadAgentLifecycleState(_ context.Context, identity runtimeagentidentity.Identity) (runtimemanager.AgentLifecycleState, bool, error) {
	key, err := identity.Normalize().Fingerprint()
	if err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.agents[key]
	if !ok {
		return runtimemanager.AgentLifecycleState{}, false, nil
	}
	return runtimemanager.AgentLifecycleState{
		Identity: identity.Normalize(), AgentID: identity.AgentID(), RuntimeEpoch: rec.LifecycleEpoch,
		Generation: rec.LifecycleGeneration, Phase: rec.LifecyclePhase,
		RunMode: rec.LifecycleRunMode, Topology: rec.Topology, ProcessBinding: rec.ProcessBinding,
	}, true, nil
}

func (s *runtimeTestRetainedSession) ListDurableAgentLifecycleStates(context.Context) ([]runtimemanager.AgentLifecycleState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	agents := make([]runtimemanager.PersistedAgent, 0, len(s.agents))
	for _, rec := range s.agents {
		agents = append(agents, rec)
	}
	return runtimeTestDurableAgentLifecycleStates(agents)
}

func runtimeTestDurableAgentLifecycleStates(agents []runtimemanager.PersistedAgent) ([]runtimemanager.AgentLifecycleState, error) {
	states := make([]runtimemanager.AgentLifecycleState, 0, len(agents))
	for _, rec := range agents {
		if rec.Topology.Lifetime != runtimeagenttopology.LifetimeDurableManaged {
			continue
		}
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return nil, err
		}
		configRevision, err := canonicaljson.Hash(rec.Config)
		if err != nil {
			return nil, err
		}
		states = append(states, runtimemanager.AgentLifecycleState{
			Identity: identity.Normalize(), AgentID: identity.AgentID(), RuntimeEpoch: rec.LifecycleEpoch,
			Generation: rec.LifecycleGeneration, Phase: rec.LifecyclePhase,
			ConfigRevision: strings.TrimPrefix(configRevision, "sha256:"), RunMode: rec.LifecycleRunMode,
			Topology: rec.Topology, ProcessBinding: rec.ProcessBinding,
		})
	}
	return states, nil
}

func (s *runtimeTestRetainedSession) Release(context.Context) error {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	s.released = true
	callback := s.callback
	s.mu.Unlock()
	if callback != nil {
		callback(runtimestartupownership.TerminalResult{Cause: runtimestartupownership.TerminalOwnershipUnprovable})
	}
	return nil
}

func newRuntimeTestProcessCapability(t testing.TB, manager *runtimemanager.AgentManager, source semanticview.Source, fact runtimecorrelation.BundleSourceFact, runtimeInstanceID string) (runtimestartupownership.ProcessCapability, runtimestartupownership.GenerationGrant, error) {
	return newRuntimeTestProcessCapabilityWithSession(t, manager, source, fact, runtimeInstanceID, nil)
}

func newRuntimeTestProcessCapabilityWithSession(t testing.TB, manager *runtimemanager.AgentManager, source semanticview.Source, fact runtimecorrelation.BundleSourceFact, runtimeInstanceID string, session *runtimeTestRetainedSession) (runtimestartupownership.ProcessCapability, runtimestartupownership.GenerationGrant, error) {
	t.Helper()
	bundleHash, bundleSource := fact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	var desired []runtimeagenttopology.DesiredAgent
	if source != nil {
		if manager == nil {
			return nil, nil, errors.New("runtime test source-set compilation requires an agent manager")
		}
		var err error
		desired, err = manager.CompileStaticTopologyDesiredAgents(source, coordinate)
		if err != nil {
			return nil, nil, err
		}
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		return nil, nil, err
	}
	if session == nil {
		authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
			OwnerID: "runtime-test-process", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
		}, "runtime_test")
		if err != nil {
			return nil, nil, err
		}
		session = &runtimeTestRetainedSession{authority: authority, agents: map[string]runtimemanager.PersistedAgent{}}
	}
	session.mu.Lock()
	session.plan = plan
	session.mu.Unlock()
	capability, err := runtimestartupownership.NewProcessCapability(session)
	if err != nil {
		return nil, nil, err
	}
	grant, err := capability.IssueGenerationGrant(context.Background(), runtimestartupownership.GrantRequest{
		BundleHash: bundleHash, BundleSource: bundleSource, RuntimeInstanceID: runtimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		_ = capability.Release(context.Background())
		return nil, nil, err
	}
	binding, err := grant.ProcessExecutionBinding()
	if err != nil {
		_ = capability.Release(context.Background())
		return nil, nil, err
	}
	session.mu.Lock()
	for key, rec := range session.agents {
		if rec.ProcessBinding.IsZero() {
			rec.ProcessBinding = binding
			session.agents[key] = rec
		}
	}
	session.mu.Unlock()
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	return capability, grant, nil
}

func installRuntimeTestManagerGeneration(t testing.TB, ctx context.Context, manager *runtimemanager.AgentManager, grant runtimestartupownership.GenerationGrant) {
	t.Helper()
	if manager == nil || grant == nil {
		t.Fatal("runtime test manager generation requires a manager and generation grant")
	}
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatalf("load runtime test generation evidence: %v", err)
	}
	plan, err := grant.SourceSetPlan(ctx)
	if err != nil {
		t.Fatalf("load runtime test generation source set: %v", err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(
		evidence.SourceSetRevision,
		evidence.BundleHash,
		evidence.BundleSource,
		runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatalf("construct runtime test static admission: %v", err)
	}
	if err := manager.InstallStartupTopology(grant, admission, plan); err != nil {
		t.Fatalf("install runtime test manager generation: %v", err)
	}
}

func registerRuntimeTestAgent(manager *runtimemanager.AgentManager, cfg runtimeactors.AgentConfig) error {
	admission, err := runtimeagenttopology.NewEphemeralAdmission(uuid.NewString(), "runtime_shard")
	if err != nil {
		return err
	}
	return manager.MaterializeAdmittedAgentForExecution(context.Background(), runtimemanager.PersistedAgent{
		Config: cfg, Topology: admission, Status: "ephemeral",
	})
}

type runtimeTestUnavailableProposedEffects struct {
	decisioncard.ProposedEffectStore
}
type runtimeTestUnavailableHumanTasks struct{ decisioncard.HumanTaskStore }
type runtimeTestUnavailableDecisionCardDraftExpiry struct{}
type runtimeTestUnavailableHumanTaskExpiry struct{}

func (*runtimeTestUnavailableDecisionCardDraftExpiry) ExpireDecisionCardInputDrafts(context.Context, time.Time) (int, error) {
	return 0, nil
}

func (*runtimeTestUnavailableHumanTaskExpiry) ExpireHumanTaskCardsInMutation(context.Context, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func (*runtimeTestUnavailableHumanTaskExpiry) ListDueHumanTaskExpiryEvents(context.Context, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func (*runtimeTestUnavailableHumanTaskExpiry) CommitHumanTaskExpirations(context.Context, runtimepipeline.HumanTaskExpiryCommand) (runtimepipeline.CommittedHumanTaskExpiry, error) {
	return runtimepipeline.CommittedHumanTaskExpiry{}, nil
}

type runtimeTestDurableEventStore interface {
	runtimebus.EventStore
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	runtimebus.FlowInstanceRoutePersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteTopologyPersistence
	runtimebus.FlowInstanceRouteRollbackPersistence
	runtimebus.ActiveAgentDescriptorLister
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.SelectedRunTargetOwnerLister
	runtimepipeline.WorkflowInstancePersistenceReader
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	runtimepipeline.StandingRestartDispositionReader
}

func runtimeTestDurableDependencies(durable runtimeTestDurableEventStore) runtimebus.DurableDependencies {
	return runtimebus.DurableDependencies{
		ReplyContext: durable, RunLifecycle: durable,
		DeliveryLifecycle: durable, FlowRoutes: durable, FlowRouteRecords: durable,
		FlowRouteSets: durable, FlowRouteTopology: durable, FlowRouteRollback: durable, ActiveAgents: durable,
		ActiveFlows: durable, TargetOwners: durable, WorkflowInstances: durable, PreparedEvents: durable,
		TargetFailureRecorder: durable, RunOrigins: durable, StandingRestarts: durable,
	}
}

func runtimeTestManagerBusRoles(bus *runtimebus.EventBus) runtimemanager.PersistenceRoles {
	return runtimemanager.PersistenceRoles{
		AgentRoutes: bus, RouteInstaller: bus, RouteVerifier: bus,
		RouteRestorer: bus, RouteRetirer: bus, RouteRemover: bus,
		FlowActivation: bus, CreationPublisher: bus, DeliveryRuntime: bus,
	}
}

func (runtimeTestCandidateOwner) ListCompletionCandidates(
	context.Context,
	runtimerunlifecycle.CandidateScope,
	runtimerunlifecycle.CandidateCursor,
	int,
) (runtimerunlifecycle.CandidatePage, error) {
	return runtimerunlifecycle.CandidatePage{Exhausted: true}, nil
}

func (runtimeTestCandidateOwner) ExecuteCompletionCandidate(
	context.Context,
	runtimerunlifecycle.Candidate,
	runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	return runtimerunlifecycle.CompletionResult{}, errors.New("unexpected runtime test completion candidate")
}

func (runtimeTestCandidateOwner) RegisterCompletionCandidateSink(
	context.Context,
	runtimerunlifecycle.CandidateScope,
	runtimerunlifecycle.CandidateSink,
) (runtimerunlifecycle.CandidateRegistration, error) {
	return runtimeTestCandidateRegistration{}, nil
}

func (runtimeTestCandidateRegistration) Release() {}

func runtimeTestProcessWorkOwner(t testing.TB) *worklifetime.Process {
	t.Helper()
	if existing, ok := runtimeTestWorkFixtures.Load(t); ok {
		return existing.(*runtimeTestWorkFixture).process
	}
	fixture := &runtimeTestWorkFixture{process: worklifetime.NewProcess()}
	actual, loaded := runtimeTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*runtimeTestWorkFixture).process
	}
	t.Cleanup(func() {
		defer runtimeTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join runtime test process owner: %v", err)
		}
	})
	return fixture.process
}

func runtimeTestOccurrence(t testing.TB, bundleHash string) *worklifetime.RuntimeOccurrence {
	t.Helper()
	owner, err := runtimeTestProcessWorkOwner(t).NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        strings.TrimSpace(bundleHash),
	})
	if err != nil {
		t.Fatalf("create runtime test occurrence: %v", err)
	}
	t.Cleanup(func() {
		owner.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("retire runtime test occurrence: %v", err)
		}
	})
	return owner
}

func newRuntimeTestEventBus(t testing.TB, store runtimebus.EventStore) (*runtimebus.EventBus, error) {
	t.Helper()
	return newRuntimeTestEventBusWithOptions(t, store, runtimebus.EventBusOptions{})
}

func newRuntimeTestEventBusWithOptions(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.BundleSourceFact.Validate() != nil {
		opts.BundleSourceFact = testBundleSourceFact(t, runtimeTestBundleHash)
	}
	if opts.PayloadAdmitter == nil && opts.ContractBundle != nil {
		opts.PayloadAdmitter = NewRuntimePayloadAdmitter(nil, opts.ContractBundle, opts.BundleSourceFact)
	}
	if opts.PayloadAdmitter == nil {
		opts.PayloadAdmitter = func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
			return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
		}
	}
	if binder, ok := store.(EventPayloadAdmissionBinder); ok {
		binder.SetEventPayloadAdmitter(opts.PayloadAdmitter)
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = runtimeTestOccurrence(t, runtimeTestBundleHash)
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, authorityErr := runtimedelivery.NewNormalExecutionAuthority(
			opts.BundleSourceFact,
			opts.RuntimeInstanceID,
			1,
		)
		if authorityErr != nil {
			return nil, authorityErr
		}
		opts.DeliveryAuthority = authority
	}
	if opts.PipelineObligations == nil {
		if provider, ok := store.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	if opts.PipelineObligations != nil {
		durable, ok := store.(runtimeTestDurableEventStore)
		if !ok {
			return nil, fmt.Errorf("runtime durable event-store fixture %T lacks exact durable roles", store)
		}
		opts.Durable = runtimeTestDurableDependencies(durable)
	}
	var bus *runtimebus.EventBus
	var err error
	if opts.PipelineObligations == nil {
		bus, err = runtimebus.NewEphemeralEventBusWithOptions(store, opts)
	} else {
		bus, err = runtimebus.NewEventBusWithOptions(store, opts)
	}
	if err != nil {
		return nil, err
	}
	if err := bus.SetDeliveryContinuationOwner(
		runtimebustest.NewDeliveryContinuationOwner(opts.PipelineObligations == nil),
	); err != nil {
		return nil, err
	}
	runtimeTestEventBusOwners.Store(bus, opts.WorkOwner)
	t.Cleanup(func() { runtimeTestEventBusOwners.Delete(bus) })
	return bus, nil
}

func runtimeTestEventBusWorkOwner(t testing.TB, bus *runtimebus.EventBus) worklifetime.Occurrence {
	t.Helper()
	owner, ok := runtimeTestEventBusOwners.Load(bus)
	if !ok {
		t.Fatal("runtime test event bus has no registered work owner")
	}
	return owner.(worklifetime.Occurrence)
}

func runtimeTestEventBusRuntimeOccurrence(t testing.TB, bus *runtimebus.EventBus) *worklifetime.RuntimeOccurrence {
	t.Helper()
	owner := runtimeTestEventBusWorkOwner(t, bus)
	runtimeOwner, ok := owner.(*worklifetime.RuntimeOccurrence)
	if !ok {
		t.Fatalf("runtime test event bus owner is %T, want runtime occurrence", owner)
	}
	return runtimeOwner
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForBundle(ctx, runtimeTestBundleHash)
}

func testAuthorActivityContextForBundle(ctx context.Context, bundleHash string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
	if err != nil {
		panic(err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		bundleHash,
	))
}

func newScopedTestRuntime(t testing.TB, ctx context.Context, deps RuntimeDeps) (*Runtime, error) {
	t.Helper()
	if deps.Options.WorkflowModule != nil {
		if bundle, ok := semanticview.Bundle(deps.Options.WorkflowModule.SemanticSource()); ok {
			admitRuntimeTestBundle(t, bundle)
		}
	}
	if deps.Config != nil && !deps.Config.Runtime.ExecutionPosture.Valid() {
		deps.Config.Runtime.ExecutionPosture = executionposture.Live
	}
	if strings.TrimSpace(deps.Options.RuntimeInstanceID) == "" {
		deps.Options.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if deps.Options.BundleSourceFact.Validate() != nil {
		deps.Options.BundleSourceFact = testBundleSourceFact(t, runtimeTestBundleHash)
	}
	if deps.Options.ProcessWorkOwner == nil {
		deps.Options.ProcessWorkOwner = runtimeTestProcessWorkOwner(t)
	}
	installTestGrant := deps.StartupGrant == nil
	var retainedSession *runtimeTestRetainedSession
	if installTestGrant {
		if provider, ok := deps.ManagerStore.(runtimeTestRetainedSessionProvider); ok {
			retainedSession = provider.runtimeTestStartupSession()
		}
	}
	if installTestGrant && retainedSession == nil && deps.ManagerStore == nil {
		authority, authorityErr := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
			OwnerID: "runtime-test-process", BootID: uuid.NewString(), RuntimeInstanceID: deps.Options.RuntimeInstanceID,
		}, "runtime_test")
		if authorityErr != nil {
			return nil, authorityErr
		}
		retainedSession = &runtimeTestRetainedSession{authority: authority, agents: map[string]runtimemanager.PersistedAgent{}}
		deps.ManagerStore = retainedSession
	}
	if retainedSession != nil {
		if deps.ManagerPersistenceRoles.LifecycleCensus == nil {
			deps.ManagerPersistenceRoles.LifecycleCensus = retainedSession
		}
		if deps.ManagerPersistenceRoles.LifecycleState == nil {
			deps.ManagerPersistenceRoles.LifecycleState = retainedSession
		}
	}
	if deps.WorkflowPersistence.Valid() {
		if deps.DecisionCards == nil {
			deps.DecisionCards = &runtimeTestUnavailableDecisionCards{}
		}
		if deps.ProposedEffects == nil {
			deps.ProposedEffects = &runtimeTestUnavailableProposedEffects{}
		}
		if deps.DecisionCardHumanTasks == nil {
			deps.DecisionCardHumanTasks = &runtimeTestUnavailableHumanTasks{}
		}
		if deps.DecisionCardDraftExpiry == nil {
			deps.DecisionCardDraftExpiry = &runtimeTestUnavailableDecisionCardDraftExpiry{}
		}
		if deps.HumanTaskExpiry == nil {
			deps.HumanTaskExpiry = &runtimeTestUnavailableHumanTaskExpiry{}
		}
	}
	if deps.WorkflowPersistence.Configured() && deps.RunLifecycleCandidates == nil {
		if candidates, ok := deps.EventStore.(runtimerunlifecycle.CandidateOwner); ok {
			deps.RunLifecycleCandidates = candidates
		} else {
			deps.RunLifecycleCandidates = runtimeTestCandidateOwner{}
		}
	}
	runtime, err := NewRuntime(ctx, deps)
	if err == nil {
		if installTestGrant {
			var source semanticview.Source
			if deps.Options.WorkflowModule != nil {
				source = deps.Options.WorkflowModule.SemanticSource()
			}
			_, grant, grantErr := newRuntimeTestProcessCapabilityWithSession(t, runtime.Manager, source, deps.Options.BundleSourceFact, deps.Options.RuntimeInstanceID, retainedSession)
			if grantErr != nil {
				return nil, grantErr
			}
			if seeder, ok := deps.ManagerStore.(runtimeTestProcessBindingSeeder); ok {
				binding, bindingErr := grant.ProcessExecutionBinding()
				if bindingErr != nil {
					return nil, bindingErr
				}
				seeder.seedRuntimeTestProcessBinding(binding)
			}
			if grantErr = runtime.InstallStartupGrant(grant); grantErr != nil {
				return nil, grantErr
			}
		}
		if runtime.Bus.DeliveryContinuationOwner() == nil {
			if ownerErr := runtime.Bus.SetDeliveryContinuationOwner(
				runtimebustest.NewDeliveryContinuationOwner(true),
			); ownerErr != nil {
				return nil, ownerErr
			}
		}
		t.Cleanup(func() {
			if shutdownErr := runtime.Shutdown(); shutdownErr != nil {
				t.Errorf("shutdown runtime test fixture: %v", shutdownErr)
			}
		})
	}
	return runtime, err
}

func admitRuntimeTestBundle(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	if bundle == nil || bundle.PackInventory == nil || bundle.PackAdmission != nil {
		return
	}
	projection, err := packadmission.Admit(bundle.PackInventory, bundle.Platform)
	if err != nil {
		t.Fatalf("admit runtime test pack projection: %v", err)
	}
	bundle.PackAdmission = projection
}

func testBundleSourceFact(t testing.TB, bundleHash string) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(strings.TrimSpace(bundleHash))
	if err != nil {
		t.Fatalf("construct test bundle source fact: %v", err)
	}
	return fact
}

func testPersistedBundleSourceFact(t testing.TB, bundleHash string) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact(strings.TrimSpace(bundleHash))
	if err != nil {
		t.Fatalf("construct persisted test bundle source fact: %v", err)
	}
	return fact
}
