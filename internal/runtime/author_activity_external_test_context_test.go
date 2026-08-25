package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

func testLiveExecutionContext(ctx context.Context) context.Context {
	return runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
}

var authorActivityTestBundleSourceFact = mustExternalTestBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("e", 64))

var externalRuntimeTestEventBusOwners sync.Map

type externalRuntimeTestEventBusOwner struct {
	process       *worklifetime.Process
	occurrence    worklifetime.Occurrence
	retireAndWait func(context.Context) error
	once          sync.Once
	err           error
}

func (o *externalRuntimeTestEventBusOwner) close(ctx context.Context) error {
	if o == nil || o.process == nil {
		return nil
	}
	o.once.Do(func() {
		if err := o.retireAndWait(ctx); err != nil {
			o.err = fmt.Errorf("retire external runtime test occurrence: %w", err)
			return
		}
		o.process.Retire()
		if _, err := o.process.Join(ctx); err != nil {
			o.err = fmt.Errorf("join external runtime test process: %w", err)
		}
	})
	return o.err
}

type externalRuntimeTestCapability interface {
	Release(context.Context) error
}

type externalRuntimeTestReleaseProbe struct {
	released chan struct{}
	once     sync.Once
}

func (p *externalRuntimeTestReleaseProbe) Release(context.Context) error {
	p.once.Do(func() { close(p.released) })
	return nil
}

func TestExternalRuntimeTestGenerationJoinsAcceptedWorkBeforeCapabilityRelease(t *testing.T) {
	process := worklifetime.NewProcess()
	lease, err := process.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin accepted process work: %v", err)
	}
	capability := &externalRuntimeTestReleaseProbe{released: make(chan struct{})}
	closed := make(chan error, 1)
	go func() {
		closed <- closeExternalRuntimeTestGeneration(nil, process, capability)
	}()

	<-lease.Context().Done()
	select {
	case <-capability.released:
		t.Fatal("process capability released before accepted work settled")
	default:
	}
	if err := lease.Done(); err != nil {
		t.Fatalf("settle accepted process work: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("close external runtime test generation: %v", err)
	}
	select {
	case <-capability.released:
	default:
		t.Fatal("process capability was not released after process join")
	}
}

func closeExternalRuntimeTestGeneration(runtime *runtimepkg.Runtime, process *worklifetime.Process, capability externalRuntimeTestCapability) error {
	if runtime != nil {
		if err := runtime.Shutdown(); err != nil {
			return fmt.Errorf("shutdown external runtime test generation: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if process != nil {
		process.Retire()
		if _, err := process.Join(ctx); err != nil {
			return fmt.Errorf("join external runtime test process: %w", err)
		}
	}
	if capability != nil {
		if err := capability.Release(context.Background()); err != nil {
			return fmt.Errorf("release external runtime test process capability: %w", err)
		}
	}
	return nil
}

func closeExternalManagerTestGeneration(manager *runtimemanager.AgentManager, bus *runtimebus.EventBus, grant runtimestartupownership.GenerationGrant, capability externalRuntimeTestCapability) error {
	if manager != nil {
		if err := manager.Shutdown(); err != nil {
			return fmt.Errorf("shutdown external runtime test manager: %w", err)
		}
	}
	if bus != nil {
		if err := bus.ResetInMemoryState(); err != nil {
			return fmt.Errorf("reset external runtime test event bus: %w", err)
		}
		if owner, ok := externalRuntimeTestEventBusOwners.Load(bus); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := owner.(*externalRuntimeTestEventBusOwner).close(ctx); err != nil {
				return err
			}
		}
	}
	if grant != nil {
		if err := grant.Retire(context.Background()); err != nil {
			return fmt.Errorf("retire external manager test generation grant: %w", err)
		}
	}
	if capability != nil {
		if err := capability.Release(context.Background()); err != nil {
			return fmt.Errorf("release external manager test process capability: %w", err)
		}
	}
	return nil
}

type externalTestFlowInstanceActivationOwner struct {
	mu       sync.Mutex
	activate runtimepipeline.FlowInstanceActivator
	pending  map[runtimeflowidentity.Route]runtimepipeline.FlowInstanceActivationRequest
}

func newExternalTestFlowInstanceActivationOwner(activate runtimepipeline.FlowInstanceActivator) *externalTestFlowInstanceActivationOwner {
	return &externalTestFlowInstanceActivationOwner{
		activate: activate,
		pending:  make(map[runtimeflowidentity.Route]runtimepipeline.FlowInstanceActivationRequest),
	}
}

func (o *externalTestFlowInstanceActivationOwner) PrepareFlowInstanceActivation(_ context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
	if req.OccurredAt.IsZero() {
		req.OccurredAt = req.TriggerEvent.CreatedAt()
	}
	if strings.TrimSpace(req.InitialState) == "" {
		if schema, ok := req.ContractBundle.FlowSchemaByID(req.Instance.TemplateID); ok {
			req.InitialState = schema.LoweredInitialState()
		}
	}
	if strings.TrimSpace(req.InitialState) == "" {
		req.InitialState = "pending"
	}
	fields := make(map[string]any, len(req.Fields))
	for key, value := range req.Fields {
		fields[key] = value
	}
	parentRoute := req.Instance.ParentRoute.Normalized()
	bundleHash, bundleSource := authorActivityTestBundleSourceFact.StorageValues()
	readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: req.Instance, RunID: req.TriggerEvent.RunID(),
		BundleHash: bundleHash, BundleSource: bundleSource,
		WorkflowVersion: req.ContractBundle.WorkflowVersion(), ExecutionMode: "live",
	}
	instance := runtimepipeline.WorkflowInstance{
		InstanceID: req.Instance.InstanceID, StorageRef: req.Instance.InstancePath, EntityID: req.Instance.EntityID,
		ParentFlowID: parentRoute.FlowID, ParentFlowInstance: parentRoute.FlowInstance, ParentEntityID: req.Instance.ParentEntityID,
		WorkflowName: req.Instance.TemplateID, WorkflowVersion: req.ContractBundle.WorkflowVersion(),
		CurrentState: req.InitialState, Config: req.Config, Fields: fields, Bookkeeping: req.Bookkeeping,
		EnteredStageAt: req.OccurredAt, CreatedAt: req.OccurredAt, RuntimeReadiness: &readiness,
		EntityType: "test_entity",
	}
	plan := runtimepipeline.FlowInstanceActivationPlan{
		Instance: instance, Identity: req.Instance, Readiness: readiness, OccurredAt: req.OccurredAt,
	}
	if err := plan.Validate(); err != nil {
		return runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	o.mu.Lock()
	o.pending[req.Instance.Route()] = req
	o.mu.Unlock()
	return plan, nil
}

func (o *externalTestFlowInstanceActivationOwner) FinalizeCommittedFlowInstanceActivation(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
	plan := committed.Plan
	o.mu.Lock()
	req, ok := o.pending[plan.Identity.Route()]
	if ok {
		delete(o.pending, plan.Identity.Route())
	}
	o.mu.Unlock()
	if !ok || o.activate == nil {
		return nil
	}
	return o.activate(ctx, req)
}

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type externalRuntimeTestWorkflowOwner interface {
	runtimebus.TargetFailureDeadLetterRecorder
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry
}

func newExternalRuntimeTestPipelineCoordinator(
	t testing.TB,
	bus *runtimebus.EventBus,
	db *sql.DB,
	selected any,
	opts runtimepipeline.PipelineCoordinatorOptions,
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	owner, ok := selected.(externalRuntimeTestWorkflowOwner)
	if !ok {
		t.Fatalf("selected workflow test owner %T lacks exact decision and expiry roles", selected)
	}
	if opts.DecisionCards == nil {
		opts.DecisionCards = owner
	}
	if opts.DeadLetters == nil {
		opts.DeadLetters = owner
	}
	if opts.ProposedEffects == nil {
		opts.ProposedEffects = owner
	}
	if opts.HumanTasks == nil {
		opts.HumanTasks = owner
	}
	if opts.DecisionCardDraftExpiry == nil {
		opts.DecisionCardDraftExpiry = owner
	}
	if opts.HumanTaskExpiry == nil {
		opts.HumanTaskExpiry = owner
	}
	if opts.DeliveryRuntime == nil {
		opts.DeliveryRuntime = bus
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, opts)
	if coordinator == nil {
		t.Fatal("construct durable pipeline coordinator with complete workflow owners")
	}
	return coordinator
}

func completeExternalRuntimeTestWorkflowDeps(t testing.TB, selected any, deps runtimepkg.RuntimeDeps) runtimepkg.RuntimeDeps {
	t.Helper()
	if deps.Config != nil && !deps.Config.Runtime.ExecutionPosture.Valid() {
		deps.Config.Runtime.ExecutionPosture = executionposture.Live
	}
	owner, ok := selected.(externalRuntimeTestWorkflowOwner)
	if !ok {
		t.Fatalf("selected workflow test owner %T lacks exact decision and expiry roles", selected)
	}
	if deps.DecisionCards == nil {
		deps.DecisionCards = owner
	}
	if deps.ProposedEffects == nil {
		deps.ProposedEffects = owner
	}
	if deps.DecisionCardHumanTasks == nil {
		deps.DecisionCardHumanTasks = owner
	}
	if deps.DecisionCardDraftExpiry == nil {
		deps.DecisionCardDraftExpiry = owner
	}
	if deps.HumanTaskExpiry == nil {
		deps.HumanTaskExpiry = owner
	}
	return deps
}

func installExternalRuntimeTestGeneration(
	t testing.TB,
	ctx context.Context,
	selected any,
	runtime *runtimepkg.Runtime,
) (runtimestartupownership.ProcessCapability, runtimestartupownership.GenerationGrant) {
	t.Helper()
	store, ok := selected.(runtimestartupownership.Store)
	if !ok {
		t.Fatalf("selected runtime test owner %T lacks process capability acquisition", selected)
	}
	if runtime == nil || runtime.Manager == nil || runtime.Options.WorkflowModule == nil {
		t.Fatal("external runtime test generation requires a constructed runtime and semantic source")
	}
	bundleHash, bundleSource := runtime.Options.BundleSourceFact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	desired, err := runtime.Manager.CompileStaticTopologyDesiredAgents(runtime.Options.WorkflowModule.SemanticSource(), coordinate)
	if err != nil {
		t.Fatalf("compile external runtime test source set: %v", err)
	}
	capability, err := store.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "external-runtime-test", BootID: uuid.NewString(), RuntimeInstanceID: runtime.Options.RuntimeInstanceID,
	})
	if err != nil {
		t.Fatalf("acquire external runtime test process capability: %v", err)
	}
	current, exists, err := capability.CurrentSourceSet(ctx)
	if err != nil {
		_ = capability.Release(context.Background())
		t.Fatalf("load external runtime test source set: %v", err)
	}
	sources := make([]runtimeagenttopology.SourceCoordinate, 0, len(current.Sources)+1)
	for _, source := range current.Sources {
		if source.Normalize().Key() != coordinate.Normalize().Key() {
			sources = append(sources, source)
		}
	}
	sources = append(sources, coordinate)
	agents := make([]runtimeagenttopology.DesiredAgent, 0, len(current.Agents)+len(desired))
	for _, agent := range current.Agents {
		if agent.Source.Normalize().Key() != coordinate.Normalize().Key() {
			agents = append(agents, agent)
		}
	}
	agents = append(agents, desired...)
	plan, err := runtimeagenttopology.NewSourceSetPlan(sources, agents)
	if err != nil {
		_ = capability.Release(context.Background())
		t.Fatalf("construct external runtime test source set: %v", err)
	}
	commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
	if exists {
		commit.ExpectedRevision = current.Revision
		_, err = capability.ReplaceSourceSet(ctx, commit)
	} else {
		_, err = capability.InstallCompleteSourceSet(ctx, commit)
	}
	if err != nil {
		_ = capability.Release(context.Background())
		t.Fatalf("commit external runtime test source set: %v", err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: bundleHash, BundleSource: bundleSource,
		RuntimeInstanceID: runtime.Options.RuntimeInstanceID, RuntimeGeneration: 1,
		SourceSetRevision: plan.Revision,
	})
	if err != nil {
		_ = capability.Release(context.Background())
		t.Fatalf("issue external runtime test generation grant: %v", err)
	}
	if err := runtime.InstallStartupGrant(grant); err != nil {
		_ = capability.Release(context.Background())
		t.Fatalf("install external runtime test generation grant: %v", err)
	}
	return capability, grant
}

func installExternalManagerTestGeneration(
	t testing.TB,
	ctx context.Context,
	manager *runtimemanager.AgentManager,
	grant runtimestartupownership.GenerationGrant,
) {
	t.Helper()
	if manager == nil || grant == nil {
		t.Fatal("external manager test generation requires a manager and generation grant")
	}
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatalf("load external manager test grant evidence: %v", err)
	}
	plan, err := grant.SourceSetPlan(ctx)
	if err != nil {
		t.Fatalf("load external manager test source set: %v", err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(
		evidence.SourceSetRevision,
		evidence.BundleHash,
		evidence.BundleSource,
		runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatalf("construct external manager test topology admission: %v", err)
	}
	if err := manager.InstallStartupTopology(grant, admission, plan); err != nil {
		t.Fatalf("install external manager test generation: %v", err)
	}
}

type externalRuntimeTestDurableEventStore interface {
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
}

func externalRuntimeTestDurableDependencies(durable externalRuntimeTestDurableEventStore) runtimebus.DurableDependencies {
	return runtimebus.DurableDependencies{
		ReplyContext: durable, RunLifecycle: durable,
		DeliveryLifecycle: durable, FlowRoutes: durable, FlowRouteRecords: durable,
		FlowRouteSets: durable, FlowRouteTopology: durable, FlowRouteRollback: durable, ActiveAgents: durable,
		ActiveFlows: durable, TargetOwners: durable, WorkflowInstances: durable, PreparedEvents: durable,
		TargetFailureRecorder: durable, RunOrigins: durable,
	}
}

func externalRuntimeTestManagerBusRoles(bus *runtimebus.EventBus) runtimemanager.PersistenceRoles {
	return runtimemanager.PersistenceRoles{
		AgentRoutes: bus, RouteInstaller: bus, RouteVerifier: bus,
		RouteRestorer: bus, RouteRetirer: bus, RouteRemover: bus,
		FlowActivation: bus, CreationPublisher: bus, DeliveryRuntime: bus,
	}
}

func externalRuntimeTestSelectedManagerRoles(selected any) runtimemanager.PersistenceRoles {
	var roles runtimemanager.PersistenceRoles
	roles.LifecycleCensus, _ = selected.(runtimemanager.AgentLifecycleCellCensus)
	roles.LifecycleState, _ = selected.(runtimemanager.AgentLifecycleStateReader)
	roles.LifecycleEffects, _ = selected.(runtimeeffects.Store)
	roles.LifecycleDiagnostics, _ = selected.(runtimemanager.AgentLifecycleDiagnosticPersistence)
	roles.EffectsRecovery, _ = selected.(runtimeeffects.RecoveryStore)
	roles.DeliveryQuiescence, _ = selected.(runtimemanager.ActiveRunDeliveryQuiescenceReader)
	roles.EventExistence, _ = selected.(runtimemanager.EventExistenceReader)
	roles.DirectiveOperations, _ = selected.(runtimeagentcontrol.DirectiveOperationStore)
	roles.DirectiveTargets, _ = selected.(runtimemanager.AgentDirectiveRunTargetResolver)
	roles.FlowRoutes, _ = selected.(runtimebus.FlowInstanceRoutePersistence)
	return roles
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, authorActivityTestBundleSourceFact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash(),
	))
}

func newScopedTestEventBus(t *testing.T, store runtimebus.EventStore, opts runtimebus.EventBusOptions, differentEvents ...string) (*runtimebus.EventBus, error) {
	t.Helper()
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.BundleSourceFact.Validate() != nil {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.TemplateInstanceActivator != nil && opts.TemplateInstancePlanner == nil {
		owner := newExternalTestFlowInstanceActivationOwner(opts.TemplateInstanceActivator)
		opts.TemplateInstancePlanner = owner
		opts.FlowActivationFinalizer = owner
	}
	if opts.FlowActivationFinalizer == nil {
		opts.FlowActivationFinalizer, _ = opts.TemplateInstancePlanner.(runtimepipeline.CommittedFlowInstanceActivationFinalizer)
	}

	if registrar, ok := store.(testAuthorActivityCatalogRegistrar); ok {
		descriptors := testAuthorActivityEventDescriptors(t, opts)
		for _, eventType := range differentEvents {
			descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
				EventType: strings.TrimSpace(eventType), Disposition: runtimeauthoractivity.StoryDifferent,
			})
		}
		lease, err := registrar.RegisterAuthorActivityEventCatalog(
			runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.BundleSourceFact.BundleHash()),
			descriptors,
		)
		if err != nil {
			return nil, err
		}
		t.Cleanup(lease.Release)
	}
	return newRuntimeTestEventBusWithOptions(t, store, opts)
}

func newRuntimeTestEventBus(t testing.TB, store runtimebus.EventStore) (*runtimebus.EventBus, error) {
	t.Helper()
	return newRuntimeTestEventBusWithOptions(t, store, runtimebus.EventBusOptions{})
}

func newRuntimeTestEventBusWithOptions(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	var lifetimeOwner *externalRuntimeTestEventBusOwner
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.BundleSourceFact.Validate() != nil {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.WorkOwner == nil {
		process := worklifetime.NewProcess()
		owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
			RuntimeInstanceID: opts.RuntimeInstanceID,
			BundleHash:        opts.BundleSourceFact.BundleHash(),
		})
		if err != nil {
			return nil, err
		}
		opts.WorkOwner = owner
		lifetimeOwner = &externalRuntimeTestEventBusOwner{
			process: process, occurrence: owner,
			retireAndWait: func(ctx context.Context) error {
				_, err := owner.RetireAndWait(ctx)
				return err
			},
		}
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
		if opts.Durable.FlowRouteTopology == nil {
			durable, ok := store.(externalRuntimeTestDurableEventStore)
			if !ok {
				return nil, fmt.Errorf("external runtime durable event-store fixture %T lacks exact durable roles", store)
			}
			opts.Durable = externalRuntimeTestDurableDependencies(durable)
		}
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
	owner := &externalRuntimeTestEventBusOwner{occurrence: opts.WorkOwner}
	if lifetimeOwner != nil {
		owner = lifetimeOwner
	}
	externalRuntimeTestEventBusOwners.Store(bus, owner)
	t.Cleanup(func() {
		defer externalRuntimeTestEventBusOwners.Delete(bus)
		if err := bus.ResetInMemoryState(); err != nil {
			t.Errorf("reset external runtime test event bus: %v", err)
			return
		}
		if lifetimeOwner != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := lifetimeOwner.close(ctx); err != nil {
				t.Errorf("close external runtime test event bus owner: %v", err)
			}
		}
	})
	return bus, nil
}

func testBundleSourceFact(t testing.TB, bundleHash string) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(strings.TrimSpace(bundleHash))
	if err != nil {
		t.Fatalf("construct external test bundle source fact: %v", err)
	}
	return fact
}

func mustExternalTestBundleSourceFact(bundleHash string) runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(strings.TrimSpace(bundleHash))
	if err != nil {
		panic(err)
	}
	return fact
}

func runtimeTestEventBusWorkOwner(t testing.TB, bus *runtimebus.EventBus) worklifetime.Occurrence {
	t.Helper()
	owner, ok := externalRuntimeTestEventBusOwners.Load(bus)
	if !ok {
		t.Fatal("external runtime test event bus has no registered work owner")
	}
	return owner.(*externalRuntimeTestEventBusOwner).occurrence
}

func ownRuntimeTestAgentManager(t testing.TB, manager *runtimemanager.AgentManager) *runtimemanager.AgentManager {
	t.Helper()
	t.Cleanup(func() {
		if err := manager.Shutdown(); err != nil {
			t.Errorf("shutdown external runtime test manager: %v", err)
		}
	})
	return manager
}

func testAuthorActivityEventDescriptors(t *testing.T, opts runtimebus.EventBusOptions) []runtimeauthoractivity.EventDescriptor {
	t.Helper()
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(opts.ContractBundle)
	if err != nil {
		t.Fatalf("project author activity event descriptors: %v", err)
	}
	return descriptors
}
