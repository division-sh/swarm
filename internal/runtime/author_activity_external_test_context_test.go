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
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

func testLiveExecutionContext(ctx context.Context) context.Context {
	return runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
}

var authorActivityTestBundleSourceFact = mustExternalTestBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("e", 64))

var externalRuntimeTestEventBusOwners sync.Map

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
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
}

func externalRuntimeTestDurableDependencies(durable externalRuntimeTestDurableEventStore) runtimebus.DurableDependencies {
	return runtimebus.DurableDependencies{
		ReplyContext: durable, RunLifecycle: durable,
		DeliveryLifecycle: durable, FlowRoutes: durable, FlowRouteRecords: durable,
		FlowRouteSets: durable, FlowRouteTopology: durable, FlowRouteRollback: durable, ActiveAgents: durable,
		ActiveFlows: durable, TargetOwners: durable, PreparedEvents: durable,
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
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := owner.RetireAndWait(ctx); err != nil {
				t.Errorf("retire external runtime test occurrence: %v", err)
			}
			if _, err := process.Join(ctx); err != nil {
				t.Errorf("join external runtime test process: %v", err)
			}
		})
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
	externalRuntimeTestEventBusOwners.Store(bus, opts.WorkOwner)
	t.Cleanup(func() { externalRuntimeTestEventBusOwners.Delete(bus) })
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
	return owner.(worklifetime.Occurrence)
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
