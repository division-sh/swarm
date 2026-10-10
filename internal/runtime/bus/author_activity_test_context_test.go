package bus

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = sourceartifactfixture.BundleHash

var authorActivityTestSourceArtifactFact = sourceartifactfixture.Fact()

func exactTestFlowInstanceDescriptors(in []ActiveFlowInstanceDescriptor, workflowVersion string, sourceFact runtimecorrelation.SourceArtifactFact, runID string, sources ...semanticview.Source) []ActiveFlowInstanceDescriptor {
	if sourceFact.Validate() != nil {
		sourceFact = authorActivityTestSourceArtifactFact
	}
	bundleHash := sourceFact.BundleHash()
	out := append([]ActiveFlowInstanceDescriptor(nil), in...)
	for idx := range out {
		if strings.TrimSpace(out[idx].RunID) == "" {
			out[idx].RunID = strings.TrimSpace(runID)
		}
		if strings.TrimSpace(out[idx].FlowTemplate) == "" {
			out[idx].FlowTemplate = runtimeflowidentity.RouteForInstancePath(out[idx].FlowInstance).ScopeKey
		}
		if strings.TrimSpace(out[idx].BundleHash) == "" {
			out[idx].BundleHash = bundleHash
		}
		if strings.TrimSpace(out[idx].WorkflowVersion) == "" {
			out[idx].WorkflowVersion = strings.TrimSpace(workflowVersion)
		}
		if out[idx].Identity == (runtimeflowidentity.Instance{}) && len(sources) == 1 {
			out[idx].Identity = ConstructedFlowInstanceIdentityFixture(sources[0], out[idx].FlowTemplate, runtimeflowidentity.LogicalInstanceID(out[idx].FlowInstance), out[idx].RunID)
			if out[idx].EntityID == "" {
				out[idx].EntityID = out[idx].Identity.EntityID
			}
		}
	}
	return out
}

var authorActivityTestDifferentEventTypes = strings.Fields(`
account.ready child/child.start child/grandchild/micro.done child/grandchild/micro.started child/inst-1/micro.started
child/output.done custom.bad custom.claimed custom.completion_failure custom.direct custom.direct.empty custom.emitted
custom.followup custom.good custom.in_flight custom.internal custom.leaf custom.markerless custom.middle custom.mixed
custom.mixed_node_agent custom.no_subscribers custom.node_only custom.node_only_outbox custom.node_only_sweep
custom.node_only_tx custom.non_transactional custom.paused custom.pool_saturation custom.publish_mutation_post_commit
custom.receipt_failure custom.replay.checked custom.replay_pool_saturation custom.root custom.routed custom.run_control
custom.run_control.acked custom.run_control.deferred custom.run_control.intercepted custom.run_control.postcommit
custom.run_control.postcommit.deferred custom.shared_claim custom.snapshot custom.trigger deploy.done human_task.approved
inbound.proof inbound.proof.normalized item.received legacy.event mailbox.card_decided opco.spinup_requested
operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested
operating/inst-1/opco.product_initialization_requested operating/opco.product_initialization_requested pipeline.start
platform.agent_failed platform.boot platform.budget_threshold_crossed platform.paused platform.recovery_failed
platform.run_stalled platform.runtime_log producer/account.ready producer/audit.seen producer/deploy.done
producer/scan.requested producer/ticket.ready producer/validation.requested producer/work.ready review/inst-1/task.started
review/task.started root.ready scan.requested task.completed task.failed task.requested task.started test.duplicate_route
test.identity_route test.new test.old test.retained test.route_generation test.route_generation_ack
test.route_generation_mutation test.tokenless thing.created validate.requested validation.requested
validation/thing.reviewed worker/work.assign
`)

type authorActivityTestCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type testFlowInstanceActivationOwner struct {
	mu       sync.Mutex
	activate runtimepipeline.FlowInstanceActivator
	pending  map[runtimeflowidentity.Route]runtimepipeline.FlowInstanceActivationRequest
}

func newTestFlowInstanceActivationOwner(activate runtimepipeline.FlowInstanceActivator) *testFlowInstanceActivationOwner {
	return &testFlowInstanceActivationOwner{
		activate: activate,
		pending:  make(map[runtimeflowidentity.Route]runtimepipeline.FlowInstanceActivationRequest),
	}
}

func (o *testFlowInstanceActivationOwner) PrepareFlowInstanceActivation(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
	fields, err := testFlowActivationConstructorFields(req)
	if err != nil {
		return runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	if req.OccurredAt.IsZero() {
		req.OccurredAt = req.TriggerEvent.CreatedAt()
	}
	if strings.TrimSpace(req.InitialState) == "" {
		if schema, ok := req.ContractBundle.FlowSchemaByID(req.Instance.TemplateID); ok {
			req.InitialState = schema.LoweredInitialState()
		}
	}
	readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity:        req.Instance,
		RunID:           req.TriggerEvent.RunID(),
		BundleHash:      authorActivityTestBundleHash,
		WorkflowVersion: req.ContractBundle.WorkflowVersion(),
		ExecutionMode:   "live",
	}
	if readiness.RunID == "" {
		readiness.RunID = runtimecorrelation.RunIDFromContext(ctx)
	}
	if bundle, found := semanticview.Bundle(req.ContractBundle); found && bundle.SourceArtifact != nil {
		readiness.BundleHash = bundle.SourceArtifact.BundleHash()
	}
	var key string
	if schema, found := req.ContractBundle.FlowSchemaByID(req.Instance.TemplateID); found && !schema.Instance.Empty() {
		key, err = runtimepipeline.AdmitFlowInstanceKey(req.ContractBundle, req.Instance.TemplateID, req.ResolvedKey)
		if err != nil {
			return runtimepipeline.FlowInstanceActivationPlan{}, err
		}
	}
	instance := runtimepipeline.WorkflowInstance{
		InstanceKey:        key,
		ParentFlowID:       req.Instance.ParentRoute.FlowID,
		ParentFlowInstance: req.Instance.ParentRoute.FlowInstance,
		ParentEntityID:     req.Instance.ParentEntityID,
		InstanceID:         req.Instance.InstanceID,
		StorageRef:         req.Instance.InstancePath,
		EntityID:           req.Instance.EntityID,
		WorkflowName:       req.Instance.TemplateID,
		WorkflowVersion:    req.ContractBundle.WorkflowVersion(),
		CurrentState:       req.InitialState,
		Fields:             fields,
		Bookkeeping:        req.Bookkeeping,
		EnteredStageAt:     req.OccurredAt,
		CreatedAt:          req.OccurredAt,
		RuntimeReadiness:   &readiness,
		EntityType:         "test_entity",
	}
	plan := runtimepipeline.FlowInstanceActivationPlan{
		Instance: instance, Identity: req.Instance, Readiness: readiness,
		CreatingInput: runtimepipeline.FlowConstructionInput{EventID: req.TriggerEvent.ID(), Input: req.ConstructorInput},
		OccurredAt:    req.OccurredAt, ActivationVariables: connectRoutePlanActivationVariables(req),
	}
	if err := plan.Validate(); err != nil {
		return runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	o.mu.Lock()
	o.pending[req.Instance.Route()] = req
	o.mu.Unlock()
	return plan, nil
}

func testFlowActivationConstructorFields(req runtimepipeline.FlowInstanceActivationRequest) (map[string]any, error) {
	constructor, err := runtimepipeline.CompileFlowConstructor(req.ContractBundle, req.Instance.TemplateID, req.ConstructorInput)
	if err != nil {
		return nil, err
	}
	payload, err := req.ConstructorPayload()
	if err != nil {
		return nil, err
	}
	return constructor.InitialFields(payload, req.ResolvedKey)
}

func (o *testFlowInstanceActivationOwner) FinalizeCommittedFlowInstanceActivation(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
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

func testAuthorActivityContext(ctx context.Context) context.Context {
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestSourceArtifactFact.BundleHash(),
	))
}

func newScopedTestEventBus(store EventStore, options ...EventBusOptions) (*EventBus, error) {
	opts := EventBusOptions{}
	if len(options) > 0 {
		opts = options[0]
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if opts.PayloadAdmitter == nil {
		opts.PayloadAdmitter = func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
			return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
		}
	}
	if opts.PipelineObligations == nil {
		if provider, ok := store.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	runOwner := opts.Durable.RunLifecycle
	if opts.PipelineObligations != nil {
		opts.Durable = ExactDurableTestDependencies(store)
	}
	if runOwner != nil {
		opts.Durable.RunLifecycle = runOwner
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if bundle, ok := semanticview.Bundle(opts.ContractBundle); opts.SourceArtifactFact.BundleHash() == "" && ok && bundle != nil && bundle.SourceArtifact != nil {
		fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
		if err != nil {
			return nil, err
		}
		opts.SourceArtifactFact = fact
	}
	if opts.FlowActivationFinalizer == nil {
		opts.FlowActivationFinalizer, _ = opts.TemplateInstancePlanner.(runtimepipeline.CommittedFlowInstanceActivationFinalizer)
	}
	if strings.TrimSpace(opts.SourceArtifactFact.BundleHash()) == "" {
		opts.SourceArtifactFact = authorActivityTestSourceArtifactFact
	}
	if err := ensureTestEventBusSourceArtifact(store, opts.ContractBundle, opts.SourceArtifactFact); err != nil {
		return nil, err
	}
	if receiver, ok := store.(interface {
		setTestConstructionSource(semanticview.Source)
	}); ok {
		receiver.setTestConstructionSource(opts.ContractBundle)
	}
	if receiver, ok := store.(interface {
		setTestSemanticSource(runtimecorrelation.SourceArtifactFact, string)
	}); ok {
		workflowVersion := "1.0.0"
		if opts.ContractBundle != nil && strings.TrimSpace(opts.ContractBundle.WorkflowVersion()) != "" {
			workflowVersion = opts.ContractBundle.WorkflowVersion()
		}
		receiver.setTestSemanticSource(opts.SourceArtifactFact, workflowVersion)
	}
	if opts.WorkOwner == nil {
		processOwner := worklifetime.NewProcess()
		owner, err := processOwner.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
			RuntimeInstanceID: opts.RuntimeInstanceID,
			BundleHash:        opts.SourceArtifactFact.BundleHash(),
		})
		if err != nil {
			return nil, err
		}
		opts.WorkOwner = owner
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, err := runtimedelivery.NewNormalExecutionAuthority(
			opts.SourceArtifactFact,
			opts.RuntimeInstanceID,
			1,
		)
		if err != nil {
			return nil, err
		}
		opts.DeliveryAuthority = authority
	}
	if registrar, ok := store.(authorActivityTestCatalogRegistrar); ok {
		descriptors := authorActivityTestEventDescriptors(opts.ContractBundle)
		lease, err := registrar.RegisterAuthorActivityEventCatalog(
			runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.SourceArtifactFact.BundleHash()), descriptors,
		)
		if err != nil {
			return nil, err
		}
		_ = lease // The store and its catalog are scoped to the test that owns them.
	}
	var bus *EventBus
	var err error
	if opts.PipelineObligations == nil {
		bus, err = NewEphemeralEventBusWithOptions(store, opts)
	} else {
		bus, err = NewEventBusWithOptions(store, opts)
	}
	if err != nil {
		return nil, err
	}
	if err := bus.SetDeliveryContinuationOwner(permissiveTestDeliveryOwner{}); err != nil {
		return nil, err
	}
	bus.SetCommittedAgentReadinessFinalizer(CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
		return nil
	}))
	return bus, nil
}

func ensureTestEventBusSourceArtifact(store EventStore, source semanticview.Source, fact runtimecorrelation.SourceArtifactFact) error {
	writer, ok := store.(sourceartifactfixture.Writer)
	if !ok {
		return nil
	}
	artifact := sourceartifactfixture.Artifact()
	if bundle, bundled := semanticview.Bundle(source); bundled && bundle != nil && bundle.SourceArtifact != nil {
		artifact = bundle.SourceArtifact
	} else if fact.BundleHash() != artifact.BundleHash() {
		return nil
	}
	if artifact.BundleHash() != fact.BundleHash() {
		return fmt.Errorf("event bus test source artifact %s contradicts selected source %s", artifact.BundleHash(), fact.BundleHash())
	}
	return sourceartifactfixture.EnsureArtifact(context.Background(), writer, artifact)
}

func authorActivityTestEventDescriptors(source semanticview.Source) []runtimeauthoractivity.EventDescriptor {
	byName := make(map[string]runtimeauthoractivity.EventDescriptor, len(authorActivityTestDifferentEventTypes))
	for _, name := range authorActivityTestDifferentEventTypes {
		byName[name] = runtimeauthoractivity.EventDescriptor{EventType: name, Disposition: runtimeauthoractivity.StoryDifferent}
	}
	if source != nil {
		resolved := source.ResolvedEventCatalog()
		authored := source.AuthoredResolvedEventCatalog()
		add := func(name string, disposition runtimeauthoractivity.StoryDisposition) {
			name = strings.TrimSpace(name)
			if name == "" {
				return
			}
			byName[name] = runtimeauthoractivity.EventDescriptor{
				EventType: name, Disposition: disposition,
			}
		}
		for name := range resolved {
			disposition := runtimeauthoractivity.StoryDifferent
			if _, ok := authored[name]; ok {
				disposition = runtimeauthoractivity.StoryAuthored
			}
			add(name, disposition)
		}
		census := semanticview.BuildAuthoredEventEndpointCensus(source)
		endpoints := append(census.Producers(), census.Consumers()...)
		endpoints = append(endpoints, census.InputPins()...)
		endpoints = append(endpoints, census.OutputPins()...)
		for _, endpoint := range endpoints {
			if endpoint.Event.HasSchema {
				disposition := runtimeauthoractivity.StoryDifferent
				if endpoint.Event.IsAuthored(source) {
					disposition = runtimeauthoractivity.StoryAuthored
				}
				add(endpoint.Event.EventKey(), disposition)
			}
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(names))
	for _, name := range names {
		descriptors = append(descriptors, byName[name])
	}
	return descriptors
}
