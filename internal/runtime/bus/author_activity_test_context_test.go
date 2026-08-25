package bus

import (
	"context"
	"sort"
	"strings"
	"sync"

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
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
const authorActivityTestBundleSource = "ephemeral"

var authorActivityTestBundleSourceFact = mustAuthorActivityTestBundleSourceFact()

func exactAuthorActivityFlowInstanceDescriptors(in []ActiveFlowInstanceDescriptor, workflowVersion string) []ActiveFlowInstanceDescriptor {
	out := append([]ActiveFlowInstanceDescriptor(nil), in...)
	for idx := range out {
		if strings.TrimSpace(out[idx].FlowTemplate) == "" {
			out[idx].FlowTemplate = runtimeflowidentity.RouteForInstancePath(out[idx].FlowInstance).ScopeKey
		}
		if strings.TrimSpace(out[idx].BundleHash) == "" {
			out[idx].BundleHash = authorActivityTestBundleHash
		}
		if strings.TrimSpace(out[idx].BundleSource) == "" {
			out[idx].BundleSource = authorActivityTestBundleSource
		}
		if strings.TrimSpace(out[idx].WorkflowVersion) == "" {
			out[idx].WorkflowVersion = strings.TrimSpace(workflowVersion)
		}
	}
	return out
}

func mustAuthorActivityTestBundleSourceFact() runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(authorActivityTestBundleHash)
	if err != nil {
		panic(err)
	}
	return fact
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

func (o *testFlowInstanceActivationOwner) PrepareFlowInstanceActivation(_ context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
	if req.OccurredAt.IsZero() {
		req.OccurredAt = req.TriggerEvent.CreatedAt()
	}
	if strings.TrimSpace(req.InitialState) == "" {
		if schema, ok := req.ContractBundle.FlowSchemaByID(req.Instance.TemplateID); ok {
			req.InitialState = schema.InitialState
		}
	}
	readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity:        req.Instance,
		RunID:           req.TriggerEvent.RunID(),
		BundleHash:      authorActivityTestBundleHash,
		BundleSource:    authorActivityTestBundleSource,
		WorkflowVersion: req.ContractBundle.WorkflowVersion(),
		ExecutionMode:   "live",
	}
	instance := runtimepipeline.WorkflowInstance{
		InstanceID:       req.Instance.InstanceID,
		StorageRef:       req.Instance.InstancePath,
		EntityID:         req.Instance.EntityID,
		WorkflowName:     req.Instance.TemplateID,
		WorkflowVersion:  req.ContractBundle.WorkflowVersion(),
		CurrentState:     req.InitialState,
		Config:           req.Config,
		Fields:           req.Fields,
		Bookkeeping:      req.Bookkeeping,
		EnteredStageAt:   req.OccurredAt,
		CreatedAt:        req.OccurredAt,
		RuntimeReadiness: &readiness,
		EntityType:       "test_entity",
	}
	plan := runtimepipeline.FlowInstanceActivationPlan{
		Instance: instance, Identity: req.Instance, Readiness: readiness,
		OccurredAt: req.OccurredAt,
	}
	if err := plan.Validate(); err != nil {
		return runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	o.mu.Lock()
	o.pending[req.Instance.Route()] = req
	o.mu.Unlock()
	return plan, nil
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
		authorActivityTestBundleSourceFact.BundleHash(),
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
	if opts.PipelineObligations == nil {
		if provider, ok := store.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	if opts.PipelineObligations != nil {
		opts.Durable = ExactDurableTestDependencies(store)
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.TemplateInstanceActivator != nil && opts.TemplateInstancePlanner == nil {
		owner := newTestFlowInstanceActivationOwner(opts.TemplateInstanceActivator)
		opts.TemplateInstancePlanner = owner
		opts.FlowActivationFinalizer = owner
	}
	if opts.FlowActivationFinalizer == nil {
		opts.FlowActivationFinalizer, _ = opts.TemplateInstancePlanner.(runtimepipeline.CommittedFlowInstanceActivationFinalizer)
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash()) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.WorkOwner == nil {
		processOwner := worklifetime.NewProcess()
		owner, err := processOwner.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
			RuntimeInstanceID: opts.RuntimeInstanceID,
			BundleHash:        opts.BundleSourceFact.BundleHash(),
		})
		if err != nil {
			return nil, err
		}
		opts.WorkOwner = owner
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, err := runtimedelivery.NewNormalExecutionAuthority(
			opts.BundleSourceFact,
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
			runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.BundleSourceFact.BundleHash()), descriptors,
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
	return bus, nil
}

func authorActivityTestEventDescriptors(source semanticview.Source) []runtimeauthoractivity.EventDescriptor {
	byName := make(map[string]runtimeauthoractivity.EventDescriptor, len(authorActivityTestDifferentEventTypes))
	for _, name := range authorActivityTestDifferentEventTypes {
		byName[name] = runtimeauthoractivity.EventDescriptor{EventType: name, Disposition: runtimeauthoractivity.StoryDifferent}
	}
	if source != nil {
		resolved := source.ResolvedEventCatalog()
		authored := source.AuthoredResolvedEventCatalog()
		add := func(name string, summaryField string, disposition runtimeauthoractivity.StoryDisposition) {
			name = strings.TrimSpace(name)
			if name == "" {
				return
			}
			byName[name] = runtimeauthoractivity.EventDescriptor{
				EventType: name, Disposition: disposition, AuthorSummaryField: strings.TrimSpace(summaryField),
			}
		}
		for name, entry := range resolved {
			disposition := runtimeauthoractivity.StoryDifferent
			if _, ok := authored[name]; ok {
				disposition = runtimeauthoractivity.StoryAuthored
			}
			add(name, entry.AuthorSummaryField, disposition)
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
				add(endpoint.Event.EventKey(), endpoint.Event.Entry.AuthorSummaryField, disposition)
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
