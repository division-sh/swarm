package bus_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const authorActivityTestBundleHash = sourceartifactfixture.BundleHash

var authorActivityTestSourceArtifactFact = sourceartifactfixture.Fact()

func exactTestFlowInstanceDescriptors(in []runtimebus.ActiveFlowInstanceDescriptor, workflowVersion string, sourceFact runtimecorrelation.SourceArtifactFact) []runtimebus.ActiveFlowInstanceDescriptor {
	if sourceFact.Validate() != nil {
		sourceFact = authorActivityTestSourceArtifactFact
	}
	bundleHash := sourceFact.BundleHash()
	out := append([]runtimebus.ActiveFlowInstanceDescriptor(nil), in...)
	for idx := range out {
		if strings.TrimSpace(out[idx].FlowTemplate) == "" {
			out[idx].FlowTemplate = runtimeflowidentity.RouteForInstancePath(out[idx].FlowInstance).ScopeKey
		}
		if strings.TrimSpace(out[idx].BundleHash) == "" {
			out[idx].BundleHash = bundleHash
		}
		if strings.TrimSpace(out[idx].WorkflowVersion) == "" {
			out[idx].WorkflowVersion = strings.TrimSpace(workflowVersion)
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
custom.run_control.postcommit.deferred custom.shared_claim custom.snapshot custom.trigger custom.exact_duplicate_noop
custom.terminal_refusal deploy.done human_task.approved
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

type pipelineObligationTestProvider interface {
	PipelineObligations() runtimepipelineobligation.Store
}

func newScopedTestEventBus(store runtimebus.EventStore, options ...runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	opts := runtimebus.EventBusOptions{}
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
		if provider, ok := store.(pipelineObligationTestProvider); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	if opts.PipelineObligations == nil {
		opts.Durable = runtimebus.DurableTestDependencyProjection(store)
	} else {
		opts.Durable = runtimebus.ExactDurableTestDependencies(store)
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if bundle, ok := semanticview.Bundle(opts.ContractBundle); ok && bundle != nil && bundle.SourceArtifact != nil {
		opts.SourceArtifactFact = testSourceArtifactFact(opts.ContractBundle)
	}
	if opts.SourceArtifactFact.Validate() != nil {
		opts.SourceArtifactFact = authorActivityTestSourceArtifactFact
	}
	if err := ensureTestEventBusSourceArtifact(store, opts.ContractBundle, opts.SourceArtifactFact); err != nil {
		return nil, err
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
		authority, authorityErr := runtimedelivery.NewNormalExecutionAuthority(
			opts.SourceArtifactFact,
			opts.RuntimeInstanceID,
			1,
		)
		if authorityErr != nil {
			return nil, authorityErr
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
		runtimebustest.NewDeliveryContinuationOwner(true),
	); err != nil {
		return nil, err
	}
	return bus, nil
}

func ensureTestEventBusSourceArtifact(store runtimebus.EventStore, source semanticview.Source, fact runtimecorrelation.SourceArtifactFact) error {
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

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForSource(ctx, nil)
}

func testSourceArtifactFact(source semanticview.Source) runtimecorrelation.SourceArtifactFact {
	if bundle, ok := semanticview.Bundle(source); ok && bundle != nil && bundle.SourceArtifact != nil {
		if fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash()); err == nil {
			return fact
		}
	}
	return authorActivityTestSourceArtifactFact
}

func testAuthorActivityContextForSource(ctx context.Context, source semanticview.Source) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	fact := testSourceArtifactFact(source)
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, authorActivityTestRuntimeInstanceID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID, fact.BundleHash(),
	))
}

func acknowledgePipelineTestEvent(t testing.TB, ctx context.Context, provider pipelineObligationTestProvider, eventID string) {
	t.Helper()
	owner := provider.PipelineObligations()
	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("claim pipeline obligation for %s: %v", eventID, err)
	}
	if _, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
		t.Fatalf("acknowledge pipeline obligation for %s: %v", eventID, err)
	}
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

func requireBusEvent(t testing.TB, ch <-chan *runtimebus.LocalDelivery, context string) events.Event {
	t.Helper()
	select {
	case delivery := <-ch:
		evt := delivery.Event()
		_ = delivery.Complete()
		return evt
	default:
		t.Fatalf("%s: expected queued bus event", context)
		return eventtest.RunCreatingRootIngress("", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	}
}

func requireNoBusEvent(t testing.TB, ch <-chan *runtimebus.LocalDelivery, context string) {
	t.Helper()
	select {
	case delivery := <-ch:
		_ = delivery.Complete()
		t.Fatalf("%s: unexpected bus event: %#v", context, delivery.Event())
	default:
	}
}

func requireBusEventTypes(t testing.TB, ch <-chan *runtimebus.LocalDelivery, context string, want ...events.EventType) {
	t.Helper()
	got := make(map[events.EventType]struct{}, len(want))
	for len(got) < len(want) {
		evt := requireBusEvent(t, ch, context)
		got[evt.Type()] = struct{}{}
	}
	for _, eventType := range want {
		if _, ok := got[eventType]; !ok {
			t.Fatalf("%s: received event types = %#v, missing %s", context, got, eventType)
		}
	}
}

func requireSignalBefore(t testing.TB, ch <-chan struct{}, timeout time.Duration, context string) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatalf("%s: timed out after %s", context, timeout)
	}
}

func requireErrorBefore(t testing.TB, ch <-chan error, timeout time.Duration, context string) error {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-ch:
		return err
	case <-timer.C:
		t.Fatalf("%s: timed out after %s", context, timeout)
		return nil
	}
}
