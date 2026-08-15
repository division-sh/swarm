package apiv1

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = mustAPITestBundleSourceFact(
	"bundle-v1:sha256:" + strings.Repeat("a", 64),
)

func mustAPITestBundleSourceFact(bundleHash string) runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

func mustAPITestPersistedBundleSourceFact(bundleHash string) runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

var authorActivityTestDifferentEventTypes = strings.Fields(`
bootstrap.requested filler.event mailbox.card_decided mailbox.review_requested platform.activity_requested review.requested
scan.followup scan.requested thing.created trace.visible triage.requested
`)

type authorActivityTestCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type apiTestRuntimeMutationOwner interface {
	runtimepipeline.WorkflowPersistenceOwner
}

type apiTestDurableEventStore interface {
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
	runtimebus.EventDeliveryRouteSetReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
}

func testAuthorActivityRuntimeContext(ctx context.Context) context.Context {
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(authorActivityTestRuntimeInstanceID))
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForSource(ctx, authorActivityTestBundleSourceFact)
}

func testAuthorActivityContextForSource(ctx context.Context, fact runtimecorrelation.BundleSourceFact) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := fact.Validate(); err != nil {
		panic(err)
	}
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, authorActivityTestRuntimeInstanceID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID, fact.BundleHash(),
	))
}

func testAuthorActivityRequest(req *http.Request) *http.Request {
	if req == nil {
		return nil
	}
	return req.WithContext(testAuthorActivityRuntimeContext(req.Context()))
}

func newScopedAPITestEventBus(t *testing.T, eventStore runtimebus.EventStore, options ...runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	opts := runtimebus.EventBusOptions{}
	if len(options) > 0 {
		opts = options[0]
	}
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.BundleSourceFact.BundleHash() == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = newAPITestRuntimeWorkOccurrence(t, opts.RuntimeInstanceID, opts.BundleSourceFact.BundleHash())
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if opts.PipelineObligations == nil {
		if provider, ok := eventStore.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	if opts.PipelineObligations != nil {
		durable, ok := eventStore.(apiTestDurableEventStore)
		if !ok {
			return nil, fmt.Errorf("API durable event-store fixture %T lacks exact durable roles", eventStore)
		}
		opts.Durable = runtimebus.DurableDependencies{
			ReplyContext:          durable,
			RunLifecycle:          durable,
			DeliveryLifecycle:     durable,
			FlowRoutes:            durable,
			FlowRouteRecords:      durable,
			FlowRouteSets:         durable,
			FlowRouteTopology:     durable,
			FlowRouteRollback:     durable,
			ActiveAgents:          durable,
			ActiveFlows:           durable,
			TargetOwners:          durable,
			DeliveryRouteSets:     durable,
			TargetFailureRecorder: durable,
			RunOrigins:            durable,
		}
	}
	deliveryStore, hasDeliveryStore := eventStore.(runtimedelivery.Store)
	if hasDeliveryStore && opts.PipelineObligations != nil && opts.DeliveryAuthority.Kind() == "" {
		authority, err := runtimedelivery.NewNormalExecutionAuthority(
			opts.BundleSourceFact,
			opts.RuntimeInstanceID,
			1,
		)
		if err != nil {
			return nil, err
		}
		if err := deliveryStore.ActivateDeliveryAuthority(
			testAuthorActivityContextForSource(context.Background(), opts.BundleSourceFact),
			authority,
		); err != nil {
			return nil, err
		}
		opts.DeliveryAuthority = authority
	}
	if registrar, ok := eventStore.(authorActivityTestCatalogRegistrar); ok {
		descriptors, err := authorActivityTestDescriptors(opts.ContractBundle)
		if err != nil {
			return nil, err
		}
		lease, err := registrar.RegisterAuthorActivityEventCatalog(
			runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.BundleSourceFact.BundleHash()), descriptors,
		)
		if err != nil {
			return nil, err
		}
		t.Cleanup(lease.Release)
	}
	var bus *runtimebus.EventBus
	var err error
	if opts.PipelineObligations == nil {
		bus, err = runtimebus.NewEphemeralEventBusWithOptions(eventStore, opts)
	} else {
		bus, err = runtimebus.NewEventBusWithOptions(eventStore, opts)
	}
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		if err := bus.ResetInMemoryState(); err != nil {
			t.Errorf("retire API test EventBus queues: %v", err)
		}
	})
	if hasDeliveryStore && opts.PipelineObligations != nil {
		coordinator, err := runtimedeliverycontinuation.New(
			deliveryStore,
			opts.DeliveryAuthority,
			opts.WorkOwner,
			bus,
			func(_ context.Context, reportErr error) {
				t.Errorf("API test delivery continuation failed: %v", reportErr)
			},
		)
		if err != nil {
			return nil, err
		}
		if err := bus.SetDeliveryContinuationOwner(coordinator); err != nil {
			return nil, err
		}
		if err := coordinator.Start(testAuthorActivityContextForSource(context.Background(), opts.BundleSourceFact)); err != nil {
			return nil, err
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := coordinator.Retire(ctx); err != nil {
				t.Errorf("retire API test delivery continuation: %v", err)
			}
		})
	}
	return bus, nil
}

func newAPITestRuntimeWorkOccurrence(t *testing.T, runtimeInstanceID, bundleHash string) *worklifetime.RuntimeOccurrence {
	t.Helper()
	process := worklifetime.NewProcess()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: strings.TrimSpace(runtimeInstanceID),
		BundleHash:        strings.TrimSpace(bundleHash),
	})
	if err != nil {
		t.Fatalf("create API test runtime work occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := occurrence.RetireAndWait(ctx); err != nil {
			t.Errorf("retire API test runtime work: %v", err)
		}
		process.Retire()
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join API test process work: %v", err)
		}
	})
	return occurrence
}

func registerScopedAPITestCatalog(t *testing.T, target authorActivityTestCatalogRegistrar, source semanticview.Source) {
	t.Helper()
	descriptors, err := authorActivityTestDescriptors(source)
	if err != nil {
		t.Fatalf("project API test author activity catalog: %v", err)
	}
	lease, err := target.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleSourceFact.BundleHash()), descriptors,
	)
	if err != nil {
		t.Fatalf("register API test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}

func authorActivityTestDescriptors(source semanticview.Source) ([]runtimeauthoractivity.EventDescriptor, error) {
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]runtimeauthoractivity.EventDescriptor, len(authorActivityTestDifferentEventTypes)+len(descriptors))
	for _, name := range authorActivityTestDifferentEventTypes {
		byName[name] = runtimeauthoractivity.EventDescriptor{EventType: name, Disposition: runtimeauthoractivity.StoryDifferent}
	}
	for _, descriptor := range descriptors {
		byName[descriptor.EventType] = descriptor
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	descriptors = descriptors[:0]
	for _, name := range names {
		descriptors = append(descriptors, byName[name])
	}
	return descriptors, nil
}
