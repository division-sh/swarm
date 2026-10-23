package pipeline_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestSourceArtifactFact = mustAuthorActivityTestSourceArtifactFact()

func mustAuthorActivityTestSourceArtifactFact() runtimecorrelation.SourceArtifactFact {
	return mustAuthorActivityTestSourceArtifactFactForHash(sourceartifactfixture.BundleHash)
}

func mustAuthorActivityTestSourceArtifactFactForHash(bundleHash string) runtimecorrelation.SourceArtifactFact {
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

type pipelineExternalTestWorkFixture struct {
	process  *worklifetime.Process
	mu       sync.Mutex
	runtimes map[worklifetime.RuntimeIdentity]*worklifetime.RuntimeOccurrence
}

var pipelineExternalTestWorkFixtures sync.Map

func pipelineExternalTestWorkOwner(t *testing.T) *worklifetime.RuntimeOccurrence {
	return pipelineExternalTestWorkOwnerForSource(t, authorActivityTestSourceArtifactFact)
}

func pipelineExternalTestWorkOwnerForSource(t *testing.T, fact runtimecorrelation.SourceArtifactFact) *worklifetime.RuntimeOccurrence {
	t.Helper()
	if err := fact.Validate(); err != nil {
		t.Fatalf("pipeline test work owner requires its exact source: %v", err)
	}
	actual, loaded := pipelineExternalTestWorkFixtures.LoadOrStore(t, &pipelineExternalTestWorkFixture{
		process: worklifetime.NewProcess(), runtimes: make(map[worklifetime.RuntimeIdentity]*worklifetime.RuntimeOccurrence),
	})
	fixture := actual.(*pipelineExternalTestWorkFixture)
	if !loaded {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := fixture.retireAndWait(ctx); err != nil {
				t.Errorf("join pipeline test work owners: %v", err)
				return
			}
			pipelineExternalTestWorkFixtures.Delete(t)
		})
	}
	identity := worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        fact.BundleHash(),
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if owner := fixture.runtimes[identity]; owner != nil {
		return owner
	}
	owner, err := fixture.process.NewRuntime(context.Background(), identity)
	if err != nil {
		t.Fatalf("create pipeline test work owner: %v", err)
	}
	fixture.runtimes[identity] = owner
	return owner
}

func replacePipelineExternalWorkOccurrenceForSource(t *testing.T, fact runtimecorrelation.SourceArtifactFact, predecessor *worklifetime.RuntimeOccurrence) {
	t.Helper()
	actual, found := pipelineExternalTestWorkFixtures.Load(t)
	if !found {
		t.Fatal("native reopen requires its existing process work owner")
	}
	fixture := actual.(*pipelineExternalTestWorkFixture)
	identity := worklifetime.RuntimeIdentity{RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleHash: fact.BundleHash()}
	join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fixture.mu.Lock()
	if fixture.runtimes[identity] != predecessor {
		fixture.mu.Unlock()
		t.Fatal("native reopen cannot replace another runtime occurrence")
	}
	fixture.mu.Unlock()
	if err := fixture.retireAndWait(join); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.process = worklifetime.NewProcess()
	next, err := fixture.process.NewRuntime(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	fixture.runtimes = map[worklifetime.RuntimeIdentity]*worklifetime.RuntimeOccurrence{identity: next}
}

func (f *pipelineExternalTestWorkFixture) retireAndWait(ctx context.Context) error {
	f.mu.Lock()
	f.process.Retire()
	owners := make([]*worklifetime.RuntimeOccurrence, 0, len(f.runtimes))
	for _, owner := range f.runtimes {
		owner.Retire()
		owners = append(owners, owner)
	}
	f.mu.Unlock()
	for _, owner := range owners {
		if _, err := owner.RetireAndWait(ctx); err != nil {
			return err
		}
	}
	_, err := f.process.Join(ctx)
	return err
}

func testAuthorActivityContext(t *testing.T, ctx context.Context) context.Context {
	return testAuthorActivityContextForSource(t, ctx, authorActivityTestSourceArtifactFact)
}

func testAuthorActivityContextForSource(t *testing.T, ctx context.Context, fact runtimecorrelation.SourceArtifactFact) context.Context {
	t.Helper()
	ctx = worklifetime.WithOccurrence(ctx, pipelineExternalTestWorkOwnerForSource(t, fact))
	fixture, _ := pipelineExternalTestWorkFixtures.Load(t)
	ctx = worklifetime.WithProcess(ctx, fixture.(*pipelineExternalTestWorkFixture).process)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		fact.BundleHash(),
	))
}

func nativeProcessContextFixture(t *testing.T, ctx context.Context, selected any) context.Context {
	t.Helper()
	if _, found := runtimecorrelation.RuntimeInstanceIDFromContext(ctx); !found {
		scope, scoped := runtimeauthoractivity.ScopeFromContext(ctx)
		if !scoped || scope.RuntimeInstanceID == "" {
			t.Fatal("native fixture requires its original runtime attribution")
		}
		ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, scope.RuntimeInstanceID)
	}
	store, ok := selected.(agentfixture.Store)
	if !ok {
		t.Fatalf("native fixture requires its process acquisition owner, got %T", selected)
	}
	capability, err := agentfixture.ProcessCapability(t, ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	return nativeOriginalProcessContextFixture(t, ctx, capability)
}

func nativeOriginalProcessContextFixture(t *testing.T, ctx context.Context, capability startupownership.ProcessCapability) context.Context {
	t.Helper()
	original, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	process, ok := worklifetime.ProcessFromContext(ctx)
	if !ok {
		t.Fatal("native fixture requires its original process work owner")
	}
	if err := process.BindOriginalProcess(original.LeaseStamp()); err != nil {
		t.Fatal(err)
	}
	return ctx
}

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type scopedTestDurableStore interface {
	runtimepipeline.FlowInstanceIndexReader
	runtimepipeline.FlowConstructionPublicationReader
	runtimebus.EventStore
	testAuthorActivityCatalogRegistrar
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	runtimebus.ActiveAgentDescriptorLister
	runtimepipeline.WorkflowInstancePersistenceReader
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	runtimerunlifecycle.StandingRestartDispositionReader
	PipelineObligations() runtimepipelineobligation.Store
}

func newScopedTestEventBus(t *testing.T, eventStore scopedTestDurableStore, opts runtimebus.EventBusOptions, differentEvents ...string) (*runtimebus.EventBus, error) {
	t.Helper()
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.SourceArtifactFact.Validate() != nil {
		opts.SourceArtifactFact = authorActivityTestSourceArtifactFact
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = pipelineExternalTestWorkOwnerForSource(t, opts.SourceArtifactFact)
	}
	admitted, err := opts.WorkOwner.Begin(context.Background())
	if err != nil {
		return nil, err
	}
	process, found := worklifetime.ProcessFromContext(admitted.Context())
	if !found {
		return nil, errors.Join(errors.New("native bus fixture requires its process work owner"), admitted.Done())
	}
	if _, bound := process.OriginalProcess(); !bound {
		ctx := runtimecorrelation.WithRuntimeInstanceID(admitted.Context(), opts.RuntimeInstanceID)
		ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.SourceArtifactFact.BundleHash()))
		nativeProcessContextFixture(t, ctx, eventStore)
	}
	if err := admitted.Done(); err != nil {
		return nil, err
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, authorityErr := runtimedelivery.NewNormalExecutionAuthority(
			opts.SourceArtifactFact,
			authorActivityTestRuntimeInstanceID,
			1,
		)
		if authorityErr != nil {
			return nil, authorityErr
		}
		opts.DeliveryAuthority = authority
	}
	if opts.PipelineObligations == nil {
		opts.PipelineObligations = eventStore.PipelineObligations()
	}
	opts.Durable = runtimebus.DurableDependencies{
		Instances:                eventStore,
		ConstructionPublications: eventStore,
		ReplyContext:             eventStore,
		RunLifecycle:             eventStore,
		DeliveryLifecycle:        eventStore,

		ActiveAgents: eventStore,

		PreparedEvents:        eventStore,
		TargetFailureRecorder: eventStore,
		RunOrigins:            eventStore,
		StandingRestarts:      eventStore,
	}
	if opts.PayloadAdmitter == nil {
		opts.PayloadAdmitter = func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
			return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
		}
	}
	descriptors := testAuthorActivityEventDescriptors(t, opts)
	for _, eventType := range differentEvents {
		descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
			EventType: strings.TrimSpace(eventType), Disposition: runtimeauthoractivity.StoryDifferent,
		})
	}
	lease, err := eventStore.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.SourceArtifactFact.BundleHash()), descriptors,
	)
	if err != nil {
		return nil, err
	}
	t.Cleanup(lease.Release)
	bus, err := runtimebus.NewEventBusWithOptions(eventStore, opts)
	if err != nil {
		return nil, err
	}
	if err := bus.SetDeliveryContinuationOwner(
		runtimebustest.NewDeliveryContinuationOwner(false),
	); err != nil {
		return nil, err
	}
	bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
		return nil
	}))
	return bus, nil
}

func testAuthorActivityEventDescriptors(t *testing.T, opts runtimebus.EventBusOptions) []runtimeauthoractivity.EventDescriptor {
	t.Helper()
	if opts.ContractBundle == nil {
		return nil
	}
	resolved := opts.ContractBundle.ResolvedEventCatalog()
	authored := opts.ContractBundle.AuthoredResolvedEventCatalog()
	byName := make(map[string]runtimeauthoractivity.EventDescriptor, len(resolved)+len(authored))
	add := func(name string, descriptor runtimeauthoractivity.EventDescriptor) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		descriptor.EventType = name
		if previous, ok := byName[name]; ok && previous != descriptor {
			t.Fatalf("author activity test descriptor %q conflicts: %#v != %#v", name, previous, descriptor)
		}
		byName[name] = descriptor
	}
	for name := range resolved {
		disposition := runtimeauthoractivity.StoryDifferent
		if _, ok := authored[name]; ok {
			disposition = runtimeauthoractivity.StoryAuthored
		}
		add(name, runtimeauthoractivity.EventDescriptor{Disposition: disposition})
	}
	census := semanticview.BuildAuthoredEventEndpointCensus(opts.ContractBundle)
	endpoints := append(census.Producers(), census.Consumers()...)
	endpoints = append(endpoints, census.InputPins()...)
	endpoints = append(endpoints, census.OutputPins()...)
	for _, endpoint := range endpoints {
		proof := endpoint.Event
		if !proof.HasSchema {
			continue
		}
		disposition := runtimeauthoractivity.StoryDifferent
		if proof.IsAuthored(opts.ContractBundle) {
			disposition = runtimeauthoractivity.StoryAuthored
		}
		add(proof.EventKey(), runtimeauthoractivity.EventDescriptor{Disposition: disposition})
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
