package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type runtimeShutdownTestAgent struct {
	id            string
	subscriptions []events.EventType
	onEvent       func(context.Context, events.Event) ([]events.Event, error)
}

type shutdownContinuationPage struct {
	runtimedelivery.Store
	item runtimedelivery.ContinuationItem
}

func (s *shutdownContinuationPage) ScanDeliveryContinuations(context.Context, runtimedelivery.ExecutionAuthority, runtimedelivery.ContinuationCursor, int) (runtimedelivery.ContinuationPage, error) {
	return runtimedelivery.ContinuationPage{Items: []runtimedelivery.ContinuationItem{s.item}, Exhausted: true}, nil
}

type shutdownBlockedDispatcher struct {
	entered, canceled, release chan struct{}
	ctx                        context.Context
}

func (d *shutdownBlockedDispatcher) DispatchDeliveryContinuation(ctx context.Context, _ events.Event, _ events.DeliveryRoute) runtimedeliverycontinuation.DispatchResult {
	d.ctx = ctx
	close(d.entered)
	<-ctx.Done()
	close(d.canceled)
	<-d.release
	return runtimedeliverycontinuation.Fatal(ctx.Err())
}

func bindRuntimeShutdownAgentReadinessFinalizer(bus *runtimebus.EventBus) {
	bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(_ context.Context, event events.Event, routes []events.DeliveryRoute) error {
		for _, route := range routes {
			if !route.Recipient.IsAgent() {
				continue
			}
			if err := route.AgentIdentity.Validate(); err != nil {
				return err
			}
			if route.AgentIdentity.RunID != event.RunID() {
				return errors.New("shutdown test agent readiness route escaped the event run")
			}
		}
		return nil
	}))
}

func (a runtimeShutdownTestAgent) ID() string { return a.id }
func (runtimeShutdownTestAgent) Type() string { return "test" }
func (a runtimeShutdownTestAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}
func (a runtimeShutdownTestAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	return a.onEvent(ctx, evt)
}

type runtimeShutdownManagerStore struct{}

type runtimeShutdownCompletionStore struct {
	started   chan struct{}
	release   chan struct{}
	completed chan struct{}
	canceled  chan error
}

func (s *runtimeShutdownCompletionStore) ListCompletionCandidates(
	context.Context,
	runtimerunlifecycle.CandidateScope,
	runtimerunlifecycle.CandidateCursor,
	int,
) (runtimerunlifecycle.CandidatePage, error) {
	return runtimerunlifecycle.CandidatePage{Exhausted: true}, nil
}

func (s *runtimeShutdownCompletionStore) ExecuteCompletionCandidate(
	ctx context.Context,
	_ runtimerunlifecycle.Candidate,
	_ runtimerunlifecycle.FinalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	close(s.started)
	select {
	case <-ctx.Done():
		s.canceled <- context.Cause(ctx)
		return runtimerunlifecycle.CompletionResult{}, context.Cause(ctx)
	case <-s.release:
	}
	close(s.completed)
	return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeAwaitMutation}, nil
}

// This spy only records the native owner's acknowledged activation; all reads,
// claims, settlements and transaction ownership stay with the original store.
type runtimeShutdownDeliveryStore struct {
	runtimedelivery.Store
	authority runtimedelivery.ExecutionAuthority
}

func (s *runtimeShutdownDeliveryStore) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) error {
	commit, err := s.ActivateDeliveryAuthorityOutcome(ctx, authority)
	if !commit.Acknowledged && err == nil {
		return errors.New("native shutdown authority activation was not acknowledged")
	}
	return err
}

func (s *runtimeShutdownDeliveryStore) ActivateDeliveryAuthorityOutcome(ctx context.Context, authority runtimedelivery.ExecutionAuthority) (runtimedelivery.ActivationCommit, error) {
	commit, err := s.Store.ActivateDeliveryAuthorityOutcome(ctx, authority)
	if commit.Acknowledged {
		s.authority = authority
	}
	return commit, err
}

func newRuntimeShutdownDeliveryStore(t *testing.T, open RuntimeLogNativeOpenerForTest) (*runtimeShutdownDeliveryStore, RuntimeLogNativeFixtureForTest) {
	t.Helper()
	fixture := open(t, sourceartifactfixture.Artifact())
	if err := fixture.RequireRun(fixture.Context, agentidentitytest.DefaultRunID); err != nil {
		t.Fatal(err)
	}
	return &runtimeShutdownDeliveryStore{Store: fixture.Deliveries}, fixture
}

func activateRuntimeShutdownDelivery(t *testing.T, store *runtimeShutdownDeliveryStore, bus *runtimebus.EventBus, ctx context.Context) {
	t.Helper()
	authority, err := bus.DeliveryAuthority()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
}

func (*runtimeShutdownManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (*runtimeShutdownManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return nil, nil
}

func (*runtimeShutdownManagerStore) EnsureEntitySchema(context.Context, string) error {
	return nil
}

type runtimeShutdownInboundStore struct {
	recorded bool
	store    runtimebus.EventStore
}

type cancellationBlockingInboundStore struct {
	entered chan struct{}
	store   runtimebus.EventStore
}

func (s *cancellationBlockingInboundStore) bindTestInboundEventStore(store runtimebus.EventStore) {
	s.store = store
}

func (s *cancellationBlockingInboundStore) CommitInboundPublication(ctx context.Context, _ runtimeinbound.CommitCommand) (runtimeinbound.CommitResult, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return runtimeinbound.CommitResult{}, ctx.Err()
}

func (*cancellationBlockingInboundStore) LoadInboundPublicationByIdentity(context.Context, runtimeinbound.Identity) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, nil
}

func (*cancellationBlockingInboundStore) ValidateInboundPublicationIntegrity(context.Context) error {
	return nil
}

func (s *runtimeShutdownInboundStore) bindTestInboundEventStore(store runtimebus.EventStore) {
	s.store = store
}

func (s *runtimeShutdownInboundStore) CommitInboundPublication(_ context.Context, command runtimeinbound.CommitCommand) (runtimeinbound.CommitResult, error) {
	s.recorded = true
	return runTestInboundPublication(command, true)
}

func (s *runtimeShutdownInboundStore) ResolveInboundTarget(context.Context, string, string) (InboundTarget, error) {
	return InboundTarget{}, nil
}

func (*runtimeShutdownInboundStore) LoadInboundPublicationByIdentity(context.Context, runtimeinbound.Identity) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, nil
}

func (*runtimeShutdownInboundStore) ValidateInboundPublicationIntegrity(context.Context) error {
	return nil
}

func VerifyRuntimeShutdownDeliveryFixtureClaimsThroughCanonicalAdapterForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	store, fixture := newRuntimeShutdownDeliveryStore(t, open)
	identity := agentidentitytest.RootRuntime(t, "agent-1", "runtime-test/shutdown-admission")
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("shutdown-fixture-claim"),
		"test.in", "tester", "", nil, 0, identity.RunID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(sourceartifactfixture.Fact(), "runtime-shutdown-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateDeliveryAuthorityOutcome(fixture.Context, authority); err != nil {
		t.Fatal(err)
	}
	missing, err := store.ClaimDelivery(fixture.Context, authority, event, route)
	if err != nil || missing.Disposition != runtimedelivery.ClaimAbsent {
		t.Fatalf("shutdown fixture claimed an unpublished obligation: %+v/%v", missing, err)
	}
	before := fixture.Physical(fixture.Context)
	if before.Events != 0 || before.Deliveries != 0 {
		t.Fatalf("claim seeded unpublished durable work: %+v", before)
	}
	event = fixture.PublishDelivery(fixture.Context, event, []events.DeliveryRoute{route}, store.authority)
	result, err := store.ClaimDelivery(testAuthorActivityContext(context.Background()), store.authority, event, route)
	if err != nil {
		t.Fatalf("shutdown fixture must admit its real delivery claim: %v", err)
	}
	if _, acquired := result.Acquired(); !acquired {
		t.Fatalf("shutdown fixture did not acquire delivery: %+v", result)
	}
}

func VerifyRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngressForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	deliveryStore, fixture := newRuntimeShutdownDeliveryStore(t, open)
	bus, err := newRuntimeTestEventBus(t, nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	activateRuntimeShutdownDelivery(t, deliveryStore, bus, fixture.Context)
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	release := make(chan struct{})
	var finish sync.Once
	releaseWork := func() { finish.Do(func() { close(release) }) }

	agent := runtimeShutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			canceled <- struct{}{}
			<-release
			return nil, ctx.Err()
		},
	}

	workOwner := runtimeTestEventBusRuntimeOccurrence(t, bus)
	rt := &Runtime{Bus: bus, workOccurrence: workOwner}
	t.Cleanup(func() { releaseWork(); _ = rt.Shutdown() })
	managerStore := &runtimeShutdownManagerStore{}
	am := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != agent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		ExecutionPosture:               executionposture.Live,
		SemanticSource:                 runtimeTestSubscriptionSource("test.in"),
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
		WorkOwner:                      workOwner,
		DeliveryStore:                  deliveryStore,
		PersistenceRoles:               runtimeTestManagerBusRoles(bus), ReceiverExecution: eventreceiver.NormalExecution(),
	}, managerStore)
	rt.Manager = am
	bindRuntimeShutdownAgentReadinessFinalizer(bus)

	inboundStore := &runtimeShutdownInboundStore{}
	testInbound := newTestInboundGateway(t, bus, nil, rt.shutdownAdmissionClosed, inboundStore)
	rt.InboundGateway = testInbound.InboundGateway

	if err := registerRuntimeTestAgent(am, runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            agent.id,
		Identity:      agentidentitytest.RootRuntime(t, agent.id, "runtime-test/shutdown-admission"),
		Subscriptions: []string{"test.in"},
	})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := am.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("runtime-shutdown-inbound-1"),
		events.EventType("test.in"),
		"tester", "", []byte(`{}`), 0, agentidentitytest.DefaultRunID, events.EventEnvelope{}, time.Now().UTC())
	event = fixture.PublishDelivery(fixture.Context, event, []events.DeliveryRoute{{
		Recipient: events.MustAgentDeliveryRecipient(agent.id), AgentIdentity: agentidentitytest.RootRuntime(t, agent.id, "runtime-test/shutdown-admission"),
	}}, deliveryStore.authority)
	if err := bus.Publish(testAuthorActivityContext(context.Background()), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight work to start")
	}

	// Hold a real coordinator dispatch across shutdown. Its manager dependency
	// must outlive it, even though retirement has already canceled dispatch.
	dispatcher := &shutdownBlockedDispatcher{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	releaseDispatch := func() { releaseOnce.Do(func() { close(dispatcher.release) }) }
	defer releaseDispatch()
	continuationEvent := eventtest.ExistingRunRootIngress(eventtest.UUID("shutdown-continuation"), "test.in", "tester", "", []byte(`{}`), 0, agentidentitytest.DefaultRunID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.id), AgentIdentity: agentidentitytest.RootRuntime(t, agent.id, "runtime-test/shutdown-admission")}
	deliveryID, err := runtimedelivery.DeliveryID(continuationEvent.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	page := &shutdownContinuationPage{item: runtimedelivery.ContinuationItem{
		DeliveryID: deliveryID, Event: continuationEvent, Disposition: runtimedelivery.ClaimAcquired,
		Snapshot: runtimedelivery.Snapshot{DeliveryID: deliveryID, Route: route, Status: runtimedelivery.StatusPending, Authority: deliveryStore.authority},
	}}
	coordinator, err := runtimedeliverycontinuation.New(page, startupRecoveryDispositionMap{}, deliveryStore.authority, workOwner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt.deliveryContinuations = coordinator
	startResult := make(chan error, 1)
	go func() { startResult <- coordinator.Start(context.Background()) }()
	select {
	case <-dispatcher.entered:
	case <-time.After(time.Second):
		t.Fatal("continuation dispatch not entered")
	}

	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- rt.Shutdown()
	}()

	select {
	case <-dispatcher.canceled:
	case <-time.After(time.Second):
		t.Fatal("continuations not retired before manager")
	}
	select {
	case <-canceled:
		t.Fatal("manager retired before continuation dispatch joined")
	default:
	}
	select {
	case err := <-shutdownErrCh:
		t.Fatalf("shutdown detached continuation: %v", err)
	default:
	}
	releaseDispatch()
	select {
	case err := <-startResult:
		if err != nil {
			t.Fatalf("startup scan: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("startup scan did not complete")
	}

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("accepted work was not canceled during retirement")
	}
	select {
	case err := <-shutdownErrCh:
		t.Fatalf("Shutdown returned before canceled work settled: %v", err)
	default:
	}

	if !rt.shutdownAdmissionClosed() {
		t.Fatal("runtime shutdown admission was not closed before manager drain")
	}
	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-1","type":"push"}`))
	rec := httptest.NewRecorder()
	testInbound.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("inbound status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "runtime shutting down") {
		t.Fatalf("inbound body = %q, want runtime shutting down", rec.Body.String())
	}
	if inboundStore.recorded {
		t.Fatal("inbound store was touched after runtime shutdown admission closed")
	}

	releaseWork()

	select {
	case err := <-shutdownErrCh:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown to finish")
	}
}

func VerifyRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrainForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	deliveryStore, fixture := newRuntimeShutdownDeliveryStore(t, open)
	bus, err := newRuntimeTestEventBus(t, nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	activateRuntimeShutdownDelivery(t, deliveryStore, bus, fixture.Context)
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	release := make(chan struct{})
	var finish sync.Once
	releaseWork := func() { finish.Do(func() { close(release) }) }

	agent := runtimeShutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			canceled <- struct{}{}
			<-release
			return nil, ctx.Err()
		},
	}

	workOwner := runtimeTestEventBusRuntimeOccurrence(t, bus)
	rt := &Runtime{Bus: bus, workOccurrence: workOwner}
	t.Cleanup(func() { releaseWork(); _ = rt.Shutdown() })
	am := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		ExecutionPosture:               executionposture.Live,
		SemanticSource:                 runtimeTestSubscriptionSource("test.in"),
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
		WorkOwner:                      workOwner,
		DeliveryStore:                  deliveryStore,
		PersistenceRoles:               runtimeTestManagerBusRoles(bus), ReceiverExecution: eventreceiver.NormalExecution(),
	})
	rt.Manager = am
	bindRuntimeShutdownAgentReadinessFinalizer(bus)

	if err := registerRuntimeTestAgent(am, runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            agent.id,
		Identity:      agentidentitytest.RootRuntime(t, agent.id, "runtime-test/shutdown-admission"),
		Subscriptions: []string{"test.in"},
	})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := am.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("runtime-shutdown-grace-inbound-1"),
		events.EventType("test.in"),
		"tester", "", []byte(`{}`), 0, agentidentitytest.DefaultRunID, events.EventEnvelope{}, time.Now().UTC())
	event = fixture.PublishDelivery(fixture.Context, event, []events.DeliveryRoute{{
		Recipient: events.MustAgentDeliveryRecipient(agent.id), AgentIdentity: agentidentitytest.RootRuntime(t, agent.id, "runtime-test/shutdown-admission"),
	}}, deliveryStore.authority)
	if err := bus.Publish(testAuthorActivityContext(context.Background()), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight work to start")
	}

	grace := 25 * time.Millisecond
	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- rt.ShutdownWithOptions(ShutdownOptions{Grace: grace})
	}()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("manager work was not canceled at shutdown")
	}
	<-time.After(2 * grace)
	select {
	case err := <-shutdownErrCh:
		t.Fatalf("ShutdownWithOptions abandoned canceled work: %v", err)
	default:
	}
	releaseWork()
	err = <-shutdownErrCh
	if err == nil || !strings.Contains(err.Error(), "agent manager shutdown:") {
		t.Fatalf("ShutdownWithOptions err = %v, want configured grace manager timeout", err)
	}
	if !rt.shutdownAdmissionClosed() {
		t.Fatal("runtime shutdown admission was not closed")
	}
}

func TestRuntimeShutdownBoundsLifecycleExecutorRetirementByGrace(t *testing.T) {
	occurrence := runtimeTestOccurrence(t, runtimeTestBundleHash)
	executor, err := runtimerunlifecycle.NewExecutor(
		runtimeTestCandidateOwner{},
		runtimerunlifecycle.CandidateScope{BundleHash: runtimeTestBundleHash},
		runtimerunlifecycle.FinalCatalog{},
		occurrence,
		runtimerunlifecycle.ExecutorOptions{},
	)
	if err != nil {
		t.Fatalf("create run lifecycle executor: %v", err)
	}
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start run lifecycle executor: %v", err)
	}
	admission, err := executor.ReserveCompletionCandidate(context.Background())
	if err != nil {
		t.Fatalf("reserve completion candidate admission: %v", err)
	}
	probe, err := occurrence.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin retirement progress probe: %v", err)
	}
	rt := &Runtime{
		workOccurrence:       occurrence,
		runLifecycleExecutor: executor,
	}
	shutdownErr := make(chan error, 1)
	go func() {
		shutdownErr <- rt.ShutdownWithOptions(ShutdownOptions{Grace: 20 * time.Millisecond})
	}()

	select {
	case <-probe.Context().Done():
		if cause := context.Cause(probe.Context()); !errors.Is(cause, worklifetime.ErrRetired) {
			t.Fatalf("retirement progress probe cause = %v, want %v", cause, worklifetime.ErrRetired)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not advance past bounded lifecycle executor retirement")
	}
	select {
	case err := <-shutdownErr:
		t.Fatalf("shutdown released accepted work after grace expiry: %v", err)
	default:
	}
	candidate := runtimerunlifecycle.Candidate{
		RunID:      "11111111-1111-4111-8111-111111111111",
		BundleHash: runtimeTestBundleHash,
		Revision:   1,
		DueAt:      time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
	}
	if err := admission.Submit(candidate); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("delayed completion candidate submission error = %v, want %v", err, worklifetime.ErrRetired)
	}
	if err := probe.Done(); err != nil {
		t.Fatalf("settle retirement progress probe: %v", err)
	}
	err = <-shutdownErr
	if err == nil || !strings.Contains(err.Error(), "run lifecycle executor retirement timed out after 20ms") {
		t.Fatalf("shutdown error = %v, want bounded lifecycle executor timeout", err)
	}
}

func TestRuntimeShutdownRetiresGrantAfterCompletionPersistenceSettles(t *testing.T) {
	store := &runtimeShutdownCompletionStore{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		completed: make(chan struct{}),
		canceled:  make(chan error, 1),
	}
	occurrence := runtimeTestOccurrence(t, runtimeTestBundleHash)
	executor, err := runtimerunlifecycle.NewExecutor(
		store,
		runtimerunlifecycle.CandidateScope{BundleHash: runtimeTestBundleHash},
		runtimerunlifecycle.FinalCatalog{},
		occurrence,
		runtimerunlifecycle.ExecutorOptions{},
	)
	if err != nil {
		t.Fatalf("create run lifecycle executor: %v", err)
	}
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start run lifecycle executor: %v", err)
	}

	authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
		OwnerID:           "runtime-shutdown-completion-test",
		BootID:            uuid.NewString(),
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	}, "runtime_test")
	if err != nil {
		t.Fatalf("construct runtime shutdown authority: %v", err)
	}
	grantRetired := make(chan struct{})
	var grantRetiredOnce sync.Once
	session := &runtimeTestRetainedSession{
		authority: authority,
		agents:    map[string]runtimemanager.PersistedAgent{},
		grantTransition: func(_ *runtimestartupownership.GrantEvidence, next runtimestartupownership.GrantEvidence) {
			if next.State == runtimestartupownership.GrantRetired {
				grantRetiredOnce.Do(func() { close(grantRetired) })
			}
		},
	}
	_, grant, err := newRuntimeTestProcessCapabilityWithSession(
		t,
		nil,
		nil,
		testSourceArtifactFact(t, runtimeTestBundleHash),
		authorActivityTestRuntimeInstanceID,
		session,
	)
	if err != nil {
		t.Fatalf("construct runtime shutdown generation: %v", err)
	}
	candidate := runtimerunlifecycle.Candidate{
		RunID:      "11111111-1111-4111-8111-111111111111",
		BundleHash: runtimeTestBundleHash,
		Revision:   1,
		DueAt:      runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-time.Second)),
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit completion candidate: %v", err)
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for completion persistence")
	}

	rt := &Runtime{
		workOccurrence:       occurrence,
		runLifecycleExecutor: executor,
		startupGrant:         grant,
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- rt.Shutdown() }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for executor.Ready() {
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for runtime retirement fence")
		default:
			goruntime.Gosched()
		}
	}
	select {
	case <-grantRetired:
		t.Fatal("generation grant retired while completion persistence was active")
	case err := <-shutdown:
		t.Fatalf("runtime shutdown completed while persistence was active: %v", err)
	default:
	}
	close(store.release)
	select {
	case <-store.completed:
	case err := <-store.canceled:
		t.Fatalf("completion persistence was canceled during retirement: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for completion persistence to settle")
	}
	if err := <-shutdown; err != nil {
		t.Fatalf("shutdown runtime: %v", err)
	}
	select {
	case <-grantRetired:
	default:
		t.Fatal("generation grant was not retired after completion persistence settled")
	}
}

func TestRuntimeContextDeactivationCancelsStuckWebhookWithoutPublishing(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	publicationStore := &cancellationBlockingInboundStore{
		entered: make(chan struct{}, 1),
	}
	hash := "bundle-v2:sha256:" + strings.Repeat("7", 64)
	workOwner := runtimeTestOccurrence(t, hash)
	bus, err := newInboundTestEventBusWithOptions(t, eventStore, runtimebus.EventBusOptions{WorkOwner: workOwner}, InboundTarget{BundleHash: hash, FlowPath: "chat", RunID: "41000000-0000-0000-0000-000000000001"})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &Runtime{Bus: bus, workOccurrence: workOwner}
	gateway := newTestInboundGateway(t, bus, nil, rt.shutdownAdmissionClosed, publicationStore)
	gateway.SetAdmissionGuard(rt.shutdownGate.BeginContext)
	rt.InboundGateway = gateway.InboundGateway
	contextDef := testBundleContext(t, hash, "inbound.telegram")
	contextDef.Runtime = rt
	contextDef.WorkOwner = rt.WorkOccurrence()
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	body := `{"update_id":901,"message":{"chat":{"id":42},"text":"blocked"}}`
	req := httptest.NewRequest(http.MethodPost, "/webhooks/chat/telegram", strings.NewReader(body))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook_signing.telegram")
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		gateway.HandleResolvedWebhook(rec, req, InboundTarget{
			BundleHash: hash, FlowPath: "chat", RunID: "41000000-0000-0000-0000-000000000001",
			ServiceID: flowidentity.StandingServiceID("chat"), Generation: 1, PublicationSequence: 1,
			Alias: "chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram",
		}, nil)
		response <- rec
	}()
	select {
	case <-publicationStore.entered:
	case <-time.After(time.Second):
		t.Fatal("webhook did not enter blocking persistence")
	}
	result := manager.DeactivateBundleHashWithOptions(hash, RuntimeContextCauseUnloaded, ShutdownOptions{Grace: 20 * time.Millisecond})
	if result.ShutdownErr == nil || !strings.Contains(result.ShutdownErr.Error(), "runtime ingress admission drain timed out") {
		t.Fatalf("deactivation error = %v", result.ShutdownErr)
	}
	select {
	case rec := <-response:
		if rec.Code < 400 {
			t.Fatalf("canceled webhook status = %d, want failure", rec.Code)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("canceled webhook did not return within configured shutdown bound")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("canceled webhook published %d event(s)", len(eventStore.events))
	}
}
