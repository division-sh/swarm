package runtime

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

type shutdownFanOutSession struct{ *runtimeTestRetainedSession }

func (*shutdownFanOutSession) FanOutServingCapacity() (startupownership.FanOutCapacity, error) {
	return startupownership.SQLiteFanOutCapacity(), nil
}

func newShutdownFanOutRegistration(t *testing.T, rt *Runtime) *startupownership.FanOutServingRegistration {
	t.Helper()
	session := &shutdownFanOutSession{newRuntimeTestRetainedSession(t)}
	var err error
	session.plan, err = agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: runtimeTestBundleHash}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	process, err := startupownership.NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	grant, err := process.IssueGenerationGrant(context.Background(), startupownership.GrantRequest{
		BundleHash: runtimeTestBundleHash, RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: session.plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(context.Background()); err != nil {
		t.Fatal(err)
	}
	registration, err := startupownership.RegisterFanOutServing(context.Background(), grant, rt.workOccurrence, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	rt.startupGrant = grant
	rt.fanOutServing = registration
	rt.fanOutContext = context.Background()
	return registration
}

func awaitShutdownFanOut(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func receiveShutdownFanOut(t *testing.T, done <-chan error, label string) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func assertShutdownFanOutPending(t *testing.T, shutdown <-chan error) {
	t.Helper()
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown escaped owned work: %v", err)
	default:
	}
}

func TestRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLive(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	store := newRuntimeShutdownDeliveryStore(t)
	bus, err := newRuntimeTestEventBus(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{Bus: bus, workOccurrence: runtimeTestEventBusRuntimeOccurrence(t, bus)}
	registration := newShutdownFanOutRegistration(t, rt)
	grant := rt.startupGrant

	// A real running manager is an independent dependency, not part of the
	// registration's finite-turn join. Its accepted handler must stay live.
	started, canceled := make(chan struct{}), make(chan struct{})
	agent := runtimeShutdownTestAgent{
		id: "agent-1", subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, _ events.Event) ([]events.Event, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		},
	}
	rt.Manager = manager.NewAgentManagerWithOptions(bus, func(actors.AgentConfig) (manager.Agent, error) {
		return agent, nil
	}, manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, SemanticSource: runtimeTestSubscriptionSource("test.in"),
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed, WorkOwner: rt.workOccurrence,
		DeliveryStore: store, PersistenceRoles: runtimeTestManagerBusRoles(bus), ReceiverExecution: eventreceiver.NormalExecution(),
	}, &runtimeShutdownManagerStore{})
	bindRuntimeShutdownAgentReadinessFinalizer(bus)
	if err := registerRuntimeTestAgent(rt.Manager, runtimeTestAgentConfig(t, actors.AgentConfig{
		ExecutionMode: "live", ID: agent.id, Subscriptions: []string{"test.in"},
		Identity: agentidentitytest.RootRuntime(t, agent.id, "runtime-test/shutdown-admission"),
	})); err != nil {
		t.Fatal(err)
	}
	if err := rt.Manager.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Manager.Shutdown() })
	if err := bus.Publish(ctx, eventtest.ExistingRunRootIngress(eventtest.UUID("fan-out-shutdown-manager"),
		"test.in", "tester", "", []byte(`{}`), 0, agentidentitytest.DefaultRunID, events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	awaitShutdownFanOut(t, started, "manager handler")

	completion := &runtimeShutdownCompletionStore{
		started: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{}), canceled: make(chan error, 1),
	}
	var releaseOnce sync.Once
	releaseCompletion := func() { releaseOnce.Do(func() { close(completion.release) }) }
	t.Cleanup(releaseCompletion)
	executor, err := runlifecycle.NewExecutor(completion, runlifecycle.CandidateScope{BundleHash: runtimeTestBundleHash},
		runlifecycle.TerminalCatalog{}, rt.workOccurrence, runlifecycle.ExecutorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseCompletion()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := executor.Retire(ctx); err != nil {
			t.Error(err)
		}
	})
	rt.runLifecycleExecutor = executor

	// The newly committed obligation is not dispatch-eligible until the
	// publication owner marks handoff. AcceptCommitted must still retain it.
	dispatcher := &shutdownBlockedDispatcher{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	close(dispatcher.release)
	coordinator, err := deliverycontinuation.New(store, startupRecoveryDispositionMap{}, store.authority, rt.workOccurrence, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coordinator.Retire(context.Background()) })
	rt.deliveryContinuations = coordinator

	permit, found, err := registration.BeginTurn(ctx)
	if err != nil || !found {
		t.Fatalf("admit finite turn: found=%v err=%v", found, err)
	}
	t.Cleanup(permit.Done)
	// Match the production precommit reservation / postcommit Submit split.
	admission, err := executor.ReserveCompletionCandidate(permit.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admission.Cancel() })
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("fan-out-shutdown-committed"),
		"test.in", "tester", "", nil, 0, agentidentitytest.DefaultRunID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.id),
		AgentIdentity: agentidentitytest.RootRuntime(t, agent.id, "runtime-test/shutdown-admission")}
	if err := eventfixture.Insert(permit.Context(), store.db, authoractivityfixture.DialectSQLite, event); err != nil {
		t.Fatal(err)
	}
	var proofs []deliverylifecycle.DurableHandoffProof
	if err := store.mutate(permit.Context(), func(ctx context.Context, tx *sql.Tx) error {
		var err error
		proofs, err = store.adapter.CommitInitial(ctx, tx, event.ID(), event.RunID(), []events.DeliveryRoute{route}, store.authority)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(proofs) != 1 || proofs[0].EventID() != event.ID() {
		t.Fatalf("missing exact native-commit handoff proof: %+v", proofs)
	}

	shutdown := make(chan error, 1)
	go func() { shutdown <- rt.ShutdownWithOptions(ShutdownOptions{Grace: 10 * time.Second}) }()
	awaitShutdownFanOut(t, permit.Context().Done(), "registration cancellation")
	assertShutdownFanOutPending(t, shutdown)
	if !rt.shutdownAdmissionClosed() || !executor.Ready() {
		t.Fatal("ingress must be closed but completion executor must remain ready")
	}
	if next, found, err := registration.BeginTurn(ctx); err == nil || found || next != nil {
		if next != nil {
			next.Done()
		}
		t.Fatalf("closing registration admitted another turn: found=%v err=%v", found, err)
	}
	// Taking this mutex also detects Close accidentally joining under it.
	unlocked := make(chan struct{})
	go func() {
		rt.lifecycleMu.Lock()
		defer rt.lifecycleMu.Unlock()
		if rt.fanOutServing != nil || rt.fanOutContext != nil {
			t.Error("registration was not captured and cleared before joining")
		}
		close(unlocked)
	}()
	awaitShutdownFanOut(t, unlocked, "unlocked runtime lifecycle mutex")
	if err := coordinator.AcceptCommitted(proofs); err != nil {
		t.Fatalf("committed turn lost continuation consumer: %v", err)
	}
	acquisition, err := coordinator.Acquire(proofs[0].DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	continuation, acquired := acquisition.Acquired()
	if !acquired || continuation.DeliveryID() != proofs[0].DeliveryID() {
		t.Fatal("committed handoff did not retain the exact continuation")
	}
	if _, err := continuation.Resolve(context.WithoutCancel(permit.Context()), worklifetime.DeliveryContinuationReturnUnqueued); err != nil {
		t.Fatal(err)
	}
	if err := admission.Submit(runlifecycle.Candidate{
		RunID: event.RunID(), BundleHash: runtimeTestBundleHash, Revision: 1,
		DueAt: runlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-time.Second)),
	}); err != nil {
		t.Fatalf("committed turn lost reserved completion consumer: %v", err)
	}
	awaitShutdownFanOut(t, completion.started, "postcommit completion persistence")
	select {
	case <-canceled:
		t.Fatal("manager retired before committed turn completed")
	default:
	}
	if err := grant.ProveCurrent(ctx); err != nil {
		t.Fatalf("generation retired before committed turn completed: %v", err)
	}
	assertShutdownFanOutPending(t, shutdown)
	permit.Done()
	// The turn may finish before its accepted completion persistence. Runtime
	// must then join that consumer before releasing the generation.
	assertShutdownFanOutPending(t, shutdown)
	releaseCompletion()
	awaitShutdownFanOut(t, completion.completed, "completion commit")
	receiveShutdownFanOut(t, shutdown, "runtime shutdown")
	awaitShutdownFanOut(t, canceled, "manager retirement")
	awaitShutdownFanOut(t, grant.Done(), "generation retirement")
	if rt.workOccurrence.ActiveCount() != 0 {
		t.Fatalf("shutdown leaked runtime work: active=%d", rt.workOccurrence.ActiveCount())
	}
}

func TestRuntimeShutdownFanOutDoesNotJoinBufferedOccurrenceBeforeBusRetirement(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	bus, err := newRuntimeTestEventBus(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{Bus: bus, workOccurrence: runtimeTestEventBusRuntimeOccurrence(t, bus)}
	registration := newShutdownFanOutRegistration(t, rt)
	subscription, err := bus.SubscribeInternal(ctx, "fan-out-shutdown-buffer", "test.buffered")
	if err != nil {
		t.Fatal(err)
	}
	subscription.MarkReady()
	t.Cleanup(func() { _ = subscription.Complete(false) })
	baseline := rt.workOccurrence.ActiveCount()
	if err := bus.Publish(ctx, eventtest.RuntimeControl(eventtest.UUID("fan-out-shutdown-buffered"),
		"test.buffered", "tester", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if len(subscription.Deliveries()) != 1 || rt.workOccurrence.ActiveCount() <= baseline {
		t.Fatalf("fixture did not retain a real buffered carrier: queued=%d active=%d baseline=%d",
			len(subscription.Deliveries()), rt.workOccurrence.ActiveCount(), baseline)
	}
	permit, found, err := registration.BeginTurn(ctx)
	if err != nil || !found {
		t.Fatalf("admit finite turn: found=%v err=%v", found, err)
	}
	t.Cleanup(permit.Done)
	receiverDone := make(chan error, 1)
	go func() {
		<-subscription.Retiring()
		// Do not consume the carrier. Only production bus retirement drains it.
		receiverDone <- subscription.Complete(false)
	}()
	shutdown := make(chan error, 1)
	go func() { shutdown <- rt.ShutdownWithOptions(ShutdownOptions{Grace: 10 * time.Second}) }()
	awaitShutdownFanOut(t, permit.Context().Done(), "registration cancellation with buffered work")
	assertShutdownFanOutPending(t, shutdown)
	select {
	case <-subscription.Retiring():
		t.Fatal("bus dependency retired before finite turn joined")
	default:
	}
	if len(subscription.Deliveries()) != 1 {
		t.Fatal("buffered carrier disappeared before registration joined")
	}
	permit.Done()
	receiveShutdownFanOut(t, receiverDone, "bus receiver retirement (not whole-occurrence join)")
	receiveShutdownFanOut(t, shutdown, "shutdown with buffered carrier")
	if len(subscription.Deliveries()) != 0 || rt.workOccurrence.ActiveCount() != 0 {
		t.Fatalf("shutdown leaked buffered ownership: queued=%d active=%d", len(subscription.Deliveries()), rt.workOccurrence.ActiveCount())
	}
	if _, err := rt.workOccurrence.Begin(ctx); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("runtime occurrence not retired: %v", err)
	}
}
