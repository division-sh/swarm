package manager

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestLifecycleConfigRevisionUsesCanonicalConfigFacts(t *testing.T) {
	revision := func(raw string) string {
		t.Helper()
		got, err := lifecycleConfigRevision(PersistedAgent{Config: managerTestAgentConfig(runtimeactors.AgentConfig{
			ID: "agent", Identity: managerAgentIdentity("agent"), Config: json.RawMessage(raw),
		})})
		if err != nil {
			t.Fatalf("lifecycleConfigRevision(%s): %v", raw, err)
		}
		return got
	}
	left := revision(`{"conversation_reference":"42","template_instance_key":"abc"}`)
	right := revision(`{"template_instance_key":"abc","conversation_reference":"42"}`)
	if left != right {
		t.Fatalf("representation-equivalent config revisions differ: left=%s right=%s", left, right)
	}
	changed := revision(`{"template_instance_key":"abc","conversation_reference":"84"}`)
	if changed == left {
		t.Fatalf("semantic config change retained revision %s", changed)
	}
	integer := revision(`{"nested":[7]}`)
	double := revision(`{"nested":[7.0]}`)
	if integer == double {
		t.Fatal("receiver config numeric-kind change retained agent revision")
	}
	if double != revision(`{ "nested" : [7.00] }`) {
		t.Fatal("equivalent runtime double representation changed agent revision")
	}
}

type lifecycleTransitionTrackingBus struct {
	*runtimebus.EventBus
	resetCalls    atomic.Int32
	shutdownJoins atomic.Int32
	shutdownErr   error
	resetStarted  chan struct{}
	resetRelease  <-chan struct{}
}

func (b *lifecycleTransitionTrackingBus) JoinManagerShutdownDependencies(context.Context) error {
	b.shutdownJoins.Add(1)
	return b.shutdownErr
}

func (b *lifecycleTransitionTrackingBus) ResetInMemoryState() error {
	b.resetCalls.Add(1)
	select {
	case b.resetStarted <- struct{}{}:
	default:
	}
	if b.resetRelease != nil {
		<-b.resetRelease
	}
	return b.EventBus.ResetInMemoryState()
}

type blockedManagerLifecycleFixture struct {
	manager   *AgentManager
	bus       *runtimebus.EventBus
	inbound   events.Event
	unblock   func()
	cancelRun context.CancelFunc
}

func newBlockedManagerLifecycleFixture(t *testing.T, managerBus Bus, eventBus *runtimebus.EventBus, native *ManagerDeliveryNativeFixture, dependencies ...ShutdownDependencies) blockedManagerLifecycleFixture {
	t.Helper()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	agent := shutdownTestAgent{
		id:            "agent-transition",
		subscriptions: []events.EventType{"test.transition"},
		onEvent: func(ctx context.Context, _ events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil, ctx.Err()
		},
	}
	opts := AgentManagerOptions{DeliveryStore: native, SemanticSource: native.SemanticSource, SourceArtifactFact: native.Authority.SourceArtifact()}
	if len(dependencies) != 0 {
		opts.ShutdownDependencies = dependencies[0]
	}
	manager := newTestAgentManagerWithOptions(t, managerBus, func(runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	}, opts)
	if err := manager.spawnAgentInternal(native.Context, PersistedAgent{
		Config: managerRootAgentConfig(agent.id, "test.transition"), Topology: managerTestEphemeralTopologyAdmissionForSource(t, native.SemanticSource),
	}, true); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(native.normalRunContext(t))
	t.Cleanup(func() {
		cancelRun()
		unblock()
	})
	if err := manager.Run(runCtx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	inbound := eventtest.RunCreatingRootIngress(eventtest.UUID("evt-transition"), events.EventType("test.transition"),
		"tester", "", []byte(`{}`), 0, managerIdentityTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
	native.seedAgentDeliveries(t, agent.id, []events.Event{inbound})
	if err := eventBus.Publish(native.Context, inbound); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked manager work")
	}
	requireManagerNativeBlockedClaim(t, native, inbound, agent.id)
	return blockedManagerLifecycleFixture{manager: manager, bus: eventBus, inbound: inbound, unblock: unblock, cancelRun: cancelRun}
}

func newLifecycleTransitionEventBus(t *testing.T) *runtimebus.EventBus {
	t.Helper()
	eventBus, err := newTestManagerEventBus(t)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	return eventBus
}

func assertLifecycleCallBlocked(t *testing.T, result <-chan error, operation string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s returned before accepted work settled: %v", operation, err)
	case <-time.After(25 * time.Millisecond):
	}
}

func awaitLifecycleCall(t *testing.T, result <-chan error, operation string) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func ProveNativeManagerWatcherAndExplicitShutdownJoinOneTransition(t *testing.T, newNativeDelivery managerDeliveryNativeFactory) {
	t.Run("ordinary_request_preserves_accepted_drain", func(t *testing.T) {
		native := newNativeDelivery(t)
		eventBus := newLifecycleTransitionEventBus(t)
		fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus, native)
		runCtx, _, _ := fixture.manager.lifecycle.runSnapshot()
		transition := fixture.manager.lifecycle.prepareShutdownTransition(true)
		if got := fixture.manager.lifecycle.requestShutdownTransition(); got != transition {
			t.Fatal("ordinary request replaced the draining transition")
		}
		if err := runCtx.Err(); err != nil {
			t.Fatalf("ordinary request bypassed accepted drain: %v", err)
		}
		fixture.unblock()
		if err := fixture.manager.Shutdown(); err != nil {
			t.Fatalf("join accepted drain: %v", err)
		}
	})
	native := newNativeDelivery(t)
	eventBus := newLifecycleTransitionEventBus(t)
	fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus, native)
	fixture.cancelRun()
	waitForManagerShuttingDown(t, fixture.manager)

	shutdown := make(chan error, 1)
	go func() { shutdown <- fixture.manager.Shutdown() }()
	assertLifecycleCallBlocked(t, shutdown, "explicit shutdown")
	fixture.unblock()
	awaitLifecycleCall(t, shutdown, "explicit shutdown")
}

func ProveNativeManagerResetSerializesAfterSharedShutdown(t *testing.T, newNativeDelivery managerDeliveryNativeFactory) {
	native := newNativeDelivery(t)
	eventBus := newLifecycleTransitionEventBus(t)
	trackingBus := &lifecycleTransitionTrackingBus{EventBus: eventBus, resetStarted: make(chan struct{}, 1)}
	fixture := newBlockedManagerLifecycleFixture(t, trackingBus, eventBus, native)

	shutdown := make(chan error, 1)
	go func() { shutdown <- fixture.manager.Shutdown() }()
	waitForManagerShuttingDown(t, fixture.manager)
	reset := make(chan error, 1)
	go func() { reset <- fixture.manager.ResetRuntimeState() }()
	assertLifecycleCallBlocked(t, shutdown, "shutdown")
	assertLifecycleCallBlocked(t, reset, "reset")
	select {
	case <-trackingBus.resetStarted:
		t.Fatal("reset cleanup started before shared shutdown completed")
	default:
	}

	fixture.unblock()
	awaitLifecycleCall(t, shutdown, "shutdown")
	awaitLifecycleCall(t, reset, "reset")
	if got := trackingBus.resetCalls.Load(); got != 1 {
		t.Fatalf("event bus reset calls = %d, want 1", got)
	}
}

func ProveNativeManagerConcurrentResetsJoinOneResetTransition(t *testing.T, newNativeDelivery managerDeliveryNativeFactory) {
	native := newNativeDelivery(t)
	eventBus := newLifecycleTransitionEventBus(t)
	trackingBus := &lifecycleTransitionTrackingBus{EventBus: eventBus, resetStarted: make(chan struct{}, 1)}
	fixture := newBlockedManagerLifecycleFixture(t, trackingBus, eventBus, native)

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- fixture.manager.ResetRuntimeState() }()
	waitForManagerShuttingDown(t, fixture.manager)
	go func() { second <- fixture.manager.ResetRuntimeState() }()
	assertLifecycleCallBlocked(t, first, "first reset")
	assertLifecycleCallBlocked(t, second, "second reset")
	select {
	case <-trackingBus.resetStarted:
		t.Fatal("reset cleanup started before shared shutdown completed")
	default:
	}

	fixture.unblock()
	awaitLifecycleCall(t, first, "first reset")
	awaitLifecycleCall(t, second, "second reset")
	if got := trackingBus.resetCalls.Load(); got != 1 {
		t.Fatalf("event bus reset calls = %d, want exactly one shared reset", got)
	}
}

func TestManagerShutdownDuringResetJoinsResetTransition(t *testing.T) {
	eventBus := newLifecycleTransitionEventBus(t)
	releaseReset := make(chan struct{})
	trackingBus := &lifecycleTransitionTrackingBus{
		EventBus: eventBus, resetStarted: make(chan struct{}, 1), resetRelease: releaseReset,
	}
	manager := newTestAgentManager(t, trackingBus, nil)
	reset := make(chan error, 1)
	go func() { reset <- manager.ResetRuntimeState() }()
	select {
	case <-trackingBus.resetStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reset cleanup to start")
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- manager.Shutdown() }()
	assertLifecycleCallBlocked(t, reset, "reset")
	assertLifecycleCallBlocked(t, shutdown, "shutdown during reset")

	close(releaseReset)
	awaitLifecycleCall(t, reset, "reset")
	awaitLifecycleCall(t, shutdown, "shutdown during reset")
	manager.lifecycle.shutdownJoin = trackingBus
	foreign := newRuntimeLifecycleTransition(runtimeLifecycleTransitionShutdown)
	defer foreign.drainCancel()
	manager.lifecycle.joinShutdownDrain(foreign)
	if got := trackingBus.shutdownJoins.Load(); got != 0 {
		t.Fatalf("foreign transition joined current runtime dependencies %d time(s)", got)
	}
}

func ProveNativeManagerSharedShutdownPreservesCallerGraceResults(t *testing.T, newNativeDelivery managerDeliveryNativeFactory) {
	t.Run("accepted_wait_preserves_terminal_and_independent_failures", func(t *testing.T) {
		native := newNativeDelivery(t)
		eventBus := newLifecycleTransitionEventBus(t)
		cached := errors.New("retained terminal diagnostic failure")
		dependencies := &lifecycleTransitionTrackingBus{EventBus: eventBus, shutdownErr: errors.New("independent producer join failure")}
		fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus, native, dependencies)
		fixture.manager.lifecycle.recordLifecycleDiagnosticFailure(cached)
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		waitErr := fixture.manager.WaitForAcceptedWork(cancelled)
		if !errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, cached) {
			t.Errorf("accepted-work wait mixed terminal evidence with its own cancellation: %v", waitErr)
		}
		fixture.unblock()
		ctx, cancelJoin := context.WithTimeout(context.Background(), time.Second)
		defer cancelJoin()
		drainedErr := fixture.manager.WaitForAcceptedWork(ctx)
		if drainedErr != nil {
			t.Errorf("drained accepted work replayed terminal evidence: %v", drainedErr)
		}
		expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancelExpired()
		expiredErr := fixture.manager.WaitForAcceptedWork(expired)
		if expiredErr != nil {
			t.Errorf("cache-only evidence fabricated an expired work wait: %v", expiredErr)
		}
		if err := fixture.manager.WaitForQuiescence(ctx); !errors.Is(err, cached) {
			t.Errorf("diagnostic-bearing quiescence lost cached evidence: %v", err)
		}
		shutdownErr := fixture.manager.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
		result := errors.Join(waitErr, drainedErr, expiredErr, shutdownErr)
		for _, cause := range []error{context.Canceled, cached, dependencies.shutdownErr} {
			if !errors.Is(result, cause) || strings.Count(result.Error(), cause.Error()) != 1 {
				t.Errorf("independent cause not retained exactly once (%v): %v", cause, result)
			}
		}
		if errors.Is(result, context.DeadlineExceeded) {
			t.Errorf("cache-only evidence became a work-drain expiry: %v", result)
		}
		if got := dependencies.shutdownJoins.Load(); got != 1 {
			t.Errorf("dependency joins = %d, want 1", got)
		}
	})
	t.Run("caller_budget_does_not_expire_shared_work", func(t *testing.T) {
		native := newNativeDelivery(t)
		eventBus := newLifecycleTransitionEventBus(t)
		fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus, native)
		transition := fixture.manager.lifecycle.prepareShutdownTransitionWithGrace(true, time.Second)
		short := make(chan error, 1)
		long := make(chan error, 1)
		go func() { short <- fixture.manager.ShutdownWithOptions(ShutdownOptions{Grace: 10 * time.Millisecond}) }()
		go func() { long <- fixture.manager.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}) }()
		assertLifecycleCallBlocked(t, short, "short caller")
		assertLifecycleCallBlocked(t, long, "long caller")
		if err := transition.drainCtx.Err(); err != nil {
			t.Fatalf("caller reporting budget expired shared work: %v", err)
		}
		fixture.unblock()
		select {
		case err := <-short:
			if err == nil {
				t.Fatal("short caller lost its own reporting timeout")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out joining short caller")
		}
		awaitLifecycleCall(t, long, "long caller with successful shared work")
		if transition.result != nil {
			t.Fatalf("caller-only timeout latched as shared failure: %v", transition.result)
		}
	})
	for _, independent := range []bool{false, true} {
		name := "shared_work_expiry"
		if independent {
			name += "_with_independent_failure"
		}
		t.Run(name, func(t *testing.T) {
			proveManagerSharedDrainExpiry(t, newNativeDelivery, independent)
		})
	}
}

func proveManagerSharedDrainExpiry(t *testing.T, newNativeDelivery managerDeliveryNativeFactory, independent bool) {
	t.Helper()
	native := newNativeDelivery(t)
	eventBus := newLifecycleTransitionEventBus(t)
	dependencies := &lifecycleTransitionTrackingBus{EventBus: eventBus}
	if independent {
		dependencies.shutdownErr = errors.New("independent shutdown dependency failure")
	}
	fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus, native, dependencies)
	runCtx, _, _ := fixture.manager.lifecycle.runSnapshot()
	transition := fixture.manager.lifecycle.prepareShutdownTransitionWithGrace(true, 10*time.Millisecond)
	deadline, _ := transition.drainCtx.Deadline()
	short := make(chan error, 1)
	long := make(chan error, 1)
	go func() { short <- fixture.manager.ShutdownWithOptions(ShutdownOptions{Grace: 10 * time.Millisecond}) }()
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("shared work deadline did not cancel held work")
	}
	if transition.drainCtx.Err() != context.DeadlineExceeded {
		t.Fatalf("held work canceled without shared drain expiry: %v", transition.drainCtx.Err())
	}
	selected := fixture.manager.lifecycle.prepareShutdownTransitionWithGrace(true, time.Second)
	if selected != transition {
		t.Fatal("later caller replaced the shared transition")
	}
	if got, _ := selected.drainCtx.Deadline(); !got.Equal(deadline) {
		t.Fatal("later caller granted a fresh work deadline")
	}
	go func() { long <- fixture.manager.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}) }()
	assertLifecycleCallBlocked(t, short, "short-grace shutdown")
	assertLifecycleCallBlocked(t, long, "long-grace shutdown")
	fixture.unblock()
	select {
	case err := <-short:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("short caller lost shared drain expiry: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for short-grace shutdown")
	}
	// Select the exact shared result even if its final join won the scheduling race.
	shared := waitForRuntimeLifecycleTransition(selected, time.Second, "shutdown drain")
	if !errors.Is(shared, context.DeadlineExceeded) {
		t.Errorf("later waiter reported success after shared work expired: %v", shared)
	}
	select {
	case err := <-long:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("long caller lost shared drain expiry: %v", err)
		}
		for _, result := range []error{shared, err} {
			if independent && (!errors.Is(result, dependencies.shutdownErr) || strings.Count(result.Error(), dependencies.shutdownErr.Error()) != 1) {
				t.Errorf("independent failure not retained exactly once: %v", result)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out joining later shutdown caller")
	}
	if got := dependencies.shutdownJoins.Load(); got != 1 {
		t.Errorf("shared dependency joins = %d, want 1", got)
	}
}

func ProveNativeManagerAuthBreakerAndExplicitShutdownJoinOneTransition(t *testing.T, newNativeDelivery managerDeliveryNativeFactory) {
	native := newNativeDelivery(t)
	runtimebus.ResumeRuntimeIngress()
	defer runtimebus.ResumeRuntimeIngress()
	eventBus := newLifecycleTransitionEventBus(t)
	fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus, native)

	identity := testAgentIdentity(t, fixture.manager, "agent-transition", "")
	if !fixture.manager.maybeTripAuthCircuitBreaker(testAuthorActivityContext(context.Background()), identity, fixture.inbound, testAuthFailure()) {
		t.Fatal("auth breaker did not require shared shutdown")
	}
	fixture.manager.lifecycle.requestShutdownTransition()
	waitForManagerShuttingDown(t, fixture.manager)
	shutdown := make(chan error, 1)
	go func() { shutdown <- fixture.manager.Shutdown() }()
	assertLifecycleCallBlocked(t, shutdown, "auth-breaker shared shutdown")

	fixture.unblock()
	awaitLifecycleCall(t, shutdown, "auth-breaker shared shutdown")
}
