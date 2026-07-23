package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

type lifecycleTransitionTrackingBus struct {
	*runtimebus.EventBus
	resetCalls   atomic.Int32
	resetStarted chan struct{}
	resetRelease <-chan struct{}
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
	release   chan struct{}
	cancelRun context.CancelFunc
}

func newBlockedManagerLifecycleFixture(t *testing.T, managerBus Bus, eventBus *runtimebus.EventBus) blockedManagerLifecycleFixture {
	t.Helper()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	agent := shutdownTestAgent{
		id:            "agent-transition",
		subscriptions: []events.EventType{"test.transition"},
		onEvent: func(ctx context.Context, _ events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			<-release
			return nil, ctx.Err()
		},
	}
	manager := newTestAgentManager(t, managerBus, func(runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	})
	if err := manager.spawnAgentInternal(testAuthorActivityContext(context.Background()), PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id, Subscriptions: []string{"test.transition"}},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(managedExecutionTestContext(t, testAuthorActivityContext(context.Background())))
	if err := manager.Run(runCtx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	inbound := eventtest.RunCreatingRootIngress(eventtest.UUID("evt-transition"), events.EventType("test.transition"),
		"tester", "", nil, 0, eventtest.UUID("run-transition"), "", events.EventEnvelope{}, time.Now().UTC())
	if err := eventBus.Publish(testAuthorActivityContext(context.Background()), inbound); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked manager work")
	}
	return blockedManagerLifecycleFixture{manager: manager, bus: eventBus, inbound: inbound, release: release, cancelRun: cancelRun}
}

func newLifecycleTransitionEventBus(t *testing.T) *runtimebus.EventBus {
	t.Helper()
	eventBus, err := runtimebus.NewEventBusWithOptions(nil, runtimebus.EventBusOptions{WorkOwner: newTestManagerWorkOwner(t)})
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

func TestManagerWatcherAndExplicitShutdownJoinOneTransition(t *testing.T) {
	eventBus := newLifecycleTransitionEventBus(t)
	fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus)
	fixture.cancelRun()
	waitForManagerShuttingDown(t, fixture.manager)

	shutdown := make(chan error, 1)
	go func() { shutdown <- fixture.manager.Shutdown() }()
	assertLifecycleCallBlocked(t, shutdown, "explicit shutdown")
	close(fixture.release)
	awaitLifecycleCall(t, shutdown, "explicit shutdown")
}

func TestManagerResetSerializesAfterSharedShutdown(t *testing.T) {
	eventBus := newLifecycleTransitionEventBus(t)
	trackingBus := &lifecycleTransitionTrackingBus{EventBus: eventBus, resetStarted: make(chan struct{}, 1)}
	fixture := newBlockedManagerLifecycleFixture(t, trackingBus, eventBus)

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

	close(fixture.release)
	awaitLifecycleCall(t, shutdown, "shutdown")
	awaitLifecycleCall(t, reset, "reset")
	if got := trackingBus.resetCalls.Load(); got != 1 {
		t.Fatalf("event bus reset calls = %d, want 1", got)
	}
}

func TestManagerConcurrentResetsJoinOneResetTransition(t *testing.T) {
	eventBus := newLifecycleTransitionEventBus(t)
	trackingBus := &lifecycleTransitionTrackingBus{EventBus: eventBus, resetStarted: make(chan struct{}, 1)}
	fixture := newBlockedManagerLifecycleFixture(t, trackingBus, eventBus)

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

	close(fixture.release)
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
}

func TestManagerSharedShutdownPreservesCallerGraceResults(t *testing.T) {
	eventBus := newLifecycleTransitionEventBus(t)
	fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus)
	short := make(chan error, 1)
	long := make(chan error, 1)
	go func() { short <- fixture.manager.ShutdownWithOptions(ShutdownOptions{Grace: 10 * time.Millisecond}) }()
	waitForManagerShuttingDown(t, fixture.manager)
	go func() { long <- fixture.manager.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}) }()
	assertLifecycleCallBlocked(t, short, "short-grace shutdown")
	assertLifecycleCallBlocked(t, long, "long-grace shutdown")

	close(fixture.release)
	select {
	case err := <-short:
		if err == nil {
			t.Fatal("short-grace shutdown returned nil after its reporting budget elapsed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for short-grace shutdown")
	}
	awaitLifecycleCall(t, long, "long-grace shutdown")
}

func TestManagerAuthBreakerAndExplicitShutdownJoinOneTransition(t *testing.T) {
	runtimebus.ResumeRuntimeIngress()
	defer runtimebus.ResumeRuntimeIngress()
	eventBus := newLifecycleTransitionEventBus(t)
	fixture := newBlockedManagerLifecycleFixture(t, eventBus, eventBus)

	if !fixture.manager.maybeTripAuthCircuitBreaker(testAuthorActivityContext(context.Background()), "agent-transition", fixture.inbound, testAuthFailure()) {
		t.Fatal("auth breaker did not require shared shutdown")
	}
	fixture.manager.lifecycle.requestShutdownTransition()
	waitForManagerShuttingDown(t, fixture.manager)
	shutdown := make(chan error, 1)
	go func() { shutdown <- fixture.manager.Shutdown() }()
	assertLifecycleCallBlocked(t, shutdown, "auth-breaker shared shutdown")

	close(fixture.release)
	awaitLifecycleCall(t, shutdown, "auth-breaker shared shutdown")
}
