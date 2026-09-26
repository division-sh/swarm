package manager

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCommittedRouteStateSeparatesLaunchRetirementAndAbsence(t *testing.T) {
	identity := runtimeagentidentitytest.RootRuntime(t, "test-agent", "committed-route-state")
	c := newAgentLifecycleCoordinator(nil, nil, newProjectionTestBus(), nil, nil)
	c.phase = runtimeLifecycleRunning
	c.runMode = AgentRunModeStandard
	c.runCtx = context.Background()
	token := lifecycleToken(identity, 1, 1)
	cell := &agentLifecycleCell{
		identity: identity, epoch: 1, generation: 1,
		phase: AgentLifecycleRegistered, runMode: AgentRunModeStandard,
	}
	c.cells[identity] = cell
	assertState := func(want committedRouteState) {
		t.Helper()
		got, _, err := c.committedRouteStateByIdentity(identity)
		if err != nil || got != want {
			t.Fatalf("committed route state = %d, err=%v; want %d", got, err, want)
		}
	}
	assertState(committedRouteMaterializable)
	cell.phase = AgentLifecycleRunning
	cell.execution = &agentExecutionProjection{
		agent: &projectionTestAgent{id: "test-agent"}, token: token,
		generationCtx: context.Background(), loopDone: make(chan struct{}),
		loopSettled: make(chan struct{}), stopAfterAccepted: make(chan struct{}),
	}
	cell.routeTransitions = 1
	assertState(committedRouteLaunching)
	cell.execution.routeToken = token
	cell.execution.route = make(chan *worklifetime.EventDelivery)
	cell.routeTransitions = 0
	assertState(committedRouteReady)
	cell.retirement = &agentRetirement{token: token}
	assertState(committedRouteRetiring)
	cell.retirement.token = runtimeeffects.LifecycleToken{}
	assertState(committedRouteUnavailable)
	cell.routeTransitions = 1
	assertState(committedRouteLaunching)
	cell.routeTransitions = 0
	cell.retirement.token = token
	cell.retirement.joinErr = errors.New("retirement settlement failed")
	assertState(committedRouteUnavailable)
	cell.retirement = nil
	cell.terminalSet = &terminalFlowRetirement{}
	assertState(committedRouteRetiring)
	cell.terminalSet.completed = true
	assertState(committedRouteUnavailable)
	cell.terminalSet = nil
	cell.phase = AgentLifecycleFailed
	assertState(committedRouteUnavailable)
	cell.phase = AgentLifecycleTerminated
	assertState(committedRouteUnavailable)
	delete(c.cells, identity)
	assertState(committedRouteAbsent)
}

func TestCommittedRouteFinalizationPreservesIndependentFailures(t *testing.T) {
	identity := runtimeagentidentitytest.RootRuntime(t, "test-agent", "committed-route-errors")
	c := newAgentLifecycleCoordinator(nil, nil, newProjectionTestBus(), nil, nil)
	c.phase = runtimeLifecycleRunning
	cell := &agentLifecycleCell{identity: identity, phase: AgentLifecycleRegistered, routeTransitions: 1}
	c.cells[identity] = cell
	am := &AgentManager{lifecycle: c}
	retirement := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_retirement_pending", "agent-lifecycle", "lifecycle_mutation", nil)
	independent := errors.New("independent cleanup failed")

	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if canceled {
			cancel()
		}
		pure := am.committedRouteFinalizeError(ctx, identity, retirement)
		if canceled {
			if !errors.Is(pure, context.Canceled) {
				t.Errorf("canceled transition = %v, want owning cancellation", pure)
			}
			owned := am.committedRouteFinalizeError(ctx, identity, errors.Join(retirement, context.Canceled))
			if !errors.Is(owned, context.Canceled) {
				t.Errorf("owning cancellation and transition = %v, want cancellation", owned)
			}
		} else if !errors.Is(pure, runtimebus.ErrCommittedAgentRouteTransition) {
			t.Errorf("launching transition = %v, want transient route marker", pure)
		}
		for _, cause := range []error{errors.Join(retirement, independent), errors.Join(ErrAgentNotFound, independent), errors.Join(retirement, context.Canceled, independent)} {
			got := am.committedRouteFinalizeError(ctx, identity, cause)
			if !errors.Is(got, independent) || errors.Is(got, runtimebus.ErrCommittedAgentRouteTransition) {
				t.Errorf("canceled=%t cause=%v lost independent failure: %v", canceled, cause, got)
			}
		}
		cancel()
	}
	if got := am.committedRouteFinalizeError(context.Background(), identity, ErrAgentNotFound); !errors.Is(got, runtimebus.ErrCommittedAgentRouteTransition) {
		t.Fatalf("pure launching not-found = %v, want transient route marker", got)
	}

	cell.routeTransitions = 0
	cell.phase = AgentLifecycleRunning
	cell.epoch, cell.generation = 1, 1
	cell.runMode = AgentRunModeStandard
	c.runMode = AgentRunModeStandard
	c.runCtx = context.Background()
	token := lifecycleToken(identity, cell.epoch, cell.generation)
	cell.execution = &agentExecutionProjection{
		agent: &projectionTestAgent{id: "test-agent"}, token: token, routeToken: token,
		generationCtx: context.Background(), loopDone: make(chan struct{}),
		loopSettled: make(chan struct{}), stopAfterAccepted: make(chan struct{}),
		route: make(chan *worklifetime.EventDelivery),
	}
	if state, _, err := c.committedRouteStateByIdentity(identity); err != nil || state != committedRouteReady {
		t.Fatalf("ready fixture classified as state %d: %v", state, err)
	}
	if got := am.committedRouteFinalizeError(context.Background(), identity, retirement); got != nil {
		t.Fatalf("settled ready transition = %v, want nil", got)
	}
	if got := am.committedRouteFinalizeError(context.Background(), identity, errors.Join(retirement, independent)); !errors.Is(got, independent) {
		t.Fatalf("ready route lost independent failure: %v", got)
	}
	cell.retirement = &agentRetirement{token: token}
	if state, _, err := c.committedRouteStateByIdentity(identity); err != nil || state != committedRouteRetiring {
		t.Fatalf("retiring fixture classified as state %d: %v", state, err)
	}
	if got := am.committedRouteFinalizeError(context.Background(), identity, retirement); !errors.Is(got, runtimebus.ErrCommittedAgentRouteTransition) {
		t.Fatalf("pure retirement = %v, want transient route marker", got)
	}
	if got := am.committedRouteFinalizeError(context.Background(), identity, errors.Join(retirement, independent)); !errors.Is(got, independent) {
		t.Fatalf("retiring route lost independent failure: %v", got)
	}
	cell.retirement = nil
	cell.phase = AgentLifecycleFailed
	if got := am.committedRouteFinalizeError(context.Background(), identity, retirement); errors.Is(got, runtimebus.ErrCommittedAgentRouteTransition) {
		t.Fatalf("failed agent became transient: %v", got)
	}
}

func TestCommittedRouteTransitionCompletionDoesNotReleaseSuccessor(t *testing.T) {
	identity := runtimeagentidentitytest.RootRuntime(t, "test-agent", "committed-route-successor")
	bus := newProjectionTestBus()
	c := newAgentLifecycleCoordinator(nil, nil, bus, nil, nil)
	c.phase = runtimeLifecycleRunning
	predecessor := &agentLifecycleCell{identity: identity, routeTransitions: 1}
	successor := &agentLifecycleCell{identity: identity, routeTransitions: 1}
	c.cells[identity] = successor
	am := &AgentManager{lifecycle: c}
	am.completeCommittedRouteTransition(predecessor)
	if predecessor.routeTransitions != 0 || successor.routeTransitions != 1 {
		t.Fatalf("transition counts predecessor=%d successor=%d, want 0 and 1", predecessor.routeTransitions, successor.routeTransitions)
	}
	if got := bus.signals.Load(); got != 1 {
		t.Fatalf("transition completion signals = %d, want 1", got)
	}
}

func TestCommittedRouteRetirementCompletionSignalsAfterStateClears(t *testing.T) {
	identity := runtimeagentidentitytest.RootRuntime(t, "test-agent", "committed-route-retirement")
	bus := newProjectionTestBus()
	c := newAgentLifecycleCoordinator(nil, nil, bus, nil, nil)
	execution := &agentExecutionProjection{}
	cell := &agentLifecycleCell{identity: identity, phase: AgentLifecycleTerminated, execution: execution}
	retirement := &agentRetirement{cell: cell, execution: execution}
	cell.retirement = retirement
	c.cells[identity] = cell
	c.finishAgentRetirement(retirement)
	if cell.retirement != nil || cell.execution != nil {
		t.Fatalf("retirement completion left live projection: %#v", cell)
	}
	if got := bus.signals.Load(); got != 1 {
		t.Fatalf("retirement completion signals = %d, want 1", got)
	}
}

func TestCommittedRouteLaunchHasLivePublicationOwner(t *testing.T) {
	bus := newProjectionTestBus()
	entered := make(chan struct{})
	release := make(chan struct{})
	var publishOnce sync.Once
	var releaseOnce sync.Once
	releasePublication := func() { releaseOnce.Do(func() { close(release) }) }
	bus.beforePublish = func() {
		publishOnce.Do(func() {
			close(entered)
			<-release
		})
	}
	defer releasePublication()
	am := newProjectionTestManager(t, bus, (&projectionTestFactory{}).Build)
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancelRun()
		if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
			t.Errorf("shutdown manager: %v", err)
		}
	})
	identity := runtimeagentidentitytest.RootRuntime(t, "launching-agent", "committed-route-launch")
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: "launching-agent", Identity: identity,
		Subscriptions: []string{"test.old"},
	})
	result := make(chan error, 1)
	go func() { result <- spawnManagerTestAgent(am, cfg) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("route publication did not reach the barrier")
	}
	state, _, err := am.lifecycle.committedRouteStateByIdentity(identity)
	if err != nil || state != committedRouteLaunching {
		t.Fatalf("held publication state = %d, err=%v; want launching", state, err)
	}
	signalsBeforeRelease := bus.signals.Load()
	releasePublication()
	if err := <-result; err != nil {
		t.Fatalf("complete route publication: %v", err)
	}
	state, _, err = am.lifecycle.committedRouteStateByIdentity(identity)
	if err != nil || state != committedRouteReady {
		t.Fatalf("published route state = %d, err=%v; want ready", state, err)
	}
	if got := bus.signals.Load(); got <= signalsBeforeRelease {
		t.Fatalf("publication completion signals = %d, want more than %d", got, signalsBeforeRelease)
	}
}

func TestPersistedExecutableAdoptionStartsInCurrentManagerOccurrence(t *testing.T) {
	for _, mode := range []struct {
		name string
		run  func(*AgentManager, context.Context) error
		want AgentRunMode
	}{
		{name: "standard", run: (*AgentManager).Run, want: AgentRunModeStandard},
		{name: "authoritative_delivery_only", run: (*AgentManager).RunAuthoritativeDeliveryOnly, want: AgentRunModeAuthoritativeDeliveryOnly},
	} {
		t.Run(mode.name, func(t *testing.T) {
			bus := newProjectionTestBus()
			handled := make(chan int, 1)
			factory := &projectionTestFactory{handled: handled}
			am := newProjectionTestManager(t, bus, factory.Build)
			runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
			if err := mode.run(am, managedExecutionTestContext(t, runCtx)); err != nil {
				t.Fatalf("start manager: %v", err)
			}
			t.Cleanup(func() {
				cancelRun()
				if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
					t.Errorf("shutdown manager: %v", err)
				}
			})

			const agentID = "restored-dynamic-agent"
			rec, err := managerTestStaticAgentRecord(am, managerTestAgentConfig(models.AgentConfig{
				ExecutionMode: "live",
				ID:            agentID,
				Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "persisted-adoption-test"),
				Subscriptions: []string{"test.old"},
			}))
			if err != nil {
				t.Fatalf("construct persisted agent: %v", err)
			}
			if err := am.adoptPersistedAgentForLifecycle(testAuthorActivityContext(context.Background()), am.semanticSource, rec); err != nil {
				t.Fatalf("adopt persisted agent: %v", err)
			}
			identity, err := rec.Config.ConcreteIdentity()
			if err != nil {
				t.Fatal(err)
			}
			readiness, err := am.lifecycle.executableReadinessByIdentity(identity)
			if err != nil {
				t.Fatalf("executable readiness: %v", err)
			}
			if readiness.Kind != executableAgentRunnableCurrentOccurrence || readiness.State.Phase != AgentLifecycleRunning || readiness.State.RunMode != mode.want {
				t.Fatalf("readiness = %#v, want current %s occurrence", readiness, mode.want)
			}
			route, ok := bus.current(agentID)
			wantToken := lifecycleToken(identity, readiness.State.RuntimeEpoch, readiness.State.Generation)
			if !ok || route.token != wantToken {
				t.Fatalf("route = %#v found=%t, want exact token %#v", route, ok, wantToken)
			}
			if mode.want == AgentRunModeStandard && len(route.subscriptions) != 1 {
				t.Fatalf("standard route subscriptions = %#v", route.subscriptions)
			}
			if mode.want == AgentRunModeAuthoritativeDeliveryOnly && len(route.subscriptions) != 0 {
				t.Fatalf("authoritative route subscriptions = %#v, want carrier-only", route.subscriptions)
			}

			generation := readiness.State.Generation
			if _, err := am.ensureExecutableAgentLifecycle(testAuthorActivityContext(context.Background()), identity); err != nil {
				t.Fatalf("exact readiness replay: %v", err)
			}
			replayed, err := am.lifecycle.executableReadinessByIdentity(identity)
			if err != nil || replayed.State.Generation != generation || len(bus.routeHistory(agentID)) != 1 {
				t.Fatalf("readiness replay = %#v err=%v routes=%d, want generation %d and one route", replayed, err, len(bus.routeHistory(agentID)), generation)
			}

			if err := bus.send(agentID, projectionRuntimeEvent("persisted-adoption-"+mode.name, events.EventType("test.old"))); err != nil {
				t.Fatalf("send exact receiver turn: %v", err)
			}
			select {
			case build := <-handled:
				if build != 1 {
					t.Fatalf("receiver build = %d, want adopted build 1", build)
				}
			case <-time.After(time.Second):
				t.Fatal("adopted exact route did not execute a receiver turn")
			}
		})
	}
}

func TestExecutableAgentPreparationUpgradesExactlyOnceWhenManagerStarts(t *testing.T) {
	bus := newProjectionTestBus()
	handled := make(chan int, 1)
	factory := &projectionTestFactory{handled: handled}
	am := newProjectionTestManager(t, bus, factory.Build)
	const agentID = "prepared-dynamic-agent"
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "prepared-agent-test"),
		Subscriptions: []string{"test.old"},
	})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatalf("prepare agent: %v", err)
	}
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := am.lifecycle.executableReadinessByIdentity(identity)
	if err != nil {
		t.Fatalf("prepared readiness: %v", err)
	}
	if prepared.Kind != executableAgentPreparedBeforeRun || prepared.State.Phase != AgentLifecycleRegistered || prepared.State.RunMode != AgentRunModeStopped {
		t.Fatalf("prepared readiness = %#v", prepared)
	}
	if _, ok := bus.current(agentID); ok {
		t.Fatal("pre-run preparation installed an executable route")
	}

	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
			t.Errorf("shutdown manager: %v", err)
		}
	})
	runnable, err := am.lifecycle.executableReadinessByIdentity(identity)
	if err != nil {
		t.Fatalf("running readiness: %v", err)
	}
	if runnable.Kind != executableAgentRunnableCurrentOccurrence || runnable.State.Generation != prepared.State.Generation+1 {
		t.Fatalf("running readiness = %#v, want one successor after %#v", runnable, prepared)
	}
	if _, err := am.ensureExecutableAgentLifecycle(testAuthorActivityContext(context.Background()), identity); err != nil {
		t.Fatalf("repeated ensure: %v", err)
	}
	after, err := am.lifecycle.executableReadinessByIdentity(identity)
	if err != nil || after.State.Generation != runnable.State.Generation || len(bus.routeHistory(agentID)) != 1 || factory.builds != 1 {
		t.Fatalf("upgrade replay = %#v err=%v routes=%d builds=%d", after, err, len(bus.routeHistory(agentID)), factory.builds)
	}
}

func TestExecutablePreparationAcceptsTakenOverRunningDurableState(t *testing.T) {
	am := newProjectionTestManager(t, newProjectionTestBus(), (&projectionTestFactory{handled: make(chan int, 1)}).Build)
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: "taken-over-agent",
		Identity:      runtimeagentidentitytest.RootRuntime(t, "taken-over-agent", "taken-over-test"),
		Subscriptions: []string{"test.old"},
	})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatal(err)
	}
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		t.Fatal(err)
	}
	am.lifecycle.mu.Lock()
	cell := am.lifecycle.cells[identity]
	cell.phase = AgentLifecycleRunning
	cell.runMode = AgentRunModeStandard
	am.lifecycle.mu.Unlock()
	readiness, err := am.lifecycle.executableReadinessByIdentity(identity)
	if err != nil || readiness.Kind != executableAgentPreparedBeforeRun || readiness.State.Phase != AgentLifecycleRunning {
		t.Fatalf("taken-over readiness = %#v err=%v", readiness, err)
	}
}

func TestFreshSpawnDuringLiveManagerReturnsRunnableProjection(t *testing.T) {
	bus := newProjectionTestBus()
	factory := &projectionTestFactory{handled: make(chan int, 1)}
	am := newProjectionTestManager(t, bus, factory.Build)
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
			t.Errorf("shutdown manager: %v", err)
		}
	})
	const agentID = "fresh-live-agent"
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "fresh-live-test"),
		Subscriptions: []string{"test.old"},
	})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatalf("spawn during run: %v", err)
	}
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		t.Fatal(err)
	}
	readiness, err := am.lifecycle.executableReadinessByIdentity(identity)
	if err != nil || readiness.Kind != executableAgentRunnableCurrentOccurrence || len(bus.routeHistory(agentID)) != 1 {
		t.Fatalf("fresh live readiness = %#v err=%v routes=%d", readiness, err, len(bus.routeHistory(agentID)))
	}
}

func TestExecutableReadinessRejectsPartialLiveManagerProjections(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*agentLifecycleCell)
	}{
		{name: "registered_stopped", mutate: func(cell *agentLifecycleCell) {
			cell.phase = AgentLifecycleRegistered
			cell.runMode = AgentRunModeStopped
		}},
		{name: "failed", mutate: func(cell *agentLifecycleCell) { cell.phase = AgentLifecycleFailed }},
		{name: "terminal", mutate: func(cell *agentLifecycleCell) { cell.phase = AgentLifecycleTerminated }},
		{name: "fenced", mutate: func(cell *agentLifecycleCell) { cell.execution.fenced = true }},
		{name: "wrong_mode", mutate: func(cell *agentLifecycleCell) { cell.runMode = AgentRunModeAuthoritativeDeliveryOnly }},
		{name: "missing_route", mutate: func(cell *agentLifecycleCell) {
			cell.execution.route = nil
			cell.execution.routeToken = runtimeeffects.LifecycleToken{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			bus := newProjectionTestBus()
			am := newProjectionTestManager(t, bus, (&projectionTestFactory{handled: make(chan int, 1)}).Build)
			runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
			if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
				t.Fatalf("Run: %v", err)
			}
			defer func() {
				cancelRun()
				_ = am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
			}()
			const agentID = "partial-live-agent"
			cfg := managerTestAgentConfig(models.AgentConfig{
				ExecutionMode: "live", ID: agentID,
				Identity: runtimeagentidentitytest.RootRuntime(t, agentID, "partial-live-test"),
			})
			if err := spawnManagerTestAgent(am, cfg); err != nil {
				t.Fatalf("spawn live agent: %v", err)
			}
			identity, err := cfg.ConcreteIdentity()
			if err != nil {
				t.Fatal(err)
			}
			cell, ok := testLifecycleCell(t, am.lifecycle, agentID, "")
			if !ok {
				t.Fatal("lifecycle cell missing")
			}
			am.lifecycle.mu.Lock()
			test.mutate(cell)
			am.lifecycle.mu.Unlock()
			if readiness, err := am.lifecycle.executableReadinessByIdentity(identity); err == nil {
				t.Fatalf("partial projection accepted as %#v", readiness)
			}
		})
	}
}

func TestLifecycleOnlyAdoptionNeverStartsObsoleteAgent(t *testing.T) {
	bus := newProjectionTestBus()
	builds := 0
	am := newProjectionTestManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		builds++
		return &projectionTestAgent{id: cfg.ID, handled: make(chan int, 1)}, nil
	})
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		_ = am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	})
	const agentID = "obsolete-agent"
	rec, err := managerTestStaticAgentRecord(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "lifecycle-only-test"),
		Subscriptions: []string{"test.old"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := am.adoptPersistedAgentLifecycleOnly(testAuthorActivityContext(context.Background()), rec); err != nil {
		t.Fatalf("lifecycle-only adoption: %v", err)
	}
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if builds != 0 {
		t.Fatalf("lifecycle-only adoption built %d provider agents", builds)
	}
	if _, ok := bus.current(agentID); ok {
		t.Fatal("lifecycle-only adoption installed an executable route")
	}
	wantRevision, err := lifecycleConfigRevision(rec)
	if err != nil {
		t.Fatal(err)
	}
	state, ok := am.lifecycle.stateByIdentity(identity)
	if !ok || state.ConfigRevision != wantRevision {
		t.Fatalf("lifecycle-only revision = %#v found=%t, want %s", state, ok, wantRevision)
	}
	projection, ok := am.lifecycle.executionSnapshotByIdentity(identity)
	if !ok || len(projection.Subscriptions) != 0 || len(projection.Config.Subscriptions) != 1 || projection.Config.Subscriptions[0] != "test.old" {
		t.Fatalf("lifecycle-only projection lost durable config or admitted executable subscriptions: %#v found=%t", projection, ok)
	}
	if readiness, err := am.lifecycle.executableReadinessByIdentity(identity); err == nil {
		t.Fatalf("lifecycle-only projection accepted as %#v", readiness)
	}
	if err := am.teardownIdentityWithTopology(testAuthorActivityContext(context.Background()), identity, "teardown", &rec.Topology); err != nil {
		t.Fatalf("teardown lifecycle-only identity: %v", err)
	}
	state, ok = am.lifecycle.stateByIdentity(identity)
	if !ok || state.Phase != AgentLifecycleTerminated {
		t.Fatalf("teardown state = %#v found=%t", state, ok)
	}
}

func TestPersistedAdoptionOwnerFenceCompensatesWithoutRouteOrLoop(t *testing.T) {
	bus := newProjectionTestBus()
	am := newProjectionTestManager(t, bus, (&projectionTestFactory{handled: make(chan int, 1)}).Build)
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	defer cancelRun()
	const agentID = "adoption-fence-agent"
	rec, err := managerTestStaticAgentRecord(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "adoption-fence-test"),
		Subscriptions: []string{"test.old"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	bus.beforePublish = func() { am.lifecycle.requestShutdownTransition() }
	if err := am.adoptPersistedAgentForLifecycle(testAuthorActivityContext(context.Background()), am.semanticSource, rec); err == nil {
		t.Fatal("persisted adoption succeeded after manager owner fence")
	}
	if _, ok := bus.current(agentID); ok {
		t.Fatal("failed adoption left a reachable route")
	}
	cell, ok := testLifecycleCell(t, am.lifecycle, agentID, "")
	if !ok {
		t.Fatal("compensated adoption lifecycle cell missing")
	}
	am.lifecycle.mu.Lock()
	phase := cell.phase
	loopDone := cell.execution.loopDone
	route := cell.execution.route
	routeToken := cell.execution.routeToken
	am.lifecycle.mu.Unlock()
	if phase != AgentLifecycleRegistered || loopDone != nil || route != nil || routeToken.Valid() {
		t.Fatalf("compensated adoption phase=%q loop=%t route=%t token=%#v", phase, loopDone != nil, route != nil, routeToken)
	}
	if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
		t.Fatalf("join fenced manager shutdown: %v", err)
	}
}

func TestDynamicReadinessRejectsRegisteredStoppedAsProcessReady(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	first := newFlowActivationManager(t, firstBus, instances, agents)
	bundle := testFlowBundle(t, "")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(first, ctx, req); err != nil {
		t.Fatalf("activate first process: %v", err)
	}
	persisted, err := agents.LoadAgents(ctx)
	if err != nil || len(persisted) != 1 {
		t.Fatalf("persisted agents = %#v err=%v", persisted, err)
	}

	restartBus := &flowActivationTestBus{routeStore: firstBus.routeStore}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	runCtx, cancelRun := context.WithCancel(ctx)
	if err := restarted.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("start restarted manager: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		_ = restarted.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	})
	rec := persisted[0]
	agent, err := restarted.buildAgent(rec.Config)
	if err != nil {
		t.Fatalf("build persisted agent: %v", err)
	}
	admission, err := admitAgentConfigSubscriptions(semanticview.Wrap(bundle), &rec.Config, nil)
	if err != nil {
		t.Fatalf("admit persisted subscriptions: %v", err)
	}
	if err := restarted.lifecycle.registerExecutionWithTopology(ctx, rec, false, agent, admission, rec.Topology, nil); err != nil {
		t.Fatalf("seed registered/stopped projection: %v", err)
	}
	if err := restarted.verifyDynamicFlowAgents(ctx, testActivationFlowIdentity(req), persisted, rec.Topology); err == nil || !strings.Contains(err.Error(), "agent_execution_not_ready") {
		t.Fatalf("partial readiness error = %v", err)
	}
	if restartBus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
		t.Fatal("partial executable projection published a process flow route")
	}
}

func TestSourceScopedCompletedTopologyReconstructsIntoLiveManagerOccurrence(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	first := newFlowActivationManager(t, firstBus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(first, ctx, req); err != nil {
		t.Fatalf("activate first process: %v", err)
	}
	if err := first.Shutdown(); err != nil {
		t.Fatalf("retire first process before reconstruction: %v", err)
	}

	restartBus := &flowActivationTestBus{routeStore: firstBus.routeStore}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	runCtx, cancelRun := context.WithCancel(ctx)
	if err := restarted.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("start restarted manager: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		_ = restarted.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	})
	armedBefore := len(instances.armedEntries)
	if err := reconcileDynamicFlowRuntimeStartupForTest(restarted, ctx, authorActivityTestSourceArtifactFact, false); err != nil {
		t.Fatalf("ReconstructDynamicFlowRuntimeStartupTopology: %v", err)
	}
	if !restartBus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
		t.Fatal("completed topology did not publish its persisted process route")
	}
	if len(instances.armedEntries) != armedBefore || len(restartBus.published) != 0 {
		t.Fatalf("topology-only reconstruction replayed durable work: timers=%d/%d events=%d", len(instances.armedEntries), armedBefore, len(restartBus.published))
	}
	for _, agentID := range []string{"reviewer", "writer"} {
		cfg, ok := testAgentConfigForRun(t, restarted, req.TriggerEvent.RunID(), agentID, req.Instance.InstancePath)
		if !ok {
			t.Fatalf("restored agent %s missing", agentID)
		}
		identity, err := cfg.ConcreteIdentity()
		if err != nil {
			t.Fatal(err)
		}
		readiness, err := restarted.lifecycle.executableReadinessByIdentity(identity)
		if err != nil || readiness.Kind != executableAgentRunnableCurrentOccurrence || readiness.State.RunMode != AgentRunModeStandard {
			t.Fatalf("restored agent %s readiness = %#v err=%v", agentID, readiness, err)
		}
	}
}

func TestSourceScopedStartupPreparesEverySiblingBeforeFirstPendingFinalizer(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	routeStore := &flowActivationTestRouteStore{}
	firstBus := &flowActivationTestBus{routeStore: routeStore}
	first := newFlowActivationManager(t, firstBus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "")
	requests := []runtimepipeline.FlowInstanceActivationRequest{
		testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"),
		testActivationRequest(bundle, "review", "inst-2", "ent-2", "review/inst-2"),
	}
	ctx := testAuthorActivityContext(context.Background())
	for _, req := range requests {
		if err := activateFlowInstanceForTest(first, ctx, req); err != nil {
			t.Fatalf("activate initial process %s: %v", req.Instance.InstancePath, err)
		}
	}
	if err := first.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
		t.Fatalf("retire initial process before startup reconstruction: %v", err)
	}

	instances.readinessMu.Lock()
	for _, req := range requests {
		key := flowActivationReadinessKey(req.TriggerEvent.RunID(), req.Instance.InstancePath)
		readiness := instances.readiness[key]
		readiness.TopologyReadyAt = time.Time{}
		instances.readiness[key] = readiness
	}
	instances.readinessMu.Unlock()
	armsBefore := len(instances.armedEntries)

	restartBus := &flowActivationTestBus{routeStore: routeStore}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	startup, err := restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, true)
	if err != nil {
		t.Fatalf("canonicalize sibling startup topology: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	if err := restarted.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		cancelRun()
		t.Fatalf("start sibling topology manager: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		_ = restarted.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	})

	verifiedBeforeFirstFinalizer := false
	instances.armInitialEntry = func(string) error {
		if verifiedBeforeFirstFinalizer {
			return nil
		}
		if len(instances.armedEntries) != armsBefore+1 {
			t.Fatalf("first startup finalizer observed %d timer arms after baseline %d", len(instances.armedEntries), armsBefore)
		}
		for _, req := range requests {
			if !restartBus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
				t.Fatalf("flow route %s was absent before the first pending finalizer", req.Instance.InstancePath)
			}
			for _, agentID := range []string{"reviewer", "writer"} {
				cfg, ok := testAgentConfigForRun(t, restarted, req.TriggerEvent.RunID(), agentID, req.Instance.InstancePath)
				if !ok {
					t.Fatalf("agent %s@%s was absent before the first pending finalizer", agentID, req.Instance.InstancePath)
				}
				identity, identityErr := cfg.ConcreteIdentity()
				if identityErr != nil {
					t.Fatal(identityErr)
				}
				readiness, readinessErr := restarted.lifecycle.executableReadinessByIdentity(identity)
				if readinessErr != nil || readiness.Kind != executableAgentRunnableCurrentOccurrence || readiness.State.RunMode != AgentRunModeStandard {
					t.Fatalf("agent %s@%s readiness before first finalizer = %#v err=%v", agentID, req.Instance.InstancePath, readiness, readinessErr)
				}
			}
		}
		verifiedBeforeFirstFinalizer = true
		return nil
	}
	if err := restarted.CompleteDynamicFlowRuntimeStartupTopology(ctx, startup); err != nil {
		t.Fatalf("complete sibling startup topology: %v", err)
	}
	if !verifiedBeforeFirstFinalizer || len(instances.armedEntries) != armsBefore+len(requests) {
		t.Fatalf("startup finalization barrier verified=%t timer arms=%d want=%d", verifiedBeforeFirstFinalizer, len(instances.armedEntries), armsBefore+len(requests))
	}
	for _, req := range requests {
		readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
		if err != nil || !found || readiness.Pending() {
			t.Fatalf("completed sibling readiness %s: found=%v readiness=%#v err=%v", req.Instance.InstancePath, found, readiness, err)
		}
	}
}

// Compile-time assertions keep the focused fixture on the real route and
// lifecycle interfaces used by production.
var _ AgentRouteBus = (*projectionTestBus)(nil)
var _ AgentRouteBus = (*flowActivationTestBus)(nil)
