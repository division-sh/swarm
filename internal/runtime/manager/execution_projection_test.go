package manager

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type projectionTestRoute struct {
	token         runtimeeffects.LifecycleToken
	channel       chan *worklifetime.EventDelivery
	subscriptions []events.EventType
}

type projectionTestBus struct {
	mu            sync.Mutex
	routes        map[string]projectionTestRoute
	history       map[string][]projectionTestRoute
	removed       []runtimeeffects.LifecycleToken
	store         runtimebus.EventStore
	owner         worklifetime.Occurrence
	prepareErr    bool
	publishErr    error
	runtimeLogs   []runtimepipeline.RuntimeLogEntry
	beforePublish func()
}

type projectionTestRoutePreparation struct {
	bus       *projectionTestBus
	route     projectionTestRoute
	published bool
	discarded bool
}

func (p *projectionTestRoutePreparation) Deliveries() <-chan *worklifetime.EventDelivery {
	if p == nil {
		return nil
	}
	return p.route.channel
}

func (p *projectionTestRoutePreparation) Publish() error {
	if p == nil || p.bus == nil || p.discarded {
		return context.Canceled
	}
	if p.bus.beforePublish != nil {
		p.bus.beforePublish()
	}
	p.bus.mu.Lock()
	defer p.bus.mu.Unlock()
	if p.bus.publishErr != nil {
		return p.bus.publishErr
	}
	p.bus.routes[p.route.token.AgentID] = p.route
	p.bus.history[p.route.token.AgentID] = append(p.bus.history[p.route.token.AgentID], p.route)
	p.published = true
	return nil
}

func (p *projectionTestRoutePreparation) Discard() error {
	if p == nil || p.discarded {
		return nil
	}
	p.discarded = true
	if p.published {
		p.bus.RemoveAgentRoute(p.route.token)
	}
	return nil
}

func newProjectionTestBus() *projectionTestBus {
	return &projectionTestBus{routes: map[string]projectionTestRoute{}, history: map[string][]projectionTestRoute{}}
}

func newProjectionTestManager(t *testing.T, bus Bus, factory AgentFactory, stores ...ManagerPersistence) *AgentManager {
	t.Helper()
	owner := newTestManagerWorkOwner(t)
	if projectionBus, ok := bus.(*projectionTestBus); ok {
		projectionBus.owner = owner
	}
	return newTestAgentManagerWithOptions(t, bus, factory, AgentManagerOptions{WorkOwner: owner}, stores...)
}

func (*projectionTestBus) Publish(context.Context, events.Event) error { return nil }
func (b *projectionTestBus) PublishDirect(_ context.Context, event events.Event, recipients []string) error {
	for _, recipient := range recipients {
		if err := b.send(recipient, event); err != nil {
			return err
		}
	}
	return nil
}
func (b *projectionTestBus) PublishPersistedRecipients(ctx context.Context, event events.Event, recipients []string) error {
	return b.PublishDirect(ctx, event, recipients)
}
func (*projectionTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	panic("generic agent Subscribe must not be used")
}
func (*projectionTestBus) Unsubscribe(string)             { panic("generic agent Unsubscribe must not be used") }
func (b *projectionTestBus) Store() runtimebus.EventStore { return b.store }
func (*projectionTestBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}
func (*projectionTestBus) SweepPipelineObligations(context.Context, int) (runtimepipelineobligation.SweepResult, error) {
	return runtimepipelineobligation.SweepResult{Exhausted: true}, nil
}
func (*projectionTestBus) PipelineWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}
func (*projectionTestBus) ResetInMemoryState() error { return nil }

func (b *projectionTestBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.mu.Lock()
	b.runtimeLogs = append(b.runtimeLogs, entry)
	b.mu.Unlock()
	return nil
}
func (b *projectionTestBus) PrepareAgentRoute(token runtimeeffects.LifecycleToken, admission semanticview.FlowOwnedAgentSubscriptionAdmission) runtimebus.AgentRoutePreparation {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.prepareErr {
		return nil
	}
	patterns := admission.RoutePatterns()
	subscriptions := make([]events.EventType, 0, len(patterns))
	for _, pattern := range patterns {
		subscriptions = append(subscriptions, events.EventType(pattern))
	}
	route := projectionTestRoute{
		token: token, channel: make(chan *worklifetime.EventDelivery, 128),
		subscriptions: append([]events.EventType(nil), subscriptions...),
	}
	return &projectionTestRoutePreparation{bus: b, route: route}
}

func (b *projectionTestBus) ReplaceAgentRoute(token runtimeeffects.LifecycleToken, admission semanticview.FlowOwnedAgentSubscriptionAdmission) <-chan *worklifetime.EventDelivery {
	prepared := b.PrepareAgentRoute(token, admission)
	if prepared == nil || prepared.Publish() != nil {
		return nil
	}
	return prepared.Deliveries()
}
func (b *projectionTestBus) RemoveAgentRoute(token runtimeeffects.LifecycleToken) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removed = append(b.removed, token)
	if route, ok := b.routes[token.AgentID]; ok && route.token == token {
		delete(b.routes, token.AgentID)
	}
}
func (b *projectionTestBus) send(agentID string, event events.Event) error {
	b.mu.Lock()
	route, ok := b.routes[agentID]
	b.mu.Unlock()
	if ok {
		delivery, err := b.owner.NewRoutedEventDelivery(testAuthorActivityContext(context.Background()), event, events.DeliveryRoute{
			SubscriberType: string(runtimedelivery.SubscriberAgent),
			SubscriberID:   agentID,
		})
		if err != nil {
			return err
		}
		route.channel <- delivery
	}
	return nil
}
func (b *projectionTestBus) current(agentID string) (projectionTestRoute, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	route, ok := b.routes[agentID]
	return route, ok
}
func (b *projectionTestBus) routeHistory(agentID string) []projectionTestRoute {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]projectionTestRoute(nil), b.history[agentID]...)
}

type projectionTestAgent struct {
	id      string
	build   int
	subs    []events.EventType
	handled chan<- int
}

type projectionDirectiveAgent struct {
	projectionTestAgent
	boardStarted chan<- runtimeeffects.LifecycleToken
	boardRelease <-chan struct{}
}

type projectionBacklogAgent struct {
	projectionTestAgent
	eventStarted chan<- runtimeeffects.LifecycleToken
	eventRelease <-chan struct{}
}

func (a *projectionBacklogAgent) OnEvent(ctx context.Context, _ events.Event) ([]events.Event, error) {
	token, _ := runtimeeffects.LifecycleTokenFromContext(ctx)
	a.eventStarted <- token
	<-a.eventRelease
	return nil, nil
}

func (a *projectionDirectiveAgent) BoardStep(ctx context.Context, _ runtimeagentcontrol.BoardDirective) (string, error) {
	token, _ := runtimeeffects.LifecycleTokenFromContext(ctx)
	a.boardStarted <- token
	<-a.boardRelease
	return "ok", nil
}

func (a *projectionTestAgent) ID() string { return a.id }
func (*projectionTestAgent) Type() string { return "projection-test" }
func (a *projectionTestAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subs...)
}
func (a *projectionTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	a.handled <- a.build
	return nil, nil
}

type projectionTestFactory struct {
	mu            sync.Mutex
	builds        int
	secondStarted chan struct{}
	releaseSecond <-chan struct{}
	handled       chan<- int
}

func (f *projectionTestFactory) Build(cfg models.AgentConfig) (Agent, error) {
	f.mu.Lock()
	f.builds++
	build := f.builds
	f.mu.Unlock()
	if build == 2 && f.secondStarted != nil {
		close(f.secondStarted)
		<-f.releaseSecond
	}
	subscription := events.EventType("test.old")
	if len(cfg.Tools) > 0 {
		subscription = events.EventType("test.new")
	}
	return &projectionTestAgent{id: cfg.ID, build: build, subs: []events.EventType{subscription}, handled: f.handled}, nil
}

func TestExecutionProjectionReconfigureSerializesRestartSelection(t *testing.T) {
	bus := newProjectionTestBus()
	handled := make(chan int, 1)
	releaseBuild := make(chan struct{})
	factory := &projectionTestFactory{secondStarted: make(chan struct{}), releaseSecond: releaseBuild, handled: handled}
	am := newProjectionTestManager(t, bus, factory.Build)
	const agentID = "projection-restart"
	if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))

	reconfigureDone := make(chan error, 1)
	go func() {
		reconfigureDone <- am.ReconfigureAgent(agentID, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-new"}, Subscriptions: []string{"test.new"}})
	}()
	<-factory.secondStarted
	restartDone := make(chan error, 1)
	go func() {
		_, err := am.Restart(testAuthorActivityContext(context.Background()), runtimeagentcontrol.RestartRequest{AgentID: agentID})
		restartDone <- err
	}()
	close(releaseBuild)
	if err := <-reconfigureDone; err != nil {
		t.Fatalf("ReconfigureAgent: %v", err)
	}
	if err := <-restartDone; err != nil {
		t.Fatalf("Restart: %v", err)
	}

	history := bus.routeHistory(agentID)
	if len(history) != 3 {
		t.Fatalf("route generations = %d, want start + reconfigure + restart", len(history))
	}
	if history[0].channel == history[1].channel || history[1].channel == history[2].channel {
		t.Fatal("generation replacement reused an agent channel")
	}
	current, ok := bus.current(agentID)
	if !ok || len(current.subscriptions) != 1 || current.subscriptions[0] != events.EventType("test.new") {
		t.Fatalf("current route = %#v, want exact test.new", current)
	}
}

func TestExecutionProjectionReconfigureSerializesBothRunModes(t *testing.T) {
	for _, tc := range []struct {
		name              string
		run               func(*AgentManager, context.Context)
		wantSubscriptions int
	}{
		{name: "standard", run: func(am *AgentManager, ctx context.Context) { am.Run(ctx) }, wantSubscriptions: 1},
		{name: "authoritative_delivery_only", run: func(am *AgentManager, ctx context.Context) { am.RunAuthoritativeDeliveryOnly(ctx) }, wantSubscriptions: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := newProjectionTestBus()
			handled := make(chan int, 1)
			releaseBuild := make(chan struct{})
			factory := &projectionTestFactory{secondStarted: make(chan struct{}), releaseSecond: releaseBuild, handled: handled}
			am := newProjectionTestManager(t, bus, factory.Build)
			const agentID = "projection-run"
			if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}); err != nil {
				t.Fatalf("SpawnAgent: %v", err)
			}
			reconfigureDone := make(chan error, 1)
			go func() {
				reconfigureDone <- am.ReconfigureAgent(agentID, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-new"}, Subscriptions: []string{"test.new"}})
			}()
			<-factory.secondStarted
			runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
			defer cancelRun()
			runDone := make(chan struct{})
			go func() { tc.run(am, managedExecutionTestContext(t, runCtx)); close(runDone) }()
			close(releaseBuild)
			if err := <-reconfigureDone; err != nil {
				t.Fatalf("ReconfigureAgent: %v", err)
			}
			<-runDone
			current, ok := bus.current(agentID)
			if !ok {
				t.Fatal("run did not install a route")
			}
			if len(current.subscriptions) != tc.wantSubscriptions {
				t.Fatalf("subscriptions = %#v, want count %d", current.subscriptions, tc.wantSubscriptions)
			}
		})
	}
}

func TestExecutionPreparationFailuresCompensateBeforeLaunchInBothRunModes(t *testing.T) {
	for _, mode := range []struct {
		name string
		run  func(*AgentManager, context.Context) error
	}{
		{name: "standard", run: (*AgentManager).Run},
		{name: "authoritative_delivery_only", run: (*AgentManager).RunAuthoritativeDeliveryOnly},
	} {
		for _, failure := range []string{"prepare", "publish", "owner_fence_during_publish"} {
			t.Run(mode.name+"/"+failure, func(t *testing.T) {
				bus := newProjectionTestBus()
				factory := &projectionTestFactory{handled: make(chan int, 1)}
				am := newProjectionTestManager(t, bus, factory.Build)
				const agentID = "projection-start-failure"
				if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}); err != nil {
					t.Fatalf("SpawnAgent: %v", err)
				}
				switch failure {
				case "prepare":
					bus.prepareErr = true
				case "publish":
					bus.publishErr = context.Canceled
				case "owner_fence_during_publish":
					bus.beforePublish = func() { am.lifecycle.requestShutdownTransition() }
				}

				err := mode.run(am, managedExecutionTestContext(t, testAuthorActivityContext(context.Background())))
				if err == nil {
					t.Fatalf("%s start succeeded despite %s failure", mode.name, failure)
				}
				if _, live := bus.current(agentID); live {
					t.Fatal("failed start left a reachable agent route")
				}
				am.lifecycle.mu.Lock()
				cell := am.lifecycle.cells[agentID]
				phase := cell.phase
				loopDone := cell.execution.loopDone
				routeToken := cell.execution.routeToken
				am.lifecycle.mu.Unlock()
				if phase != AgentLifecycleRegistered || loopDone != nil || routeToken.Valid() {
					t.Fatalf("failed start left phase=%q loop_done=%v route=%+v", phase, loopDone != nil, routeToken)
				}
				if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
					t.Fatalf("shutdown after compensated start failure: %v", err)
				}
			})
		}
	}
}

func TestSelectedForkEphemeralRegistrationInstallsCarrierOnlyRoute(t *testing.T) {
	bus := newProjectionTestBus()
	am := newProjectionTestManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		return &projectionTestAgent{id: cfg.ID, subs: []events.EventType{"foreign/task.ready"}, handled: make(chan int, 1)}, nil
	})
	const agentID = "selected-fork-agent"
	if err := am.RegisterEphemeralAgentForExecution(context.Background(), PersistedAgent{Config: models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		FlowPath:      "review/inst-1",
		Subscriptions: []string{"task.ready", "task.*"},
	}}); err != nil {
		t.Fatalf("RegisterEphemeralAgentForExecution: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	if err := am.RunAuthoritativeDeliveryOnly(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("RunAuthoritativeDeliveryOnly: %v", err)
	}
	route, ok := bus.current(agentID)
	if !ok {
		t.Fatal("selected-fork execution did not install its typed carrier")
	}
	if len(route.subscriptions) != 0 {
		t.Fatalf("selected-fork carrier subscriptions = %#v, want none", route.subscriptions)
	}
	execution, ok := am.lifecycle.executionSnapshot(agentID)
	if !ok {
		t.Fatal("selected-fork execution projection missing")
	}
	want := []events.EventType{"review/inst-1/task.*", "review/inst-1/task.ready"}
	if !reflect.DeepEqual(execution.Subscriptions, want) {
		t.Fatalf("selected-fork admitted subscriptions = %#v, want %#v", execution.Subscriptions, want)
	}
}

func TestEphemeralCloneConsumesAdmittedBaseSubscriptions(t *testing.T) {
	bus := newProjectionTestBus()
	am := newProjectionTestManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		return &projectionTestAgent{id: cfg.ID, handled: make(chan int, 1)}, nil
	})
	if err := am.SpawnAgent(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "base-agent",
		FlowPath:      "review/inst-1",
		Subscriptions: []string{"task.ready", "task.*"},
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := am.SpawnEphemeralClone("base-agent", "clone-agent"); err != nil {
		t.Fatalf("SpawnEphemeralClone: %v", err)
	}

	base, baseOK := am.lifecycle.executionSnapshot("base-agent")
	clone, cloneOK := am.lifecycle.executionSnapshot("clone-agent")
	if !baseOK || !cloneOK {
		t.Fatalf("execution snapshots: base=%v clone=%v", baseOK, cloneOK)
	}
	if !reflect.DeepEqual(clone.Subscriptions, base.Subscriptions) || clone.Admission.FlowPath() != base.Admission.FlowPath() {
		t.Fatalf("clone admission = %#v/%q, want base %#v/%q", clone.Subscriptions, clone.Admission.FlowPath(), base.Subscriptions, base.Admission.FlowPath())
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	route, ok := bus.current("clone-agent")
	if !ok {
		t.Fatal("clone route missing")
	}
	if !reflect.DeepEqual(route.subscriptions, base.Subscriptions) {
		t.Fatalf("clone route subscriptions = %#v, want admitted base %#v", route.subscriptions, base.Subscriptions)
	}
}

func TestExecutionProjectionDirectiveLeaseFencesReplacement(t *testing.T) {
	bus := newProjectionTestBus()
	bus.store = &directiveEventStore{}
	boardStarted := make(chan runtimeeffects.LifecycleToken, 1)
	boardRelease := make(chan struct{})
	build := 0
	factory := func(cfg models.AgentConfig) (Agent, error) {
		build++
		return &projectionDirectiveAgent{
			projectionTestAgent: projectionTestAgent{id: cfg.ID, build: build, subs: []events.EventType{"test.directive"}, handled: make(chan int, 1)},
			boardStarted:        boardStarted, boardRelease: boardRelease,
		}, nil
	}
	targetStore := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: "00000000-0000-0000-0000-000000009901", Mode: runtimeagentcontrol.RunResolutionSpecified}}
	am := newProjectionTestManager(t, bus, factory, targetStore)
	const agentID = "projection-directive"
	if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	predecessor, ok := am.lifecycle.token(agentID)
	if !ok {
		t.Fatal("predecessor generation is not running")
	}
	directiveDone := make(chan error, 1)
	go func() {
		_, err := am.SendDirective(testAuthorActivityContext(context.Background()), runtimeagentcontrol.SendDirectiveRequest{
			AgentID: agentID, Directive: "hold generation", ActorTokenID: "operator-token",
			IdempotencyKey: "projection-directive", RequestHash: "projection-directive-hash",
		})
		directiveDone <- err
	}()
	if got := <-boardStarted; got != predecessor {
		t.Fatalf("directive token = %+v, want exact predecessor %+v", got, predecessor)
	}
	reconfigureDone := make(chan error, 1)
	go func() {
		reconfigureDone <- am.ReconfigureAgent(agentID, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-new"}, Subscriptions: []string{"test.new"}})
	}()
	select {
	case <-runCtx.Done():
		t.Fatal("runtime canceled unexpectedly")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case err := <-reconfigureDone:
		t.Fatalf("replacement completed before predecessor direct lease released: %v", err)
	default:
	}
	close(boardRelease)
	if err := <-directiveDone; err != nil {
		t.Fatalf("SendDirective: %v", err)
	}
	if err := <-reconfigureDone; err != nil {
		t.Fatalf("ReconfigureAgent: %v", err)
	}
	if successor, ok := am.lifecycle.token(agentID); !ok || successor == predecessor {
		t.Fatalf("successor token = %+v ok=%v, want a new generation", successor, ok)
	}
}

func TestExecutionProjectionRunCancellationRemovesExactRoute(t *testing.T) {
	bus := newProjectionTestBus()
	factory := &projectionTestFactory{handled: make(chan int, 1)}
	am := newProjectionTestManager(t, bus, factory.Build)
	const agentID = "projection-shutdown"
	if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	am.Run(managedExecutionTestContext(t, runCtx))
	route, ok := bus.current(agentID)
	if !ok {
		t.Fatal("run did not install route")
	}
	cancelRun()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, live := bus.current(agentID); !live {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, live := bus.current(agentID); live {
		t.Fatal("run cancellation left predecessor route live")
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	found := false
	for _, removed := range bus.removed {
		if removed == route.token {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("removed tokens = %+v, want exact route token %+v", bus.removed, route.token)
	}
}

func TestExecutionProjectionTeardownRemovesExactRoute(t *testing.T) {
	bus := newProjectionTestBus()
	factory := &projectionTestFactory{handled: make(chan int, 1)}
	am := newProjectionTestManager(t, bus, factory.Build)
	const agentID = "projection-teardown"
	if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	route, ok := bus.current(agentID)
	if !ok {
		t.Fatal("run did not install route")
	}
	if err := am.TeardownAgent(agentID); err != nil {
		t.Fatalf("TeardownAgent: %v", err)
	}
	if _, live := bus.current(agentID); live {
		t.Fatal("teardown left predecessor route live")
	}
	if _, exists := am.lifecycle.executionSnapshot(agentID); exists {
		t.Fatal("teardown left executable projection live")
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	found := false
	for _, removed := range bus.removed {
		if removed == route.token {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("removed tokens = %+v, want exact teardown token %+v", bus.removed, route.token)
	}
}

func TestExecutionProjectionNaturalLoopExitRemovesExactRoute(t *testing.T) {
	bus := newProjectionTestBus()
	factory := &projectionTestFactory{handled: make(chan int, 1)}
	am := newProjectionTestManager(t, bus, factory.Build)
	const agentID = "projection-self-release"
	if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	route, ok := bus.current(agentID)
	if !ok {
		t.Fatal("run did not install route")
	}
	close(route.channel)
	deadline := time.Now().Add(time.Second)
	for {
		if _, live := bus.current(agentID); !live {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("natural loop exit did not remove its exact route")
		}
		time.Sleep(time.Millisecond)
	}
	am.lifecycle.mu.Lock()
	cell := am.lifecycle.cells[agentID]
	phase := cell.phase
	loopDone := cell.execution.loopDone
	routeToken := cell.execution.routeToken
	am.lifecycle.mu.Unlock()
	if phase != AgentLifecycleRegistered || loopDone != nil || routeToken.Valid() {
		t.Fatalf("natural loop exit left phase=%q loop_done=%v route_token=%+v, want registered without loop/route", phase, loopDone != nil, routeToken)
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	found := false
	for _, removed := range bus.removed {
		if removed == route.token {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("removed tokens = %+v, want natural self-release token %+v", bus.removed, route.token)
	}
}

func TestExecutionProjectionBacklogLeaseFencesReplacement(t *testing.T) {
	const agentID = "projection-backlog"
	store := newStartupReplayTestStore(t, recoveryTestStore{}, map[string][]events.Event{
		agentID: {projectionRuntimeEvent(eventtest.UUID("backlog-event"), "test.backlog")},
	})
	bus := &recoveryTestBus{}
	eventStarted := make(chan runtimeeffects.LifecycleToken, 1)
	eventRelease := make(chan struct{})
	build := 0
	factory := func(cfg models.AgentConfig) (Agent, error) {
		build++
		return &projectionBacklogAgent{
			projectionTestAgent: projectionTestAgent{id: cfg.ID, build: build, subs: []events.EventType{"test.backlog"}, handled: make(chan int, 1)},
			eventStarted:        eventStarted, eventRelease: eventRelease,
		}, nil
	}
	am := newProjectionTestManager(t, bus, factory, store)
	if err := am.spawnAgentInternal(testAuthorActivityContext(context.Background()), PersistedAgent{Config: models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.backlog"}}}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}
	predecessor, ok := am.lifecycle.executionSnapshot(agentID)
	if !ok {
		t.Fatal("predecessor execution is absent")
	}
	replayDone := make(chan error, 1)
	go func() { replayDone <- am.ReplayAgentBacklog(testAuthorActivityContext(context.Background()), agentID) }()
	if got := <-eventStarted; got != predecessor.Token {
		t.Fatalf("backlog token = %+v, want exact predecessor %+v", got, predecessor.Token)
	}
	reconfigureDone := make(chan error, 1)
	go func() {
		reconfigureDone <- am.ReconfigureAgent(agentID, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-new"}, Subscriptions: []string{"test.backlog"}})
	}()
	select {
	case err := <-reconfigureDone:
		t.Fatalf("replacement completed before predecessor backlog lease released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(eventRelease)
	if err := <-replayDone; err != nil {
		t.Fatalf("ReplayAgentBacklog: %v", err)
	}
	if err := <-reconfigureDone; err != nil {
		t.Fatalf("ReconfigureAgent: %v", err)
	}
	successor, ok := am.lifecycle.executionSnapshot(agentID)
	if !ok || successor.Token == predecessor.Token {
		t.Fatalf("successor = %+v ok=%v, want a new execution token", successor.Token, ok)
	}
}

func TestExecutionProjectionRecoveryStartsPersistedRunningCell(t *testing.T) {
	bus := newProjectionTestBus()
	handled := make(chan int, 1)
	factory := &projectionTestFactory{handled: handled}
	am := newProjectionTestManager(t, bus, factory.Build)
	const agentID = "projection-recovery"
	rec := PersistedAgent{
		Config: models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}, LifecycleEpoch: runtimebus.CurrentRuntimeEpoch(),
		LifecycleGeneration: 4, LifecyclePhase: AgentLifecycleRunning, LifecycleRunMode: AgentRunModeStandard,
	}
	if err := am.spawnAgentInternal(testAuthorActivityContext(context.Background()), rec, false); err != nil {
		t.Fatalf("hydrate persisted running agent: %v", err)
	}
	if _, live := bus.current(agentID); live {
		t.Fatal("hydration installed a route before runtime start")
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	route, live := bus.current(agentID)
	if !live {
		t.Fatal("Run treated persisted phase as a live process and skipped activation")
	}
	if route.token.Generation != 5 {
		t.Fatalf("recovered route generation = %d, want 5", route.token.Generation)
	}
	bus.send(agentID, projectionRuntimeEvent("recovery-result", "test.old"))
	select {
	case build := <-handled:
		if build != 1 {
			t.Fatalf("recovered event handled by build %d, want hydrated build 1", build)
		}
	case <-time.After(time.Second):
		bus.mu.Lock()
		logs := append([]runtimepipeline.RuntimeLogEntry(nil), bus.runtimeLogs...)
		bus.mu.Unlock()
		var failure any
		if len(logs) > 0 && logs[len(logs)-1].Failure != nil {
			failure = *logs[len(logs)-1].Failure
		}
		t.Fatalf("recovered execution did not handle event; logs=%+v failure=%+v", logs, failure)
	}
}

func TestExecutionProjectionSpawnDuringRunActivatesRegisteredProjection(t *testing.T) {
	bus := newProjectionTestBus()
	handled := make(chan int, 1)
	factory := &projectionTestFactory{handled: handled}
	am := newProjectionTestManager(t, bus, factory.Build)
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	const agentID = "projection-flow-activation"
	if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: agentID, Subscriptions: []string{"test.old"}}); err != nil {
		t.Fatalf("SpawnAgent while running: %v", err)
	}
	route, live := bus.current(agentID)
	if !live {
		t.Fatal("spawn during run did not install a generation route")
	}
	execution, ok := am.lifecycle.executionSnapshot(agentID)
	if !ok || execution.Token != route.token {
		t.Fatalf("execution token = %+v ok=%v route token=%+v", execution.Token, ok, route.token)
	}
	bus.send(agentID, projectionRuntimeEvent("activation-result", "test.old"))
	select {
	case build := <-handled:
		if build != 1 {
			t.Fatalf("activation event handled by build %d, want build 1", build)
		}
	case <-time.After(time.Second):
		bus.mu.Lock()
		logs := append([]runtimepipeline.RuntimeLogEntry(nil), bus.runtimeLogs...)
		bus.mu.Unlock()
		var failure any
		if len(logs) > 0 && logs[len(logs)-1].Failure != nil {
			failure = *logs[len(logs)-1].Failure
		}
		t.Fatalf("activated projection did not handle event; logs=%+v failure=%+v", logs, failure)
	}
}

func projectionRuntimeEvent(id string, eventType events.EventType) events.Event {
	return eventtest.RuntimeControl(eventtest.UUID(id), eventType, "test", "", []byte(`{}`), 0, eventtest.UUID("projection-run"), "", events.EventEnvelope{}, time.Now())
}
