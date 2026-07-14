package manager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type projectionTestRoute struct {
	token         runtimeeffects.LifecycleToken
	channel       chan events.Event
	subscriptions []events.EventType
}

type projectionTestBus struct {
	mu      sync.Mutex
	routes  map[string]projectionTestRoute
	history map[string][]projectionTestRoute
	removed []runtimeeffects.LifecycleToken
	store   runtimebus.EventStore
}

func newProjectionTestBus() *projectionTestBus {
	return &projectionTestBus{routes: map[string]projectionTestRoute{}, history: map[string][]projectionTestRoute{}}
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
func (*projectionTestBus) ResetInMemoryState() error      { return nil }
func (*projectionTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}
func (b *projectionTestBus) ReplaceAgentRoute(token runtimeeffects.LifecycleToken, subscriptions ...events.EventType) <-chan events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	route := projectionTestRoute{
		token: token, channel: make(chan events.Event, 128),
		subscriptions: append([]events.EventType(nil), subscriptions...),
	}
	b.routes[token.AgentID] = route
	b.history[token.AgentID] = append(b.history[token.AgentID], route)
	return route.channel
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
		route.channel <- event
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
	am := NewAgentManager(bus, factory.Build)
	const agentID = "projection-restart"
	if err := am.SpawnAgent(models.AgentConfig{ID: agentID}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(runCtx)

	reconfigureDone := make(chan error, 1)
	go func() {
		reconfigureDone <- am.ReconfigureAgent(agentID, models.AgentConfig{Tools: []string{"tool-new"}})
	}()
	<-factory.secondStarted
	restartDone := make(chan error, 1)
	go func() {
		_, err := am.Restart(context.Background(), runtimeagentcontrol.RestartRequest{AgentID: agentID})
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
	bus.send(agentID, projectionRuntimeEvent("restart-result", "test.new"))
	select {
	case build := <-handled:
		if build != 2 {
			t.Fatalf("event handled by build %d, want committed build 2", build)
		}
	case <-time.After(time.Second):
		t.Fatal("current generation did not handle event")
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
			am := NewAgentManager(bus, factory.Build)
			const agentID = "projection-run"
			if err := am.SpawnAgent(models.AgentConfig{ID: agentID}); err != nil {
				t.Fatalf("SpawnAgent: %v", err)
			}
			reconfigureDone := make(chan error, 1)
			go func() {
				reconfigureDone <- am.ReconfigureAgent(agentID, models.AgentConfig{Tools: []string{"tool-new"}})
			}()
			<-factory.secondStarted
			runCtx, cancelRun := context.WithCancel(context.Background())
			defer cancelRun()
			runDone := make(chan struct{})
			go func() { tc.run(am, runCtx); close(runDone) }()
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
			bus.send(agentID, projectionRuntimeEvent("run-result", "test.new"))
			select {
			case build := <-handled:
				if build != 2 {
					t.Fatalf("event handled by build %d, want committed build 2", build)
				}
			case <-time.After(time.Second):
				t.Fatal("current generation did not handle event")
			}
		})
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
	am := NewAgentManager(bus, factory, targetStore)
	const agentID = "projection-directive"
	if err := am.SpawnAgent(models.AgentConfig{ID: agentID}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(runCtx)
	predecessor, ok := am.lifecycle.token(agentID)
	if !ok {
		t.Fatal("predecessor generation is not running")
	}
	directiveDone := make(chan error, 1)
	go func() {
		_, err := am.SendDirective(context.Background(), runtimeagentcontrol.SendDirectiveRequest{
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
		reconfigureDone <- am.ReconfigureAgent(agentID, models.AgentConfig{Tools: []string{"tool-new"}})
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
	am := NewAgentManager(bus, factory.Build)
	const agentID = "projection-shutdown"
	if err := am.SpawnAgent(models.AgentConfig{ID: agentID}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	am.Run(runCtx)
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
	am := NewAgentManager(bus, factory.Build)
	const agentID = "projection-teardown"
	if err := am.SpawnAgent(models.AgentConfig{ID: agentID}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(runCtx)
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
	am := NewAgentManager(bus, factory.Build)
	const agentID = "projection-self-release"
	if err := am.SpawnAgent(models.AgentConfig{ID: agentID}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(runCtx)
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
	store := &startupReplayTestStore{pending: map[string][]events.Event{
		agentID: {projectionRuntimeEvent("backlog-event", "test.backlog")},
	}}
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
	am := NewAgentManager(bus, factory, store)
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{Config: models.AgentConfig{ID: agentID}}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}
	predecessor, ok := am.lifecycle.executionSnapshot(agentID)
	if !ok {
		t.Fatal("predecessor execution is absent")
	}
	replayDone := make(chan error, 1)
	go func() { replayDone <- am.ReplayAgentBacklog(context.Background(), agentID) }()
	if got := <-eventStarted; got != predecessor.Token {
		t.Fatalf("backlog token = %+v, want exact predecessor %+v", got, predecessor.Token)
	}
	reconfigureDone := make(chan error, 1)
	go func() {
		reconfigureDone <- am.ReconfigureAgent(agentID, models.AgentConfig{Tools: []string{"tool-new"}})
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
	am := NewAgentManager(bus, factory.Build)
	const agentID = "projection-recovery"
	rec := PersistedAgent{
		Config: models.AgentConfig{ID: agentID}, LifecycleEpoch: runtimebus.CurrentRuntimeEpoch(),
		LifecycleGeneration: 4, LifecyclePhase: AgentLifecycleRunning, LifecycleRunMode: AgentRunModeStandard,
	}
	if err := am.spawnAgentInternal(context.Background(), rec, false); err != nil {
		t.Fatalf("hydrate persisted running agent: %v", err)
	}
	if _, live := bus.current(agentID); live {
		t.Fatal("hydration installed a route before runtime start")
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(runCtx)
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
		t.Fatal("recovered execution did not handle event")
	}
}

func TestExecutionProjectionSpawnDuringRunActivatesRegisteredProjection(t *testing.T) {
	bus := newProjectionTestBus()
	handled := make(chan int, 1)
	factory := &projectionTestFactory{handled: handled}
	am := NewAgentManager(bus, factory.Build)
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(runCtx)
	const agentID = "projection-flow-activation"
	if err := am.SpawnAgent(models.AgentConfig{ID: agentID}); err != nil {
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
		t.Fatal("activated projection did not handle event")
	}
}

func projectionRuntimeEvent(id string, eventType events.EventType) events.Event {
	return eventtest.RuntimeControl(id, eventType, "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
}
