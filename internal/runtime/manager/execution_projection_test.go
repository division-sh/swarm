package manager

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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
	authority     runtimedelivery.ExecutionAuthority
	continuations runtimebus.DeliveryContinuationOwner
	prepareErr    bool
	publishErr    error
	runtimeLogs   []runtimepipeline.RuntimeLogEntry
	beforePublish func()
	deliveryCtx   context.Context
}

func TestAgentManagerWithOptionsRejectsUnconfiguredReceiverExecution(t *testing.T) {
	defer func() {
		got := recover()
		if got == nil || !strings.Contains(fmt.Sprint(got), "not configured") {
			t.Fatalf("unconfigured AgentManager receiver execution panic = %v", got)
		}
	}()
	_ = NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{})
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

func (*projectionTestBus) AdmitBundleSourceFact(ctx context.Context) (context.Context, error) {
	return admitManagerTestBusContext(ctx)
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
func (b *projectionTestBus) SetDeliveryAuthority(authority runtimedelivery.ExecutionAuthority) error {
	if err := authority.Validate(); err != nil {
		return err
	}
	b.authority = authority
	return nil
}
func (b *projectionTestBus) DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error) {
	if err := b.authority.Validate(); err != nil {
		return runtimedelivery.ExecutionAuthority{}, err
	}
	return b.authority, nil
}
func (b *projectionTestBus) SetDeliveryContinuationOwner(owner runtimebus.DeliveryContinuationOwner) error {
	if owner == nil {
		return context.Canceled
	}
	b.continuations = owner
	return nil
}
func (b *projectionTestBus) AcquireDeliveryContinuation(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	return b.continuations.Acquire(deliveryID)
}
func (b *projectionTestBus) RetainDeliveryContinuation(snapshot runtimedelivery.Snapshot) error {
	return b.continuations.Retain(snapshot)
}
func (b *projectionTestBus) ReleaseDeliveryContinuation(deliveryID string) error {
	return b.continuations.Release(deliveryID)
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
func (b *projectionTestBus) FenceAgentRoute(runtimeeffects.LifecycleToken) {}
func (b *projectionTestBus) RemoveAgentRoute(token runtimeeffects.LifecycleToken) {
	b.mu.Lock()
	b.removed = append(b.removed, token)
	var retired projectionTestRoute
	if route, ok := b.routes[token.AgentID]; ok && route.token == token {
		retired = route
		delete(b.routes, token.AgentID)
	}
	b.mu.Unlock()
	for retired.channel != nil {
		select {
		case delivery, ok := <-retired.channel:
			if !ok {
				return
			}
			if delivery != nil {
				_ = delivery.Complete()
			}
		default:
			return
		}
	}
}
func (b *projectionTestBus) send(agentID string, event events.Event) error {
	b.mu.Lock()
	localRoute, ok := b.routes[agentID]
	b.mu.Unlock()
	if ok {
		deliveryRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: localRoute.token.Identity}
		deliveryCtx := b.deliveryCtx
		if deliveryCtx == nil {
			deliveryCtx = context.Background()
		}
		delivery, err := b.owner.NewRoutedEventDelivery(testAuthorActivityContext(deliveryCtx), event, deliveryRoute)
		if err != nil {
			return err
		}
		deliveryID, err := runtimedelivery.DeliveryID(event.ID(), deliveryRoute)
		if err != nil {
			return err
		}
		continuation, err := b.continuations.Acquire(deliveryID)
		if err != nil {
			return err
		}
		if err := delivery.AttachContinuation(continuation); err != nil {
			return err
		}
		localRoute.channel <- delivery
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

type projectionSelfRetiringAgent struct {
	projectionTestAgent
	identity runtimeagentidentity.Identity
	started  chan<- struct{}
	release  <-chan struct{}
	retire   func(context.Context, runtimeagentidentity.Identity) error
	handled  atomic.Int32
}

func (a *projectionBacklogAgent) OnEvent(ctx context.Context, _ events.Event) ([]events.Event, error) {
	token, _ := runtimeeffects.LifecycleTokenFromContext(ctx)
	a.eventStarted <- token
	<-a.eventRelease
	return nil, nil
}

func (a *projectionSelfRetiringAgent) OnEvent(ctx context.Context, _ events.Event) ([]events.Event, error) {
	if a.handled.Add(1) != 1 {
		return nil, nil
	}
	a.started <- struct{}{}
	<-a.release
	return nil, a.retire(ctx, a.identity)
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
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
		Subscriptions: []string{"test.old"},
	})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))

	reconfigureDone := make(chan error, 1)
	go func() {
		err := reconfigureAgentThroughLifecycleForTest(t, am, agentID, "", models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-new"}, Subscriptions: []string{"test.new"}})
		reconfigureDone <- err
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
			if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
				ExecutionMode: "live",
				ID:            agentID,
				Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
				Subscriptions: []string{"test.old"},
			})); err != nil {
				t.Fatalf("SpawnAgent: %v", err)
			}
			reconfigureDone := make(chan error, 1)
			go func() {
				err := reconfigureAgentThroughLifecycleForTest(t, am, agentID, "", models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-new"}, Subscriptions: []string{"test.new"}})
				reconfigureDone <- err
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
				if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
					ExecutionMode: "live",
					ID:            agentID,
					Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
					Subscriptions: []string{"test.old"},
				})); err != nil {
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
				cell, _ := testLifecycleCell(t, am.lifecycle, agentID, "")
				am.lifecycle.mu.Lock()
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
	if err := registerManagerTestEphemeralAgent(context.Background(), am, PersistedAgent{Config: managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.Runtime(t, agentID, "selected-fork-test", "review", "inst-1", "review/inst-1"),
		FlowPath:      "review/inst-1",
		Subscriptions: []string{"task.ready", "task.*"},
	})}); err != nil {
		t.Fatalf("MaterializeAdmittedAgentForExecution: %v", err)
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
	execution, ok := testExecutionSnapshot(t, am, agentID, "review/inst-1")
	if !ok {
		t.Fatal("selected-fork execution projection missing")
	}
	want := []events.EventType{"review/inst-1/task.*", "review/inst-1/task.ready"}
	if !reflect.DeepEqual(execution.Subscriptions, want) {
		t.Fatalf("selected-fork admitted subscriptions = %#v, want %#v", execution.Subscriptions, want)
	}
}

func TestExecutionProjectionDirectiveLeaseFencesReplacement(t *testing.T) {
	bus := newProjectionTestBus()
	directiveStore := &directiveEventStore{}
	bus.store = directiveStore
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
	owner := newTestManagerWorkOwner(t)
	bus.owner = owner
	am := newTestAgentManagerWithOptions(t, bus, factory, AgentManagerOptions{
		WorkOwner: owner,
		PersistenceRoles: PersistenceRoles{
			DirectiveOperations: directiveStore,
			DirectiveTargets:    targetStore,
		},
	}, targetStore)
	const agentID = "projection-directive"
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
		Subscriptions: []string{"test.old"},
	})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	identity := testAgentIdentity(t, am, agentID, "")
	predecessor, ok := am.lifecycle.tokenIdentity(identity)
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
		err := reconfigureAgentThroughLifecycleForTest(t, am, agentID, "", models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-new"}, Subscriptions: []string{"test.new"}})
		reconfigureDone <- err
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
	if successor, ok := am.lifecycle.tokenIdentity(identity); !ok || successor == predecessor {
		t.Fatalf("successor token = %+v ok=%v, want a new generation", successor, ok)
	}
}

func TestExecutionProjectionRunCancellationRemovesExactRoute(t *testing.T) {
	bus := newProjectionTestBus()
	factory := &projectionTestFactory{handled: make(chan int, 1)}
	am := newProjectionTestManager(t, bus, factory.Build)
	const agentID = "projection-shutdown"
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
		Subscriptions: []string{"test.old"},
	})); err != nil {
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
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
		Subscriptions: []string{"test.old"},
	})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	route, ok := bus.current(agentID)
	if !ok {
		t.Fatal("run did not install route")
	}
	if err := teardownAgentThroughLifecycleForTest(t, am, agentID, ""); err != nil {
		t.Fatalf("TeardownAgent: %v", err)
	}
	if _, live := bus.current(agentID); live {
		t.Fatal("teardown left predecessor route live")
	}
	if _, exists := testExecutionSnapshot(t, am, agentID, ""); exists {
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
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
		Subscriptions: []string{"test.old"},
	})); err != nil {
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
	cell, _ := testLifecycleCell(t, am.lifecycle, agentID, "")
	am.lifecycle.mu.Lock()
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

func TestExecutionProjectionRecoveryStartsPersistedRunningCell(t *testing.T) {
	bus := newProjectionTestBus()
	handled := make(chan int, 1)
	factory := &projectionTestFactory{handled: handled}
	am := newProjectionTestManager(t, bus, factory.Build)
	const agentID = "projection-recovery"
	rec := PersistedAgent{
		Config: managerTestAgentConfig(models.AgentConfig{
			ExecutionMode: "live",
			ID:            agentID,
			Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
			Subscriptions: []string{"test.old"},
		}),
		LifecycleEpoch:      runtimebus.CurrentRuntimeEpoch(),
		LifecycleGeneration: 4, LifecyclePhase: AgentLifecycleRunning, LifecycleRunMode: AgentRunModeStandard,
	}
	rec.Topology = managerTestTopologyAdmission(t)
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
		t.Fatalf("recovered route generation = %d, want one running-to-running successor generation", route.token.Generation)
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
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "execution-projection-test"),
		Subscriptions: []string{"test.old"},
	})); err != nil {
		t.Fatalf("SpawnAgent while running: %v", err)
	}
	route, live := bus.current(agentID)
	if !live {
		t.Fatal("spawn during run did not install a generation route")
	}
	execution, ok := testExecutionSnapshot(t, am, agentID, "")
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

type projectionCarrierResolution struct {
	intent     worklifetime.DeliveryContinuationIntent
	resolution worklifetime.DeliveryContinuationResolution
}

type projectionResolutionOwner struct {
	resolution worklifetime.DeliveryContinuationResolution
	resolved   chan projectionCarrierResolution
	released   chan struct{}
	releases   atomic.Int32
	resolves   atomic.Int32
}

func (*projectionResolutionOwner) AcceptCommitted([]runtimedelivery.DurableHandoffProof) error {
	return nil
}
func (o *projectionResolutionOwner) Acquire(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	return &projectionResolutionContinuation{owner: o, deliveryID: deliveryID}, nil
}
func (*projectionResolutionOwner) Retain(runtimedelivery.Snapshot) error { return nil }
func (o *projectionResolutionOwner) Release(string) error {
	o.releases.Add(1)
	select {
	case o.released <- struct{}{}:
	default:
	}
	return nil
}
func (*projectionResolutionOwner) OwnsPersistedRecovery() bool { return false }
func (*projectionResolutionOwner) Signal()                     {}

type projectionResolutionContinuation struct {
	owner      *projectionResolutionOwner
	deliveryID string
	once       sync.Once
}

func (c *projectionResolutionContinuation) DeliveryID() string { return c.deliveryID }
func (c *projectionResolutionContinuation) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	c.owner.resolves.Add(1)
	resolution := c.owner.resolution
	if resolution == 0 {
		if intent == worklifetime.DeliveryContinuationReturn {
			resolution = worklifetime.DeliveryContinuationReturned
		} else {
			resolution = worklifetime.DeliveryContinuationConsumed
		}
	}
	c.once.Do(func() {
		c.owner.resolved <- projectionCarrierResolution{intent: intent, resolution: resolution}
	})
	return resolution, nil
}

type projectionScriptedClaimStore struct {
	runtimedelivery.Store
	result   runtimedelivery.ClaimResult
	err      error
	delegate bool
	claims   *atomic.Int32
}

func (s projectionScriptedClaimStore) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, evt events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	if s.claims != nil {
		s.claims.Add(1)
	}
	if s.delegate {
		return s.Store.ClaimDelivery(ctx, authority, evt, route)
	}
	return s.result, s.err
}

func projectionDeliveryAuthorities(t *testing.T) map[string]runtimedelivery.ExecutionAuthority {
	t.Helper()
	source, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("e", 64))
	if err != nil {
		t.Fatalf("construct projection delivery source: %v", err)
	}
	normal, err := runtimedelivery.NewNormalExecutionAuthority(source, "manager-delivery-test", 1)
	if err != nil {
		t.Fatalf("construct normal projection authority: %v", err)
	}
	selected, err := runtimedelivery.NewSelectedExecutionAuthority(source, eventtest.UUID("projection-selected-execution"), eventtest.UUID("projection-selected-run"), 1)
	if err != nil {
		t.Fatalf("construct selected projection authority: %v", err)
	}
	return map[string]runtimedelivery.ExecutionAuthority{"normal": normal, "selected": selected}
}

func TestRunningManagerDeliveryCarrierDispositionMatrix(t *testing.T) {
	type branch struct {
		name         string
		result       func(string) runtimedelivery.ClaimResult
		claimErr     error
		delegate     bool
		shutdown     bool
		missingWork  bool
		fenceWork    bool
		cancelLane   bool
		missingAuth  bool
		terminalRace bool
		wantIntent   worklifetime.DeliveryContinuationIntent
		wantRelease  int32
		wantHandled  bool
	}
	branches := []branch{
		{name: "shutdown_admission", shutdown: true, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "begin_work_failure", fenceWork: true, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "missing_occurrence", missingWork: true, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "lane_failure", cancelLane: true, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "missing_authority", missingAuth: true, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "claim_error", claimErr: errors.New("injected claim error"), wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "deferred", result: func(string) runtimedelivery.ClaimResult {
			return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimDeferred}
		}, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "busy", result: func(string) runtimedelivery.ClaimResult {
			return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimBusy}
		}, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "terminal", result: func(id string) runtimedelivery.ClaimResult {
			return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimTerminal, Snapshot: runtimedelivery.Snapshot{DeliveryID: id}}
		}, wantIntent: worklifetime.DeliveryContinuationReturn, wantRelease: 1},
		{name: "wrong_authority", result: func(string) runtimedelivery.ClaimResult {
			return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimWrongAuthority, Invariant: errors.New("wrong authority")}
		}, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "absent", result: func(string) runtimedelivery.ClaimResult {
			return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimAbsent, Invariant: errors.New("absent")}
		}, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "invariant_invalid", result: func(string) runtimedelivery.ClaimResult {
			return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimInvariantInvalid, Invariant: errors.New("invalid")}
		}, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "unknown", result: func(string) runtimedelivery.ClaimResult {
			return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimDisposition("unknown")}
		}, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "acquired_without_claim", result: func(string) runtimedelivery.ClaimResult {
			return runtimedelivery.ClaimResult{Disposition: runtimedelivery.ClaimAcquired}
		}, wantIntent: worklifetime.DeliveryContinuationReturn},
		{name: "acquired_terminal_race", delegate: true, terminalRace: true, wantIntent: worklifetime.DeliveryContinuationConsume, wantRelease: 1},
		{name: "acquired_success", delegate: true, wantIntent: worklifetime.DeliveryContinuationConsume, wantRelease: 1, wantHandled: true},
	}
	for authorityName, authority := range projectionDeliveryAuthorities(t) {
		for _, test := range branches {
			t.Run(authorityName+"/"+test.name, func(t *testing.T) {
				bus := newProjectionTestBus()
				handled := make(chan int, 1)
				am := newProjectionTestManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
					return &projectionTestAgent{id: cfg.ID, subs: []events.EventType{"test.old"}, handled: handled}, nil
				})
				baseStore := am.deliveryStore
				const agentID = "carrier-disposition-agent"
				if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
					ExecutionMode: "live", ID: agentID,
					Identity: runtimeagentidentitytest.RootRuntime(t, agentID, "carrier-disposition-test"), Subscriptions: []string{"test.old"},
				})); err != nil {
					t.Fatalf("SpawnAgent: %v", err)
				}
				runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
				defer cancelRun()
				managedCtx := managedExecutionTestContext(t, runCtx)
				if authority.Kind() == runtimedelivery.ExecutionAuthoritySelectedContractFork {
					admission, err := managedexecution.New(
						managedexecution.KindSelectedContractFork, authority.ExecutionID(), authority.Generation(), authority.ForkRunID(),
						"carrier-disposition-actors", authority.BundleSource().BundleHash(), nil,
					)
					if err != nil {
						t.Fatalf("construct selected managed execution: %v", err)
					}
					managedCtx = managedexecution.WithAdmission(runCtx, admission)
				}
				if err := am.Run(managedCtx); err != nil {
					t.Fatalf("Run: %v", err)
				}
				runID := eventtest.UUID("projection-run")
				if authority.Kind() == runtimedelivery.ExecutionAuthoritySelectedContractFork {
					runID = authority.ForkRunID()
				}
				evt := eventtest.RuntimeControl(
					eventtest.UUID("carrier-disposition-"+authorityName+"-"+test.name), "test.old", "test", "", []byte(`{}`), 0,
					runID, "", events.EventEnvelope{}, time.Now(),
				)
				route := managerAgentDeliveryRoute(agentID)
				deliveryID, err := runtimedelivery.DeliveryID(evt.ID(), route)
				if err != nil {
					t.Fatalf("derive delivery identity: %v", err)
				}
				result := runtimedelivery.ClaimResult{}
				if test.result != nil {
					result = test.result(deliveryID)
				}
				am.deliveryStore = projectionScriptedClaimStore{Store: baseStore, result: result, err: test.claimErr, delegate: test.delegate}
				owner := &projectionResolutionOwner{resolved: make(chan projectionCarrierResolution, 1), released: make(chan struct{}, 1)}
				if test.terminalRace {
					owner.resolution = worklifetime.DeliveryContinuationTerminal
				}
				bus.continuations = owner
				bus.authority = authority
				if test.shutdown {
					am.runtimeShutdownAdmissionClosed = func() bool { return true }
				}
				workOwner := am.workOwner
				if test.missingWork {
					am.workOwner = nil
					defer func() { am.workOwner = workOwner }()
				}
				if test.fenceWork {
					if err := am.lifecycle.runOwner.Fence(); err != nil {
						t.Fatalf("fence manager work owner: %v", err)
					}
				}
				if test.cancelLane {
					canceled, cancel := context.WithCancel(context.Background())
					cancel()
					bus.deliveryCtx = canceled
				}
				if test.missingAuth {
					bus.authority = runtimedelivery.ExecutionAuthority{}
				}
				if err := bus.send(agentID, evt); err != nil {
					t.Fatalf("send branch delivery: %v", err)
				}
				select {
				case got := <-owner.resolved:
					if got.intent != test.wantIntent {
						t.Fatalf("carrier intent = %d, want %d", got.intent, test.wantIntent)
					}
					if test.terminalRace && got.resolution != worklifetime.DeliveryContinuationTerminal {
						t.Fatalf("terminal-race resolution = %d, want terminal", got.resolution)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("running manager did not resolve delivery carrier")
				}
				if test.wantHandled {
					select {
					case <-handled:
					case <-time.After(2 * time.Second):
						t.Fatal("acquired delivery did not reach the handler")
					}
				} else {
					select {
					case <-handled:
						t.Fatal("pre-attempt branch reached the handler")
					default:
					}
				}
				if test.wantRelease > 0 {
					select {
					case <-owner.released:
					case <-time.After(2 * time.Second):
						t.Fatal("running manager did not release terminal continuation")
					}
				}
				if got := owner.releases.Load(); got != test.wantRelease {
					t.Fatalf("continuation releases = %d, want %d", got, test.wantRelease)
				}
				if test.missingWork {
					am.workOwner = workOwner
				}
				if err := am.ShutdownWithOptions(ShutdownOptions{Grace: 2 * time.Second}); err != nil {
					t.Fatalf("bounded manager shutdown: %v", err)
				}
			})
		}
	}
}

func TestAgentManagerFenceWinsDequeuedDeliveryAdmissionReturnsContinuationOnce(t *testing.T) {
	bus := newProjectionTestBus()
	handled := make(chan int, 1)
	am := newProjectionTestManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		return &projectionTestAgent{id: cfg.ID, subs: []events.EventType{"test.old"}, handled: handled}, nil
	})
	const agentID = "fenced-dequeued-agent"
	identity := runtimeagentidentitytest.RootRuntime(t, agentID, "fenced-dequeued-test")
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: agentID, Identity: identity, Subscriptions: []string{"test.old"},
	})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	route, ok := bus.current(agentID)
	if !ok {
		t.Fatal("running route missing")
	}
	if err := am.teardownIdentityAfterTerminalEvent(runCtx, identity, "test_fence", true); err != nil {
		t.Fatalf("fence execution: %v", err)
	}

	evt := projectionRuntimeEvent("fenced-dequeued-event", "test.old")
	deliveryRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: identity}
	delivery, err := bus.owner.NewRoutedEventDelivery(testAuthorActivityContext(context.Background()), evt, deliveryRoute)
	if err != nil {
		t.Fatalf("construct dequeued carrier: %v", err)
	}
	owner := &projectionResolutionOwner{resolved: make(chan projectionCarrierResolution, 1), released: make(chan struct{}, 1)}
	continuation, err := owner.Acquire(eventtest.UUID("fenced-dequeued-delivery"))
	if err != nil {
		t.Fatalf("acquire continuation: %v", err)
	}
	if err := delivery.AttachContinuation(continuation); err != nil {
		t.Fatalf("attach continuation: %v", err)
	}
	var claims atomic.Int32
	am.deliveryStore = projectionScriptedClaimStore{Store: am.deliveryStore, delegate: true, claims: &claims}
	carrier, lease, err := am.admitDequeuedAgentDelivery(runCtx, delivery, route.token, agentID, evt)
	if err == nil || carrier != nil || lease != nil {
		t.Fatalf("fenced admission = carrier %v lease %v err %v, want rejected", carrier != nil, lease != nil, err)
	}
	select {
	case resolution := <-owner.resolved:
		if resolution.intent != worklifetime.DeliveryContinuationReturn || resolution.resolution != worklifetime.DeliveryContinuationReturned {
			t.Fatalf("fenced continuation = %+v, want one return", resolution)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fenced dequeued carrier was not returned")
	}
	if got := owner.resolves.Load(); got != 1 {
		t.Fatalf("continuation resolutions = %d, want exactly 1", got)
	}
	if got := claims.Load(); got != 0 {
		t.Fatalf("delivery claims after fence = %d, want 0", got)
	}
	select {
	case <-handled:
		t.Fatal("fenced dequeued delivery reached handler")
	default:
	}
	if err := am.ShutdownWithOptions(ShutdownOptions{Grace: 2 * time.Second}); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestAgentManagerDeferredSelfRetirementSettlesAcceptedAndReturnsBufferedDelivery(t *testing.T) {
	bus := newProjectionTestBus()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var am *AgentManager
	var retiring *projectionSelfRetiringAgent
	am = newProjectionTestManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		retiring = &projectionSelfRetiringAgent{
			projectionTestAgent: projectionTestAgent{id: cfg.ID, subs: []events.EventType{"test.old"}},
			identity:            cfg.Identity, started: started, release: release,
			retire: func(ctx context.Context, identity runtimeagentidentity.Identity) error {
				return am.teardownIdentityAfterTerminalEvent(ctx, identity, "flow_instance_terminal", true)
			},
		}
		return retiring, nil
	})
	const agentID = "self-retiring-buffered-agent"
	if err := spawnManagerTestAgent(am, managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: agentID,
		Identity:      runtimeagentidentitytest.RootRuntime(t, agentID, "self-retiring-buffered-test"),
		Subscriptions: []string{"test.old"},
	})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelRun()
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	authority := projectionDeliveryAuthorities(t)["normal"]
	bus.authority = authority
	owner := &projectionResolutionOwner{resolved: make(chan projectionCarrierResolution, 4), released: make(chan struct{}, 1)}
	bus.continuations = owner

	if err := bus.send(agentID, projectionRuntimeEvent("self-retiring-first", "test.old")); err != nil {
		t.Fatalf("send accepted delivery: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted delivery did not reach handler")
	}
	if err := bus.send(agentID, projectionRuntimeEvent("self-retiring-second", "test.old")); err != nil {
		t.Fatalf("buffer second delivery: %v", err)
	}
	close(release)

	intents := map[worklifetime.DeliveryContinuationIntent]int{}
	for len(intents) < 2 || intents[worklifetime.DeliveryContinuationConsume]+intents[worklifetime.DeliveryContinuationReturn] < 2 {
		select {
		case resolution := <-owner.resolved:
			intents[resolution.intent]++
		case <-time.After(2 * time.Second):
			t.Fatalf("continuation dispositions = %#v, want one consume and one return", intents)
		}
	}
	if intents[worklifetime.DeliveryContinuationConsume] != 1 || intents[worklifetime.DeliveryContinuationReturn] != 1 {
		t.Fatalf("continuation dispositions = %#v, want one consume and one return", intents)
	}
	if got := retiring.handled.Load(); got != 1 {
		t.Fatalf("handled deliveries = %d, want exactly 1", got)
	}
	if got := owner.resolves.Load(); got != 2 {
		t.Fatalf("continuation resolutions = %d, want exactly 2", got)
	}
	if err := am.ShutdownWithOptions(ShutdownOptions{Grace: 2 * time.Second}); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
