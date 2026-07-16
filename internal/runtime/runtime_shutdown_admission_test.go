package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

type runtimeShutdownTestAgent struct {
	id            string
	subscriptions []events.EventType
	onEvent       func(context.Context, events.Event) ([]events.Event, error)
}

func (a runtimeShutdownTestAgent) ID() string { return a.id }
func (runtimeShutdownTestAgent) Type() string { return "test" }
func (a runtimeShutdownTestAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}
func (a runtimeShutdownTestAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	return a.onEvent(ctx, evt)
}

type runtimeShutdownManagerStore struct {
	listPendingCalled bool
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

func (*runtimeShutdownManagerStore) UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, *runtimefailures.Envelope) error {
	return nil
}

func (s *runtimeShutdownManagerStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	s.listPendingCalled = true
	return nil, nil
}

func (s *runtimeShutdownManagerStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	s.listPendingCalled = true
	return nil, nil
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

func (s *cancellationBlockingInboundStore) RunInboundPublicationMutation(ctx context.Context, _ runtimeinbound.Request, _ func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return runtimeinbound.Record{}, ctx.Err()
}

func (*cancellationBlockingInboundStore) LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, nil
}

func (*cancellationBlockingInboundStore) ValidateInboundPublicationIntegrity(context.Context) error {
	return nil
}

func (s *runtimeShutdownInboundStore) bindTestInboundEventStore(store runtimebus.EventStore) {
	s.store = store
}

func (s *runtimeShutdownInboundStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	s.recorded = true
	return runTestInboundPublication(ctx, s.store, request, true, fn)
}

func (s *runtimeShutdownInboundStore) ResolveInboundTarget(context.Context, string, string) (InboundTarget, error) {
	return InboundTarget{EntityID: "entity-1", EntitySlug: "entity-1"}, nil
}

func (*runtimeShutdownInboundStore) LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, nil
}

func (*runtimeShutdownInboundStore) ValidateInboundPublicationIntegrity(context.Context) error {
	return nil
}

func TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	agent := runtimeShutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, nil
		},
	}

	rt := &Runtime{}
	managerStore := &runtimeShutdownManagerStore{}
	am := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != agent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
	}, managerStore)
	rt.Manager = am

	inboundStore := &runtimeShutdownInboundStore{}
	testInbound := newTestInboundGateway(t, bus, nil, rt.shutdownAdmissionClosed, inboundStore)
	rt.InboundGateway = testInbound.InboundGateway

	if err := am.SpawnAgent(runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            agent.id,
		Subscriptions: []string{"test.in"},
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := am.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := bus.Publish(testAuthorActivityContext(context.Background()), eventtest.RootIngress("evt-in-1",
		events.EventType("test.in"),
		"tester", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight work to start")
	}

	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- rt.Shutdown()
	}()

	select {
	case err := <-shutdownErrCh:
		t.Fatalf("Shutdown returned before in-flight work drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if !rt.shutdownAdmissionClosed() {
		t.Fatal("runtime shutdown admission was not closed before manager drain")
	}
	if err := am.ReplayAgentBacklog(testAuthorActivityContext(context.Background()), agent.id); err == nil || !strings.Contains(err.Error(), "runtime shutting down") {
		t.Fatalf("ReplayAgentBacklog during shutdown err = %v, want runtime shutting down", err)
	}
	if managerStore.listPendingCalled {
		t.Fatal("ReplayAgentBacklog touched manager persistence even though runtime shutdown admission was closed")
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

	close(release)

	select {
	case err := <-shutdownErrCh:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown to finish")
	}
}

func TestRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrain(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	started := make(chan struct{}, 1)

	agent := runtimeShutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	rt := &Runtime{}
	am := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
	})
	rt.Manager = am

	if err := am.SpawnAgent(runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            agent.id,
		Subscriptions: []string{"test.in"},
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := am.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := bus.Publish(testAuthorActivityContext(context.Background()), eventtest.RootIngress("evt-in-1",
		events.EventType("test.in"),
		"tester", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight work to start")
	}

	grace := 25 * time.Millisecond
	startedAt := time.Now()
	err = rt.ShutdownWithOptions(ShutdownOptions{Grace: grace})
	elapsed := time.Since(startedAt)
	if err == nil || !strings.Contains(err.Error(), "agent manager shutdown:") {
		t.Fatalf("ShutdownWithOptions err = %v, want configured grace manager timeout", err)
	}
	if elapsed > 150*time.Millisecond {
		t.Fatalf("ShutdownWithOptions elapsed = %s, want configured %s bound", elapsed, grace)
	}
	if !rt.shutdownAdmissionClosed() {
		t.Fatal("runtime shutdown admission was not closed")
	}
}

func TestRuntimeContextDeactivationCancelsStuckWebhookWithoutPublishing(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	publicationStore := &cancellationBlockingInboundStore{
		entered: make(chan struct{}, 1),
	}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &Runtime{Bus: bus}
	gateway := newTestInboundGateway(t, bus, nil, rt.shutdownAdmissionClosed, publicationStore)
	gateway.SetAdmissionGuard(rt.shutdownGate.BeginContext)
	rt.InboundGateway = gateway.InboundGateway
	hash := "bundle-v1:sha256:" + strings.Repeat("7", 64)
	contextDef := testBundleContext(t, hash, "inbound.telegram")
	contextDef.Runtime = rt
	manager, err := NewRuntimeContextManager(nil, contextDef)
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
			BundleHash: hash, FlowID: "chat", RunID: "41000000-0000-0000-0000-000000000001",
			FlowInstance: "chat/a", EntityID: "41000000-0000-0000-0000-000000000002",
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
