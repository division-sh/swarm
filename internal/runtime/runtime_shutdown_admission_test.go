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

func (*runtimeShutdownManagerStore) MarkAgentTerminated(context.Context, string) error {
	return nil
}

func (*runtimeShutdownManagerStore) EnsureEntitySchema(context.Context, string) error {
	return nil
}

func (*runtimeShutdownManagerStore) UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, string) error {
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
}

func (s *runtimeShutdownInboundStore) RecordInboundEvent(context.Context, string, string, string) (bool, error) {
	s.recorded = true
	return true, nil
}

func (s *runtimeShutdownInboundStore) ResolveInboundTarget(context.Context, string, string) (InboundTarget, error) {
	return InboundTarget{EntityID: "entity-1", EntitySlug: "entity-1"}, nil
}

func (*runtimeShutdownInboundStore) PurgeInboundEventsBefore(context.Context, time.Time, int) (int, error) {
	return 0, nil
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
	rt.InboundGateway = NewInboundGateway(bus, nil, rt.shutdownAdmissionClosed, inboundStore)

	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: agent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	am.Run(context.Background())
	if err := bus.Publish(context.Background(), eventtest.RootIngress("evt-in-1",
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
	if err := am.ReplayAgentBacklog(context.Background(), agent.id); err == nil || !strings.Contains(err.Error(), "runtime shutting down") {
		t.Fatalf("ReplayAgentBacklog during shutdown err = %v, want runtime shutting down", err)
	}
	if managerStore.listPendingCalled {
		t.Fatal("ReplayAgentBacklog touched manager persistence even though runtime shutdown admission was closed")
	}

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-1","type":"push"}`))
	rec := httptest.NewRecorder()
	rt.InboundGateway.Handler().ServeHTTP(rec, req)
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

	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: agent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	am.Run(context.Background())
	if err := bus.Publish(context.Background(), eventtest.RootIngress("evt-in-1",
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
	err = rt.ShutdownWithOptions(ShutdownOptions{Grace: grace})
	if err == nil || !strings.Contains(err.Error(), "agent manager shutdown drain timed out after 25ms") {
		t.Fatalf("ShutdownWithOptions err = %v, want configured grace manager timeout", err)
	}
	if !rt.shutdownAdmissionClosed() {
		t.Fatal("runtime shutdown admission was not closed")
	}
}
