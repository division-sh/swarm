package runtime

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
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

type runtimeShutdownDeliveryStore struct {
	runtimedelivery.Store
	db      *sql.DB
	adapter *runtimedelivery.Adapter
	mu      sync.Mutex
	events  map[string]events.Event
}

func newRuntimeShutdownDeliveryStore(t *testing.T) *runtimeShutdownDeliveryStore {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+uuid.NewString()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open runtime shutdown delivery store: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT)`,
		`CREATE TABLE events (
			event_class TEXT NOT NULL, event_id TEXT PRIMARY KEY, run_id TEXT, event_name TEXT NOT NULL,
			task_id TEXT, entity_id TEXT, flow_instance TEXT, scope TEXT NOT NULL, payload BLOB NOT NULL,
			execution_mode TEXT NOT NULL, chain_depth INTEGER NOT NULL, produced_by TEXT NOT NULL,
			produced_by_type TEXT NOT NULL, source_event_id TEXT, created_at TIMESTAMP NOT NULL,
			routing_source_kind TEXT NOT NULL, routing_source_authority TEXT, source_route BLOB NOT NULL,
			target_route BLOB NOT NULL, target_set BLOB NOT NULL, operator_reference_event_id TEXT
		)`,
		`CREATE TABLE event_deliveries (
			delivery_id TEXT PRIMARY KEY, run_id TEXT, event_id TEXT NOT NULL, route_identity TEXT NOT NULL,
			subscriber_type TEXT NOT NULL, subscriber_id TEXT NOT NULL, delivery_target_route BLOB NOT NULL,
			delivery_context BLOB NOT NULL, delivery_payload_projection BLOB NOT NULL, status TEXT NOT NULL,
			retry_count INTEGER NOT NULL, max_retries INTEGER NOT NULL, next_eligible_at TIMESTAMP,
			claim_version INTEGER NOT NULL, current_attempt_version INTEGER, current_attempt_open BOOLEAN,
			reason_code TEXT, failure BLOB, started_at TIMESTAMP,
			settled_at TIMESTAMP, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
			UNIQUE(event_id, route_identity)
		)`,
		`CREATE TABLE event_delivery_attempts (
			delivery_id TEXT NOT NULL, claim_version INTEGER NOT NULL, claim_token TEXT NOT NULL UNIQUE,
			started_at TIMESTAMP NOT NULL, lease_expires_at TIMESTAMP NOT NULL,
			active_session_id TEXT, session_run_id TEXT, session_agent_id TEXT, open_marker BOOLEAN NOT NULL,
			outcome TEXT,
			reason_code TEXT, failure BLOB, side_effects BLOB NOT NULL DEFAULT '[]', duration_ms INTEGER,
			completed_at TIMESTAMP, PRIMARY KEY(delivery_id, claim_version)
		)`,
		`CREATE TABLE event_delivery_outcomes (
			delivery_id TEXT NOT NULL, claim_version INTEGER NOT NULL, outcome TEXT NOT NULL,
			reason_code TEXT, failure BLOB, side_effects BLOB NOT NULL DEFAULT '[]', duration_ms INTEGER NOT NULL,
			settled_at TIMESTAMP NOT NULL, PRIMARY KEY(delivery_id, claim_version)
		)`,
		`CREATE TABLE author_activity_order (
			singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
			last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0)
		)`,
		`CREATE TABLE author_activity_occurrences (
			occurrence_id TEXT PRIMARY KEY, sequence BIGINT NOT NULL UNIQUE CHECK (sequence > 0),
			kind TEXT NOT NULL, version INTEGER NOT NULL CHECK (version = 2), transition TEXT NOT NULL,
			source_owner TEXT NOT NULL, source_identity TEXT NOT NULL, dedup_key TEXT NOT NULL UNIQUE,
			run_id TEXT, entity_id TEXT, agent_id TEXT, flow_id TEXT, scope_kind TEXT NOT NULL,
			runtime_instance_id TEXT, bundle_hash TEXT, author_safe_summary TEXT,
			projection TEXT NOT NULL DEFAULT '{}', failure TEXT, occurred_at TIMESTAMP NOT NULL
		)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("create runtime shutdown delivery schema: %v", err)
		}
	}
	adapter, err := runtimedelivery.NewAdapter(runtimedelivery.DialectSQLite)
	if err != nil {
		t.Fatalf("create runtime shutdown delivery adapter: %v", err)
	}
	return &runtimeShutdownDeliveryStore{db: db, adapter: adapter, events: make(map[string]events.Event)}
}

func (s *runtimeShutdownDeliveryStore) mutate(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	story, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := fn(story, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := runtimeauthoractivity.Finalize(story); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *runtimeShutdownDeliveryStore) ClaimAgentDelivery(ctx context.Context, evt events.Event, route events.DeliveryRoute) (claimed runtimedelivery.ClaimedObligation, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := eventfixture.Insert(ctx, s.db, runtimeauthoractivity.DialectSQLite, evt); err != nil {
		return claimed, err
	}
	s.events[evt.ID()] = evt
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		if _, err := s.adapter.CommitInitial(story, tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route}); err != nil {
			return err
		}
		claimed, err = s.adapter.ClaimExact(story, tx, evt, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return claimed, err
}

func (s *runtimeShutdownDeliveryStore) seedAgentDelivery(t *testing.T, ctx context.Context, evt events.Event, agentID string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := eventfixture.Insert(ctx, s.db, runtimeauthoractivity.DialectSQLite, evt); err != nil {
		t.Fatalf("seed runtime delivery event: %v", err)
	}
	s.events[evt.ID()] = evt
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: agentID}
	if err := s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		_, err := s.adapter.CommitInitial(story, tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route})
		return err
	}); err != nil {
		t.Fatalf("seed runtime delivery obligation: %v", err)
	}
}

func (s *runtimeShutdownDeliveryStore) ClaimAgentBacklog(ctx context.Context, agentID string, limit int) (executions []runtimedelivery.AgentExecution, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var claimed []runtimedelivery.ClaimedObligation
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		claimed, err = s.adapter.ClaimPendingAgent(story, tx, agentID, limit, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	if err != nil {
		return nil, err
	}
	for _, obligation := range claimed {
		evt, ok := s.events[obligation.Snapshot.EventID]
		if !ok {
			return nil, runtimedelivery.ErrNotFound
		}
		executions = append(executions, runtimedelivery.AgentExecution{
			Event: evt, Snapshot: obligation.Snapshot, Claim: obligation.Claim,
		})
	}
	return executions, nil
}

func (s *runtimeShutdownDeliveryStore) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, effects []string, duration time.Duration) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.SettleSuccess(story, tx, claim, effects, duration)
		return err
	})
	return snapshot, err
}

func (s *runtimeShutdownDeliveryStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.RenewClaim(story, tx, claim, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return snapshot, err
}

func (s *runtimeShutdownDeliveryStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.SettleFailure(story, tx, claim, settlement)
		return err
	})
	return snapshot, err
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
	deliveryStore := newRuntimeShutdownDeliveryStore(t)
	bus, err := newRuntimeTestEventBus(t, nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	release := make(chan struct{})

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
	managerStore := &runtimeShutdownManagerStore{}
	am := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != agent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
		WorkOwner:                      workOwner,
		DeliveryStore:                  deliveryStore,
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
	if err := bus.Publish(testAuthorActivityContext(context.Background()), eventtest.RunCreatingRootIngress(eventtest.UUID("runtime-shutdown-inbound-1"),
		events.EventType("test.in"),
		"tester", "", nil, 0, eventtest.UUID("runtime-shutdown-run-1"), "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
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
	deliveryStore := newRuntimeShutdownDeliveryStore(t)
	bus, err := newRuntimeTestEventBus(t, nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	release := make(chan struct{})

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
	am := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
		WorkOwner:                      workOwner,
		DeliveryStore:                  deliveryStore,
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
	if err := bus.Publish(testAuthorActivityContext(context.Background()), eventtest.RunCreatingRootIngress(eventtest.UUID("runtime-shutdown-grace-inbound-1"),
		events.EventType("test.in"),
		"tester", "", nil, 0, eventtest.UUID("runtime-shutdown-grace-run-1"), "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
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
	close(release)
	err = <-shutdownErrCh
	if err == nil || !strings.Contains(err.Error(), "agent manager shutdown:") {
		t.Fatalf("ShutdownWithOptions err = %v, want configured grace manager timeout", err)
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
	hash := "bundle-v1:sha256:" + strings.Repeat("7", 64)
	workOwner := runtimeTestOccurrence(t, hash)
	bus, err := newRuntimeTestEventBusWithOptions(t, eventStore, runtimebus.EventBusOptions{WorkOwner: workOwner})
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
