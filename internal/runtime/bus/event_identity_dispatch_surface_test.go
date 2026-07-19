package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type completeEventDispatchStore interface {
	runtimebus.EventStore
	runtimemanager.ManagerPersistence
	UpsertPipelineReceipt(context.Context, string, string, *runtimefailures.Envelope) error
}

type completeEventDispatchFixture struct {
	store   completeEventDispatchStore
	db      *sql.DB
	dialect string
	ctx     context.Context
	bus     *runtimebus.EventBus
	event   events.Event
	agentID string
}

func TestCompleteEventSnapshotDispatchesThroughRecoveryOwnersOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, surface := range []string{"startup", "global_sweeper", "run_queue", "decision_obligation"} {
			t.Run(backend+"/"+surface, func(t *testing.T) {
				fixture := newCompleteEventDispatchFixture(t, backend, surface == "decision_obligation")
				ch := fixture.bus.Subscribe(fixture.agentID, fixture.event.Type())
				defer fixture.bus.Unsubscribe(fixture.agentID)

				if err := fixture.updateChainDepth(-1); err == nil {
					t.Fatalf("%s schema admitted negative chain_depth", backend)
				}
				if _, err := fixture.invoke(surface); err != nil {
					t.Fatalf("%s dispatch: %v", surface, err)
				}
				assertCompleteEventDelivery(t, ch, fixture.event)
			})
		}
	}
}

func TestCompleteEventSnapshotDispatchesThroughManagerBacklogOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			if err := fixture.store.UpsertAgent(fixture.ctx, runtimemanager.PersistedAgent{
				Config: runtimeactors.AgentConfig{
					ID:            fixture.agentID,
					Role:          "complete-event-proof",
					Type:          "recording",
					FlowID:        "global",
					Model:         "regular",
					ExecutionMode: executionmode.Live,
					Subscriptions: []string{string(fixture.event.Type())},
					Config:        []byte(`{"system_prompt":"complete event proof"}`),
				},
				Status:    "active",
				HiredBy:   "complete-event-proof",
				StartedAt: fixture.event.CreatedAt().Add(-time.Minute),
			}); err != nil {
				t.Fatalf("UpsertAgent: %v", err)
			}
			if err := fixture.store.UpsertPipelineReceipt(fixture.ctx, fixture.event.ID(), "processed", nil); err != nil {
				t.Fatalf("UpsertPipelineReceipt: %v", err)
			}

			if err := fixture.updateChainDepth(-1); err == nil {
				t.Fatalf("%s schema admitted negative chain_depth", backend)
			}
			seen := make(chan events.Event, 1)
			manager := fixture.newRecordingManager(t, seen)
			managerCtx := fixture.managedContext(t)
			if _, err := manager.HydrateForStartup(managerCtx); err != nil {
				t.Fatalf("hydrate manager: %v", err)
			}
			if _, err := manager.ReplayBacklog(managerCtx, runtimeagentcontrol.ReplayBacklogRequest{AgentID: fixture.agentID}); err != nil {
				t.Fatalf("manager backlog replay: %v", err)
			}
			assertCompleteEventDelivery(t, seen, fixture.event)
		})
	}
}

func newCompleteEventDispatchFixture(t *testing.T, backend string, decisionObligation bool) completeEventDispatchFixture {
	t.Helper()
	var selected completeEventDispatchStore
	var db *sql.DB
	switch backend {
	case "sqlite":
		sqlite := storetest.StartSQLiteRuntimeStore(t)
		selected, db = sqlite, sqlite.DB
	case "postgres":
		_, postgresDB, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		postgres := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
		selected, db = postgres, postgresDB
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	bus, err := newScopedTestEventBus(selected)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := testAuthorActivityContext(context.Background())
	createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	runID, eventID := uuid.NewString(), uuid.NewString()
	seedCompleteEventDispatchRun(t, ctx, db, backend, runID, createdAt)
	sourceRoute := events.RouteIdentity{
		FlowID: "source-flow", FlowInstance: "source-flow/one", EntityID: uuid.NewString(),
	}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute)
	event := eventtest.InExecutionMode(eventtest.PersistedProjectionForProducer(
		eventID,
		events.EventType("custom.replay.checked"),
		eventtest.Producer(events.EventProducerNode, "declarative-node"),
		"event-owned-task",
		[]byte(`{"task_id":"payload-owned-task","text":"complete snapshot"}`),
		3,
		runID,
		"",
		envelope,
		createdAt,
	), executionmode.Mock)
	agentID := "complete-event-agent"
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{{SubscriberType: "agent", SubscriberID: agentID}}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	fixture := completeEventDispatchFixture{
		store: selected, db: db, dialect: backend, ctx: ctx, bus: bus, event: event, agentID: agentID,
	}
	if decisionObligation {
		fixture.insertDecisionObligation(t)
	}
	return fixture
}

func seedCompleteEventDispatchRun(t testing.TB, ctx context.Context, db *sql.DB, backend, runID string, startedAt time.Time) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`
	if backend == "postgres" {
		query = `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`
	}
	if _, err := db.ExecContext(ctx, query, runID, startedAt); err != nil {
		t.Fatalf("seed %s complete-event run: %v", backend, err)
	}
}

func (f completeEventDispatchFixture) invoke(surface string) (int, error) {
	switch surface {
	case "startup":
		return 0, runtimepipeline.NewRecoveryManagerWith(f.store, f.bus).Recover(f.ctx)
	case "global_sweeper", "decision_obligation":
		return f.bus.SweepUndispatched(f.ctx, time.Hour, 10)
	case "run_queue":
		return f.bus.ReleaseRunQueue(f.ctx, f.event.RunID(), time.Hour, 10)
	default:
		return 0, errors.New("unknown complete event dispatch surface")
	}
}

func (f completeEventDispatchFixture) updateChainDepth(depth int) error {
	query := `UPDATE events SET chain_depth = ? WHERE event_id = ?`
	args := []any{depth, f.event.ID()}
	if f.dialect == "postgres" {
		query = `UPDATE events SET chain_depth = $1 WHERE event_id = $2::uuid`
	}
	_, err := f.db.ExecContext(f.ctx, query, args...)
	return err
}

func (f completeEventDispatchFixture) assertNoDispatchMutation(t *testing.T) {
	t.Helper()
	var pipelineReceipts int
	query := `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if f.dialect == "postgres" {
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	if err := f.db.QueryRowContext(f.ctx, query, f.event.ID()).Scan(&pipelineReceipts); err != nil {
		t.Fatalf("count pipeline receipts: %v", err)
	}
	if pipelineReceipts != 0 {
		t.Fatalf("pipeline receipts after corrupt readback = %d, want 0", pipelineReceipts)
	}
	if f.hasDecisionObligation(t) {
		var status string
		query = `SELECT status FROM decision_card_route_obligations WHERE event_id = ?`
		if f.dialect == "postgres" {
			query = `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`
		}
		if err := f.db.QueryRowContext(f.ctx, query, f.event.ID()).Scan(&status); err != nil {
			t.Fatalf("load decision obligation status: %v", err)
		}
		if status != "pending" {
			t.Fatalf("decision obligation status after corrupt readback = %q, want pending", status)
		}
	}
}

func (f completeEventDispatchFixture) assertNoAgentDispatchMutation(t *testing.T) {
	t.Helper()
	var receipts int
	query := `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'agent' AND subscriber_id = ?`
	args := []any{f.event.ID(), f.agentID}
	if f.dialect == "postgres" {
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'agent' AND subscriber_id = $2`
	}
	if err := f.db.QueryRowContext(f.ctx, query, args...).Scan(&receipts); err != nil {
		t.Fatalf("count agent receipts: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("agent receipts after corrupt readback = %d, want 0", receipts)
	}
}

func (f completeEventDispatchFixture) hasDecisionObligation(t *testing.T) bool {
	t.Helper()
	var count int
	query := `SELECT COUNT(*) FROM decision_card_route_obligations WHERE event_id = ?`
	if f.dialect == "postgres" {
		query = `SELECT COUNT(*) FROM decision_card_route_obligations WHERE event_id = $1::uuid`
	}
	if err := f.db.QueryRowContext(f.ctx, query, f.event.ID()).Scan(&count); err != nil {
		t.Fatalf("count decision obligations: %v", err)
	}
	return count > 0
}

func (f completeEventDispatchFixture) insertDecisionObligation(t *testing.T) {
	t.Helper()
	cardID := uuid.NewString()
	if f.dialect == "postgres" {
		if _, err := f.db.ExecContext(f.ctx, `
			INSERT INTO decision_cards (
				card_id, run_id, anchor_kind, anchor, status, execution_mode, snapshot,
				card_content_hash, decision_schema_hash, bundle_hash, effective_cadence,
				provenance, verdict, fields, decided_by, decided_at, decision_event_id,
				created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, 'stage_gate', '{}'::jsonb, 'decided', 'mock', '{}'::jsonb,
				'card-hash', 'schema-hash', 'bundle-hash', '{}'::jsonb,
				'{}'::jsonb, 'approve', '{}'::jsonb, 'test', $3, $4::uuid, $3, $3
			)
		`, cardID, f.event.RunID(), f.event.CreatedAt(), f.event.ID()); err != nil {
			t.Fatalf("insert decision card: %v", err)
		}
		if _, err := f.db.ExecContext(f.ctx, `
			INSERT INTO decision_card_route_obligations (
				event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3::uuid, 'pending', 0, $4, $4, $4)
		`, f.event.ID(), cardID, f.event.RunID(), f.event.CreatedAt()); err != nil {
			t.Fatalf("insert decision obligation: %v", err)
		}
		return
	}
	if _, err := f.db.ExecContext(f.ctx, `
		INSERT INTO decision_cards (
			card_id, run_id, anchor_kind, anchor, status, execution_mode, snapshot,
			card_content_hash, decision_schema_hash, bundle_hash, effective_cadence,
			provenance, verdict, fields, decided_by, decided_at, decision_event_id,
			created_at, updated_at
		) VALUES (?, ?, 'stage_gate', '{}', 'decided', 'mock', '{}',
			'card-hash', 'schema-hash', 'bundle-hash', '{}', '{}', 'approve', '{}',
			'test', ?, ?, ?, ?)
	`, cardID, f.event.RunID(), f.event.CreatedAt(), f.event.ID(), f.event.CreatedAt(), f.event.CreatedAt()); err != nil {
		t.Fatalf("insert decision card: %v", err)
	}
	if _, err := f.db.ExecContext(f.ctx, `
		INSERT INTO decision_card_route_obligations (
			event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at
		) VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
	`, f.event.ID(), cardID, f.event.RunID(), f.event.CreatedAt(), f.event.CreatedAt(), f.event.CreatedAt()); err != nil {
		t.Fatalf("insert decision obligation: %v", err)
	}
}

func (f completeEventDispatchFixture) managedContext(t *testing.T) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"complete-event-dispatch",
		1,
		"",
		"complete-event-proof",
		"complete-event-bundle",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed execution admission: %v", err)
	}
	return managedexecution.WithAdmission(f.ctx, admission)
}

func (f completeEventDispatchFixture) newRecordingManager(t *testing.T, seen chan<- events.Event) *runtimemanager.AgentManager {
	t.Helper()
	return runtimemanager.NewAgentManager(f.bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return &completeEventRecordingAgent{id: cfg.ID, subscriptions: []events.EventType{f.event.Type()}, seen: seen}, nil
	}, f.store)
}

type completeEventRecordingAgent struct {
	id            string
	subscriptions []events.EventType
	seen          chan<- events.Event
}

func (a *completeEventRecordingAgent) ID() string { return a.id }

func (*completeEventRecordingAgent) Type() string { return "recording" }

func (a *completeEventRecordingAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}

func (a *completeEventRecordingAgent) OnEvent(_ context.Context, event events.Event) ([]events.Event, error) {
	a.seen <- event
	return nil, nil
}

func assertCompleteEventDelivery(t *testing.T, delivered <-chan events.Event, want events.Event) {
	t.Helper()
	select {
	case got := <-delivered:
		assertCompleteEventSnapshot(t, got, want)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for complete event dispatch")
	}
}

func assertNoCompleteEventDelivery(t *testing.T, delivered <-chan events.Event) {
	t.Helper()
	select {
	case got := <-delivered:
		t.Fatalf("corrupt event dispatched: %#v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertCompleteEventSnapshot(t *testing.T, got, want events.Event) {
	t.Helper()
	var gotPayload, wantPayload any
	if err := json.Unmarshal(got.Payload(), &gotPayload); err != nil {
		t.Fatalf("decode delivered payload: %v", err)
	}
	if err := json.Unmarshal(want.Payload(), &wantPayload); err != nil {
		t.Fatalf("decode expected payload: %v", err)
	}
	if got.ID() != want.ID() || got.Type() != want.Type() || !got.Producer().Equal(want.Producer()) ||
		got.TaskID() != want.TaskID() || got.ChainDepth() != want.ChainDepth() || got.RunID() != want.RunID() ||
		got.ParentEventID() != want.ParentEventID() || got.ExecutionMode() != want.ExecutionMode() ||
		!got.CreatedAt().Equal(want.CreatedAt()) || !reflect.DeepEqual(gotPayload, wantPayload) ||
		!reflect.DeepEqual(got.Envelope(), want.Envelope()) {
		t.Fatalf("complete event snapshot changed\n got: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v\nwant: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v",
			got.ID(), got.Type(), got.ProducerType(), got.SourceAgent(), got.TaskID(), got.ChainDepth(), got.RunID(), got.ParentEventID(), got.ExecutionMode(), got.CreatedAt(), got.Payload(), got.Envelope(),
			want.ID(), want.Type(), want.ProducerType(), want.SourceAgent(), want.TaskID(), want.ChainDepth(), want.RunID(), want.ParentEventID(), want.ExecutionMode(), want.CreatedAt(), want.Payload(), want.Envelope())
	}
}

var _ completeEventDispatchStore = (*store.PostgresStore)(nil)
var _ completeEventDispatchStore = (*store.SQLiteRuntimeStore)(nil)
