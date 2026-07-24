package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type forkedConsumerTestBackend struct {
	name      string
	db        *sql.DB
	postgres  *PostgresStore
	sqlite    *SQLiteRuntimeStore
	sourceRun string
	continued string
	forkedAt  time.Time
}

func newForkedConsumerTestBackend(t *testing.T, backend string) *forkedConsumerTestBackend {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	out := &forkedConsumerTestBackend{name: backend, sourceRun: uuid.NewString(), continued: uuid.NewString(), forkedAt: now}
	switch backend {
	case "postgres":
		_, db, _ := testutil.StartPostgres(t)
		out.db = db
		out.postgres = admitTestPostgresStore(t, db)
		if _, err := db.Exec(`INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at) VALUES ($1::uuid, 'running', $2, 'ephemeral', $3)`, out.sourceRun, authorActivityTestBundleHash, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO runs (run_id, status, forked_from_run_id, forked_from_event_id, bundle_hash, bundle_source, started_at) VALUES ($1::uuid, 'paused', $2::uuid, $3::uuid, $4, 'ephemeral', $5)`, out.continued, out.sourceRun, uuid.NewString(), authorActivityTestBundleHash, now); err != nil {
			t.Fatal(err)
		}
	case "sqlite":
		out.sqlite = newBootstrappedSQLiteRuntimeStoreForTest(t)
		out.db = out.sqlite.DB
		if _, err := out.db.Exec(`INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at) VALUES (?, 'running', ?, 'ephemeral', ?)`, out.sourceRun, authorActivityTestBundleHash, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := out.db.Exec(`INSERT INTO runs (run_id, status, forked_from_run_id, forked_from_event_id, bundle_hash, bundle_source, started_at) VALUES (?, 'paused', ?, ?, ?, 'ephemeral', ?)`, out.continued, out.sourceRun, uuid.NewString(), authorActivityTestBundleHash, now); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown backend %q", backend)
	}
	return out
}

func (b *forkedConsumerTestBackend) freeze(t *testing.T) {
	t.Helper()
	ctx := testAuthorActivityBundleSourceContext()
	if b.postgres != nil {
		lineage := runForkActivationLineage{
			SourceRunID: b.sourceRun, ForkRunID: b.continued, ForkEventID: uuid.NewString(),
			ForkEventName: "consumer.freeze", ForkEventTime: b.forkedAt, SourceRunStatus: "running", ForkStatus: "paused",
			SourceBundleHash: authorActivityTestBundleHash, ForkBundleHash: authorActivityTestBundleHash,
		}
		if err := commitRunForkSourceFreezeForTest(ctx, b.db, lineage, b.forkedAt, true); err != nil {
			t.Fatal(err)
		}
		return
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'forked', ended_at = ?, continued_as_run_id = ? WHERE run_id = ? AND status IN ('running', 'paused')`, b.forkedAt, b.continued, b.sourceRun); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'running' WHERE run_id = ? AND status = 'paused'`, b.continued); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func requireForkedSourceRefusal(t *testing.T, label string, err error) {
	t.Helper()
	if !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
		t.Fatalf("%s error = %v, want run-not-active", label, err)
	}
}

func TestForkedSourceEventDeliveryAndReplayConsumersRefuseAndSelectorsExclude(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedConsumerTestBackend(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			eventID := uuid.NewString()
			event := eventtest.PersistedProjectionForProducer(
				eventID, events.EventType("freeze.pending"), eventtest.Producer(events.EventProducerPlatform, "test"),
				"", []byte(`{}`), 0, fixture.sourceRun, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, fixture.forkedAt.Add(-time.Minute),
			)
			route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "freeze-agent"}
			var claim runtimedelivery.Claim
			if fixture.postgres != nil {
				if err := commitSemanticEventFixtureWithAgents(ctx, fixture.postgres, event, []string{"freeze-agent"}); err != nil {
					t.Fatal(err)
				}
				claimed, err := fixture.postgres.ClaimAgentDelivery(ctx, event, route)
				if err != nil {
					t.Fatal(err)
				}
				claim = claimed.Claim
				if _, err := fixture.postgres.SettleSuccess(ctx, claim, nil, 0); err != nil {
					t.Fatalf("settle source delivery before freeze: %v", err)
				}
			} else {
				if err := commitSemanticEventFixtureWithAgents(ctx, fixture.sqlite, event, []string{"freeze-agent"}); err != nil {
					t.Fatal(err)
				}
				claimed, err := fixture.sqlite.ClaimAgentDelivery(ctx, event, route)
				if err != nil {
					t.Fatal(err)
				}
				claim = claimed.Claim
				if _, err := fixture.sqlite.SettleSuccess(ctx, claim, nil, 0); err != nil {
					t.Fatalf("settle source delivery before freeze: %v", err)
				}
			}
			fixture.freeze(t)

			if fixture.postgres != nil {
				assertForkedEventConsumerRefusals(t, fixture.postgres, event, route, claim)
				assertForkedEventSelectors(t, fixture.postgres, fixture.sourceRun, eventID)
			} else {
				assertForkedEventConsumerRefusals(t, fixture.sqlite, event, route, claim)
				assertForkedEventSelectors(t, fixture.sqlite, fixture.sourceRun, eventID)
			}
			var status string
			query := `SELECT status FROM event_deliveries WHERE event_id = ? AND subscriber_id = 'freeze-agent'`
			if fixture.postgres != nil {
				query = `SELECT status FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_id = 'freeze-agent'`
			}
			if err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&status); err != nil || status != "delivered" {
				t.Fatalf("preserved delivery status = %q, %v", status, err)
			}
		})
	}
}

func assertForkedEventConsumerRefusals(t *testing.T, store any, event events.Event, route events.DeliveryRoute, claim runtimedelivery.Claim) {
	t.Helper()
	ctx := testAuthorActivityBundleSourceContext()
	s, ok := store.(forkedEventSelectorSurface)
	if !ok {
		t.Fatalf("unsupported event store %T", store)
	}
	_, err := s.ClaimAgentDelivery(ctx, event, route)
	requireForkedSourceRefusal(t, "delivery claim", err)
	_, err = s.SettleSuccess(ctx, claim, nil, 0)
	requireForkedSourceRefusal(t, "delivery settlement", err)
	if _, err := s.PipelineObligations().ClaimEvent(ctx, event.ID(), runtimepipelineobligation.PurposeRecovery); !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
		t.Fatalf("pipeline recovery claim error = %v, want ineligible", err)
	}
	publication, err := s.PipelineObligations().ClaimPublication(ctx, event.ID())
	if err != nil {
		t.Fatalf("publication serialization claim: %v", err)
	}
	if err := s.PipelineObligations().Release(ctx, publication); err != nil {
		t.Fatalf("release publication serialization claim: %v", err)
	}
}

type forkedEventSelectorSurface interface {
	runtimedelivery.Store
	PipelineObligations() runtimepipelineobligation.Store
}

func assertForkedEventSelectors(t *testing.T, store forkedEventSelectorSurface, runID, eventID string) {
	t.Helper()
	ctx := testAuthorActivityBundleSourceContext()
	work, ok, err := store.PipelineObligations().ClaimNext(ctx, runtimepipelineobligation.RunRecoveryQuery(runID))
	if err != nil || ok {
		t.Fatalf("pipeline selector returned frozen event: work=%#v ok=%t err=%v", work, ok, err)
	}
	summary, err := store.SummarizeRun(ctx, runID)
	if err != nil {
		t.Fatalf("delivery summary: %v", err)
	}
	if !summary.Settled() {
		t.Fatalf("delivery selector retained frozen work for %s: %#v", eventID, summary)
	}
}

func TestForkedSourceTimerConsumersRefuseWhileClaimsCanBeReleased(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedConsumerTestBackend(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			schedule := runtimepipeline.Schedule{
				RunID: fixture.sourceRun, AgentID: "freeze-agent", EventType: "freeze.timer", Mode: "once",
				At: fixture.forkedAt.Add(time.Hour), TaskID: "freeze-timer", Payload: []byte(`{"timer":true}`),
			}
			var store interface {
				UpsertSchedule(context.Context, runtimepipeline.Schedule) error
				CancelScheduleExact(context.Context, runtimepipeline.Schedule) error
				CancelScheduleExactTerminal(context.Context, runtimepipeline.Schedule) error
				CompleteScheduleFireExact(context.Context, runtimepipeline.Schedule) error
				ClaimSchedule(context.Context, runtimepipeline.Schedule) (bool, error)
				ReleaseSchedule(context.Context, runtimepipeline.Schedule) error
				LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error)
			}
			if fixture.postgres != nil {
				store = fixture.postgres
			} else {
				store = fixture.sqlite
			}
			if err := store.UpsertSchedule(ctx, schedule); err != nil {
				t.Fatal(err)
			}
			claimed, err := store.ClaimSchedule(ctx, schedule)
			if err != nil || !claimed {
				t.Fatalf("active claim = %v, %v", claimed, err)
			}
			fixture.freeze(t)

			for label, mutate := range map[string]func() error{
				"upsert":          func() error { return store.UpsertSchedule(ctx, schedule) },
				"cancel":          func() error { return store.CancelScheduleExact(ctx, schedule) },
				"terminal cancel": func() error { return store.CancelScheduleExactTerminal(ctx, schedule) },
				"fire":            func() error { return store.CompleteScheduleFireExact(ctx, schedule) },
			} {
				requireForkedSourceRefusal(t, "timer "+label, mutate())
			}
			if claimed, err := store.ClaimSchedule(ctx, schedule); err != nil || claimed {
				t.Fatalf("post-freeze timer claim = %v, %v", claimed, err)
			}
			if err := store.ReleaseSchedule(ctx, schedule); err != nil {
				t.Fatalf("release pre-freeze ownership: %v", err)
			}
			active, err := store.LoadActiveSchedules(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range active {
				if row.RunID == fixture.sourceRun {
					t.Fatalf("active selector returned frozen timer: %#v", row)
				}
			}
			var status string
			query := `SELECT status FROM timers WHERE run_id = ? AND owner_agent = ? AND fire_event = ?`
			if fixture.postgres != nil {
				query = `SELECT status FROM timers WHERE run_id = $1::uuid AND owner_agent = $2 AND fire_event = $3`
			}
			if err := fixture.db.QueryRowContext(ctx, query, fixture.sourceRun, schedule.AgentID, schedule.EventType).Scan(&status); err != nil || status != "active" {
				t.Fatalf("preserved timer status = %q, %v", status, err)
			}
		})
	}
}

func TestForkedSourceSessionTurnAndConversationConsumersRefuse(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedConsumerTestBackend(t, backend)
			fixture.freeze(t)
			ctx := runtimeeffects.WithExecutionMode(testAuthorActivityBundleSourceContext(), runtimeeffects.ExecutionModeLive)
			identity := agentmemory.Identity{RunID: fixture.sourceRun, AgentID: "freeze-agent", FlowInstance: "freeze/flow"}
			lease := &runtimesessions.Lease{SessionID: uuid.NewString(), Identity: identity, LockOwner: "worker", ExpiresAt: time.Now().Add(time.Minute)}
			conversation := runtimellm.ConversationRecord{
				SessionID: lease.SessionID, AgentID: identity.AgentID, Identity: identity, Memory: agentmemory.Authored(true),
				TurnCount: 1, Status: "active",
			}
			turn := runtimellm.AgentTurnRecord{
				SessionID: uuid.NewString(), AgentID: identity.AgentID, RunID: identity.RunID, FlowInstance: identity.FlowInstance,
				Memory: agentmemory.PlatformDefault(), RequestPayload: []byte(`{"request":true}`), ResponseRaw: []byte(`{"ok":true}`), ParseOK: true,
			}
			turn = managedAgentTurnRecordForTest(t, turn)
			watchdog := runtimellm.ConversationWatchdogUpdate{
				SessionID: lease.SessionID, AgentID: identity.AgentID, Identity: identity,
				Watchdog: &runtimellm.ConversationWatchdog{State: "healthy_long_running", BlockingLayer: "session_execution", Action: "turn_long_running", Outcome: "observed", LastOutputAt: "2026-07-15T12:00:00Z", RecordedAt: "2026-07-15T12:00:30Z"},
			}
			var store interface {
				Acquire(context.Context, agentmemory.Identity, string) (*runtimesessions.Lease, error)
				Release(context.Context, *runtimesessions.Lease) error
				Rotate(context.Context, agentmemory.Identity, string, runtimesessions.RotationMetadata) (*runtimesessions.Lease, error)
				IncrementTurn(context.Context, agentmemory.Identity, string) error
				AdoptSessionID(context.Context, agentmemory.Identity, string, string) error
				AppendAgentTurn(context.Context, runtimellm.AgentTurnRecord) error
				UpsertConversation(context.Context, runtimellm.ConversationRecord) error
				UpdateLiveSessionWatchdog(context.Context, runtimellm.ConversationWatchdogUpdate) error
				LoadActiveConversation(context.Context, agentmemory.Identity) (runtimellm.ConversationRecord, bool, error)
			}
			if fixture.postgres != nil {
				store = fixture.postgres
			} else {
				store = fixture.sqlite
			}
			if _, err := store.Acquire(ctx, identity, "worker"); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				t.Fatalf("session acquire error = %v", err)
			}
			requireForkedSourceRefusal(t, "session release", store.Release(ctx, lease))
			if _, err := store.Rotate(ctx, identity, "worker", runtimesessions.RotationMetadata{OperationID: uuid.NewString()}); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				t.Fatalf("session rotate error = %v", err)
			}
			requireForkedSourceRefusal(t, "session turn", store.IncrementTurn(ctx, identity, lease.SessionID))
			requireForkedSourceRefusal(t, "session adopt", store.AdoptSessionID(ctx, identity, "worker", uuid.NewString()))
			requireForkedSourceRefusal(t, "append agent turn", store.AppendAgentTurn(ctx, turn))
			requireForkedSourceRefusal(t, "conversation upsert", store.UpsertConversation(ctx, conversation))
			requireForkedSourceRefusal(t, "watchdog update", store.UpdateLiveSessionWatchdog(ctx, watchdog))
			if _, found, err := store.LoadActiveConversation(ctx, identity); err != nil || found {
				t.Fatalf("active conversation selector = found:%v err:%v", found, err)
			}
		})
	}
}
