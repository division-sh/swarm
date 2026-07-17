package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type eventProducerIdentityLifecycleStore interface {
	PersistEventWithDeliveries(context.Context, events.Event, []string) error
	ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error)
	ListEventsMissingPipelineReceiptForRun(context.Context, string, time.Time, int) ([]events.PersistedReplayEvent, error)
	ListEventsWithPendingDeliveriesForRun(context.Context, string, time.Time, int) ([]events.PersistedReplayEvent, error)
	ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error)
	ListPendingAgentDeliveryDetails(context.Context, PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error)
	ListDueDecisionRouteObligations(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error)
	LoadOperatorEvent(context.Context, string) (OperatorEventFull, error)
}

func TestEventProducerIdentityPersistenceToReadbackParity(t *testing.T) {
	for _, test := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := test.open(t)
			surface, ok := fixture.store.(eventProducerIdentityLifecycleStore)
			if !ok {
				t.Fatalf("%T does not implement event producer identity lifecycle surface", fixture.store)
			}
			ctx := testAuthorActivityContext()
			createdAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			eventID := uuid.NewString()
			agentID := "normalizer"
			producer := events.NodeProducer("declarative-node")
			envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
				FlowID:       "source-flow",
				FlowInstance: "source-flow/one",
				EntityID:     uuid.NewString(),
			})
			event := eventtest.PersistedProjectionForProducer(
				eventID,
				events.EventType("test.node_emitted"),
				producer,
				"event-owned-task",
				[]byte(`{"task_id":"payload-owned-task","text":"how are you"}`),
				3,
				runID,
				"",
				envelope,
				createdAt,
			)
			if err := surface.PersistEventWithDeliveries(ctx, event, []string{agentID}); err != nil {
				t.Fatalf("PersistEventWithDeliveries: %v", err)
			}

			readbacks := eventProducerIdentityReadbacks(surface, fixture, ctx, eventID, runID, agentID, createdAt)
			for _, readback := range readbacks {
				t.Run(readback.name, func(t *testing.T) {
					loaded, err := readback.load()
					if err != nil {
						t.Fatalf("readback: %v", err)
					}
					assertPersistedNodeProducerEvent(t, loaded, eventID, runID, producer, readback.runtimeEvent)
				})
			}

			setPersistedEventProducerIdentity(t, fixture, ctx, eventID, "", string(events.EventProducerNode))
			for _, readback := range readbacks {
				t.Run(readback.name+"_fails_closed", func(t *testing.T) {
					if _, err := readback.load(); err == nil || !strings.Contains(err.Error(), "producer identity") {
						t.Fatalf("readback error = %v, want missing producer identity failure", err)
					}
				})
			}

			setPersistedEventProducerIdentity(t, fixture, ctx, eventID, producer.ID(), string(producer.Type()))
			insertProducerIdentityDecisionObligation(t, fixture, ctx, eventID, runID, createdAt)
			due, err := surface.ListDueDecisionRouteObligations(ctx, createdAt.Add(time.Hour), 10)
			if err != nil {
				t.Fatalf("ListDueDecisionRouteObligations: %v", err)
			}
			assertPersistedNodeProducerEvent(t, persistedEventByID(t, due, eventID), eventID, runID, producer, true)

			setPersistedEventProducerIdentity(t, fixture, ctx, eventID, producer.ID(), "")
			if _, err := surface.ListDueDecisionRouteObligations(ctx, createdAt.Add(time.Hour), 10); err == nil || !strings.Contains(err.Error(), "producer identity") {
				t.Fatalf("decision-route readback error = %v, want partial producer identity failure", err)
			}
		})
	}
}

type eventProducerIdentityReadback struct {
	name         string
	runtimeEvent bool
	load         func() (events.Event, error)
}

func eventProducerIdentityReadbacks(surface eventProducerIdentityLifecycleStore, fixture authorActivityReceiptFixture, ctx context.Context, eventID, runID, agentID string, createdAt time.Time) []eventProducerIdentityReadback {
	fromReplayRecords := func(load func() ([]events.PersistedReplayEvent, error)) func() (events.Event, error) {
		return func() (events.Event, error) {
			records, err := load()
			if err != nil {
				return events.EmptyEvent(), err
			}
			for _, record := range records {
				if record.Event.ID() == eventID {
					return record.Event, nil
				}
			}
			return events.EmptyEvent(), fmt.Errorf("event %s not returned", eventID)
		}
	}
	fromEvents := func(load func() ([]events.Event, error)) func() (events.Event, error) {
		return func() (events.Event, error) {
			loaded, err := load()
			if err != nil {
				return events.EmptyEvent(), err
			}
			for _, event := range loaded {
				if event.ID() == eventID {
					return event, nil
				}
			}
			return events.EmptyEvent(), fmt.Errorf("event %s not returned", eventID)
		}
	}
	since := createdAt.Add(-time.Minute)
	return []eventProducerIdentityReadback{
		{
			name:         "global_pipeline_recovery",
			runtimeEvent: true,
			load: fromReplayRecords(func() ([]events.PersistedReplayEvent, error) {
				return surface.ListEventsMissingPipelineReceipt(ctx, since, 10)
			}),
		},
		{
			name:         "run_pipeline_recovery",
			runtimeEvent: true,
			load: fromReplayRecords(func() ([]events.PersistedReplayEvent, error) {
				return surface.ListEventsMissingPipelineReceiptForRun(ctx, runID, since, 10)
			}),
		},
		{
			name:         "pending_run_delivery_replay",
			runtimeEvent: true,
			load: fromReplayRecords(func() ([]events.PersistedReplayEvent, error) {
				return surface.ListEventsWithPendingDeliveriesForRun(ctx, runID, since, 10)
			}),
		},
		{
			name:         "subscription_replay",
			runtimeEvent: true,
			load: fromEvents(func() ([]events.Event, error) {
				return surface.ListPendingSubscribedEvents(ctx, agentID, []events.EventType{"test.node_emitted"}, since, 10)
			}),
		},
		{
			name:         "pending_delivery_diagnostics",
			runtimeEvent: true,
			load: func() (events.Event, error) {
				page, err := surface.ListPendingAgentDeliveryDetails(ctx, PendingAgentDeliveryListOptions{AgentID: agentID, Since: since, Limit: 10})
				if err != nil {
					return events.EmptyEvent(), err
				}
				for _, detail := range page.PendingDeliveries {
					if detail.Event.ID() == eventID {
						return detail.Event, nil
					}
				}
				return events.EmptyEvent(), fmt.Errorf("event %s not returned", eventID)
			},
		},
		{
			name:         "inbound_publication_readback",
			runtimeEvent: true,
			load: func() (events.Event, error) {
				if fixture.dialect == runtimeauthoractivity.DialectPostgres {
					return loadPostgresInboundPublicationEvent(ctx, fixture.db, eventID)
				}
				return loadSQLiteInboundPublicationEvent(ctx, fixture.db, eventID)
			},
		},
		{
			name:         "operator_readback",
			runtimeEvent: true,
			load: func() (events.Event, error) {
				view, err := surface.LoadOperatorEvent(ctx, eventID)
				if err != nil {
					return events.EmptyEvent(), err
				}
				return view.EventSnapshot()
			},
		},
	}
}

func assertPersistedNodeProducerEvent(t *testing.T, event events.Event, eventID, runID string, producer events.ProducerIdentity, requireRuntimeFacts bool) {
	t.Helper()
	if event.ID() != eventID || event.RunID() != runID || event.Type() != events.EventType("test.node_emitted") {
		t.Fatalf("event identity = %q/%q/%q, want %q/%q/test.node_emitted", event.ID(), event.RunID(), event.Type(), eventID, runID)
	}
	if !event.Producer().Equal(producer) || event.SourceAgent() != producer.ID() || event.ProducerType() != producer.Type() {
		t.Fatalf("producer = %q/%q, want %q/%q", event.ProducerType(), event.SourceAgent(), producer.Type(), producer.ID())
	}
	if !requireRuntimeFacts {
		return
	}
	if event.TaskID() != "event-owned-task" || event.ChainDepth() != 3 ||
		!jsonSemanticallyEqual(event.Payload(), []byte(`{"task_id":"payload-owned-task","text":"how are you"}`)) ||
		event.SourceRoute().FlowInstance != "source-flow/one" {
		t.Fatalf("event-owned facts changed: task=%q depth=%d payload=%s", event.TaskID(), event.ChainDepth(), event.Payload())
	}
}

func persistedEventByID(t *testing.T, records []events.PersistedReplayEvent, eventID string) events.Event {
	t.Helper()
	for _, record := range records {
		if record.Event.ID() == eventID {
			return record.Event
		}
	}
	t.Fatalf("event %s not returned in %#v", eventID, records)
	return events.EmptyEvent()
}

func setPersistedEventProducerIdentity(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, eventID, producerID, producerType string) {
	t.Helper()
	query := `UPDATE events SET produced_by = NULLIF(?, ''), produced_by_type = NULLIF(?, '') WHERE event_id = ?`
	args := []any{producerID, producerType, eventID}
	if fixture.dialect == runtimeauthoractivity.DialectPostgres {
		query = `UPDATE events SET produced_by = NULLIF($1, ''), produced_by_type = NULLIF($2, '') WHERE event_id = $3::uuid`
	}
	if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("set persisted producer identity: %v", err)
	}
}

func insertProducerIdentityDecisionObligation(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, eventID, runID string, at time.Time) {
	t.Helper()
	cardID := uuid.NewString()
	if fixture.dialect == runtimeauthoractivity.DialectPostgres {
		if _, err := fixture.db.ExecContext(ctx, `
			INSERT INTO decision_cards (
				card_id, run_id, anchor_kind, anchor, status, execution_mode, snapshot,
				card_content_hash, decision_schema_hash, bundle_hash, effective_cadence,
				provenance, verdict, fields, decided_by, decided_at, decision_event_id,
				created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, 'stage_gate', '{}'::jsonb, 'decided', 'live', '{}'::jsonb,
				'card-hash', 'schema-hash', 'bundle-hash', '{}'::jsonb,
				'{}'::jsonb, 'approve', '{}'::jsonb, 'test', $3, $4::uuid, $3, $3
			)
		`, cardID, runID, at.UTC(), eventID); err != nil {
			t.Fatalf("insert postgres decision card: %v", err)
		}
		if _, err := fixture.db.ExecContext(ctx, `
			INSERT INTO decision_card_route_obligations (
				event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3::uuid, 'pending', 0, $4, $4, $4)
		`, eventID, cardID, runID, at.UTC()); err != nil {
			t.Fatalf("insert postgres decision route obligation: %v", err)
		}
		return
	}
	if _, err := fixture.db.ExecContext(ctx, `
		INSERT INTO decision_cards (
			card_id, run_id, anchor_kind, anchor, status, execution_mode, snapshot,
			card_content_hash, decision_schema_hash, bundle_hash, effective_cadence,
			provenance, verdict, fields, decided_by, decided_at, decision_event_id,
			created_at, updated_at
		) VALUES (?, ?, 'stage_gate', '{}', 'decided', 'live', '{}',
			'card-hash', 'schema-hash', 'bundle-hash', '{}', '{}', 'approve', '{}',
			'test', ?, ?, ?, ?)
	`, cardID, runID, at.UTC(), eventID, at.UTC(), at.UTC()); err != nil {
		t.Fatalf("insert sqlite decision card: %v", err)
	}
	if _, err := fixture.db.ExecContext(ctx, `
		INSERT INTO decision_card_route_obligations (
			event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at
		) VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
	`, eventID, cardID, runID, at.UTC(), at.UTC(), at.UTC()); err != nil {
		t.Fatalf("insert sqlite decision route obligation: %v", err)
	}
}

func TestPostgresHistoricalReplayPreservesProducerIdentity(t *testing.T) {
	fixture := openPostgresAuthorActivityReceiptFixture(t)
	surface := fixture.store.(eventProducerIdentityLifecycleStore)
	ctx := testAuthorActivityContext()
	createdAt := time.Date(2026, 7, 14, 13, 0, 0, 0, time.UTC)
	sourceRunID := uuid.NewString()
	sourceEventID := uuid.NewString()
	producer := events.NodeProducer("declarative-node")
	sourceEnvelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "source-flow", FlowInstance: "source-flow/one", EntityID: uuid.NewString()})
	sourceEvent := events.Project(eventtest.PersistedProjectionForProducer(
		sourceEventID, events.EventType("test.node_emitted"), producer, "event-owned-task",
		[]byte(`{"task_id":"payload-owned-task"}`), 2, sourceRunID, "", sourceEnvelope, createdAt,
	), events.ProjectExecutionMode(executionmode.Mock))
	if err := surface.PersistEventWithDeliveries(ctx, sourceEvent, nil); err != nil {
		t.Fatalf("persist source event: %v", err)
	}
	forkRunID := uuid.NewString()
	forkOwner := eventtest.PersistedProjectionForProducer(
		uuid.NewString(), events.EventType("test.node_emitted"), events.PlatformProducer("runtime"), "",
		[]byte(`{}`), 0, forkRunID, "", events.EventEnvelope{}, createdAt.Add(time.Minute),
	)
	if err := surface.PersistEventWithDeliveries(ctx, forkOwner, nil); err != nil {
		t.Fatalf("persist fork run owner: %v", err)
	}

	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin historical replay transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	txctx, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		t.Fatalf("begin author activity transaction: %v", err)
	}
	loaded, err := loadRunForkReplaySourceEvent(txctx, tx, sourceRunID, sourceEventID)
	if err != nil {
		t.Fatalf("loadRunForkReplaySourceEvent: %v", err)
	}
	if !loaded.Producer().Equal(producer) {
		t.Fatalf("historical replay source producer = %q/%q, want %q/%q", loaded.ProducerType(), loaded.SourceAgent(), producer.Type(), producer.ID())
	}
	replayedEventID := uuid.NewString()
	replayedProjection, err := projectRunForkReplayEvent(loaded, forkRunID, replayedEventID, createdAt.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("projectRunForkReplayEvent: %v", err)
	}
	pg := fixture.store.(*PostgresStore)
	outcome, err := pg.appendEventSpec(txctx, tx, replayedProjection)
	if err != nil {
		t.Fatalf("append replay event: %v", err)
	}
	if outcome != runtimebus.EventAppendInserted {
		t.Fatalf("append replay event outcome = %d, want inserted", outcome)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit historical replay transaction: %v", err)
	}

	replayed, err := loadPostgresInboundPublicationEvent(ctx, fixture.db, replayedEventID)
	if err != nil {
		t.Fatalf("load replayed event: %v", err)
	}
	if !replayed.Producer().Equal(producer) {
		t.Fatalf("replayed producer = %q/%q, want %q/%q", replayed.ProducerType(), replayed.SourceAgent(), producer.Type(), producer.ID())
	}
	if replayed.TaskID() != "event-owned-task" || replayed.ExecutionMode() != executionmode.Mock || replayed.ChainDepth() != 0 || replayed.ParentEventID() != "" ||
		!jsonSemanticallyEqual(replayed.Payload(), []byte(`{"task_id":"payload-owned-task"}`)) || replayed.Envelope().Source != sourceEnvelope.Source {
		t.Fatalf("replayed complete snapshot changed: task=%q mode=%q depth=%d parent=%q payload=%s envelope=%#v", replayed.TaskID(), replayed.ExecutionMode(), replayed.ChainDepth(), replayed.ParentEventID(), replayed.Payload(), replayed.Envelope())
	}
}

var _ eventProducerIdentityLifecycleStore = (*PostgresStore)(nil)
var _ eventProducerIdentityLifecycleStore = (*SQLiteRuntimeStore)(nil)
