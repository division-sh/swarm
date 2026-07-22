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
	semanticEventFixtureStore
	ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error)
	ListEventsMissingPipelineReceiptForRun(context.Context, string, time.Time, int) ([]events.PersistedReplayEvent, error)
	ListEventsWithPendingDeliveriesForRun(context.Context, string, time.Time, int) ([]events.PersistedReplayEvent, error)
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
			producer := eventtest.Producer(events.EventProducerNode, "declarative-node")
			parentID := eventtest.UUID("producer-lifecycle-parent:" + eventID)
			envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
				FlowID:       "source-flow",
				FlowInstance: "source-flow/one",
				EntityID:     uuid.NewString(),
			})
			event := eventtest.PersistedChildForProducer(
				eventID,
				events.EventType("test.node_emitted"),
				producer,
				"event-owned-task",
				[]byte(`{"task_id":"payload-owned-task","text":"how are you"}`),
				3,
				runID,
				parentID,
				envelope,
				createdAt,
			)
			if err := commitSemanticParentFixture(ctx, surface, runID, parentID, createdAt.Add(-time.Microsecond)); err != nil {
				t.Fatalf("persist source parent: %v", err)
			}
			if err := commitSemanticEventFixtureWithAgents(ctx, surface, event, []string{agentID}); err != nil {
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

			for _, malformed := range []struct {
				name         string
				producerID   string
				producerType string
				mutateRecord func(*persistedEventIdentity)
			}{
				{
					name: "missing_producer_id", producerType: string(events.EventProducerNode),
					mutateRecord: func(record *persistedEventIdentity) { record.ProducedBy = "" },
				},
				{
					name: "missing_producer_type", producerID: producer.ID(),
					mutateRecord: func(record *persistedEventIdentity) { record.ProducedByType = "" },
				},
			} {
				t.Run(malformed.name, func(t *testing.T) {
					if err := setPersistedEventProducerIdentity(fixture, ctx, eventID, malformed.producerID, malformed.producerType); err == nil {
						t.Fatal("strict event schema accepted malformed producer identity")
					}
					record, found, err := loadEventProducerIdentityRecord(ctx, fixture, eventID)
					if err != nil || !found {
						t.Fatalf("load canonical event record: found=%v err=%v", found, err)
					}
					malformed.mutateRecord(&record)
					if _, err := decodeEventRecord(record); err == nil || !strings.Contains(err.Error(), "producer identity") {
						t.Fatalf("canonical decoder error = %v, want producer identity failure", err)
					}
				})
			}

			insertProducerIdentityDecisionObligation(t, fixture, ctx, eventID, runID, createdAt)
			due, err := surface.ListDueDecisionRouteObligations(ctx, createdAt.Add(time.Hour), 10)
			if err != nil {
				t.Fatalf("ListDueDecisionRouteObligations: %v", err)
			}
			assertPersistedNodeProducerEvent(t, persistedEventByID(t, due, eventID), eventID, runID, producer, true)

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
				return events.Event{}, err
			}
			for _, record := range records {
				if record.Event.ID() == eventID {
					return record.Event, nil
				}
			}
			return events.Event{}, fmt.Errorf("event %s not returned", eventID)
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
			name:         "pending_delivery_diagnostics",
			runtimeEvent: true,
			load: func() (events.Event, error) {
				page, err := surface.ListPendingAgentDeliveryDetails(ctx, PendingAgentDeliveryListOptions{AgentID: agentID, Since: since, Limit: 10})
				if err != nil {
					return events.Event{}, err
				}
				for _, detail := range page.PendingDeliveries {
					if detail.Event.ID() == eventID {
						return detail.Event, nil
					}
				}
				return events.Event{}, fmt.Errorf("event %s not returned", eventID)
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
					return events.Event{}, err
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
	return events.Event{}
}

func setPersistedEventProducerIdentity(fixture authorActivityReceiptFixture, ctx context.Context, eventID, producerID, producerType string) error {
	query := `UPDATE events SET produced_by = NULLIF(?, ''), produced_by_type = NULLIF(?, '') WHERE event_id = ?`
	args := []any{producerID, producerType, eventID}
	if fixture.dialect == runtimeauthoractivity.DialectPostgres {
		query = `UPDATE events SET produced_by = NULLIF($1, ''), produced_by_type = NULLIF($2, '') WHERE event_id = $3::uuid`
	}
	_, err := fixture.db.ExecContext(ctx, query, args...)
	return err
}

func loadEventProducerIdentityRecord(ctx context.Context, fixture authorActivityReceiptFixture, eventID string) (persistedEventIdentity, bool, error) {
	if fixture.dialect == runtimeauthoractivity.DialectPostgres {
		return loadPostgresEventIdentity(ctx, fixture.db, eventID)
	}
	return loadSQLiteEventIdentity(ctx, fixture.db, eventID)
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
	producer := eventtest.Producer(events.EventProducerNode, "declarative-node")
	parentID := eventtest.UUID("historical-replay-parent:" + sourceEventID)
	sourceEnvelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "source-flow", FlowInstance: "source-flow/one", EntityID: uuid.NewString()})
	sourceEvent := eventtest.InExecutionMode(eventtest.PersistedChildForProducer(
		sourceEventID, events.EventType("test.node_emitted"), producer, "event-owned-task",
		[]byte(`{"task_id":"payload-owned-task"}`), 2, sourceRunID, parentID, sourceEnvelope, createdAt,
	), executionmode.Mock)
	if err := commitSemanticParentFixture(ctx, surface, sourceRunID, parentID, createdAt.Add(-time.Microsecond)); err != nil {
		t.Fatalf("persist source parent: %v", err)
	}
	if err := commitSemanticEventFixtureWithAgents(ctx, surface, sourceEvent, nil); err != nil {
		t.Fatalf("persist source event: %v", err)
	}
	forkRunID := uuid.NewString()
	forkOwner := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType("test.fork_root"), "fixture", "", []byte(`{}`), 0,
		forkRunID, "", events.EventEnvelope{}, createdAt.Add(time.Minute),
	)
	if err := commitSemanticEventFixtureWithAgents(ctx, surface, forkOwner, nil); err != nil {
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
	replayedProjection, err := projectRunForkReplayEvent(loaded, runForkActivationLineage{SourceRunID: sourceRunID, ForkRunID: forkRunID}, replayedEventID, createdAt.Add(2*time.Minute))
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
	route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "replay-agent"}
	sourceProofs, err := postgresDeliveryAdapter.CommitInitial(txctx, tx, sourceEventID, sourceRunID, []events.DeliveryRoute{route})
	if err != nil || len(sourceProofs) != 1 {
		t.Fatalf("commit source replay delivery fixture: proofs=%d err=%v", len(sourceProofs), err)
	}
	forkProofs, err := postgresDeliveryAdapter.CommitInitial(txctx, tx, replayedEventID, forkRunID, []events.DeliveryRoute{route})
	if err != nil || len(forkProofs) != 1 {
		t.Fatalf("commit fork replay delivery fixture: proofs=%d err=%v", len(forkProofs), err)
	}
	sourceDeliveryID := sourceProofs[0].DeliveryID()
	forkDeliveryID := forkProofs[0].DeliveryID()
	if _, err := tx.ExecContext(txctx, `
		INSERT INTO run_fork_delivery_event_replays (
			replay_id, fork_run_id, source_run_id, source_event_id, source_delivery_id,
			fork_event_id, fork_delivery_id, subscriber_type, subscriber_id, selection_authority, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid,
			$6::uuid, $7::uuid, 'agent', 'replay-agent', $8, $9
		)
	`, uuid.NewString(), forkRunID, sourceRunID, sourceEventID, sourceDeliveryID, replayedEventID, forkDeliveryID, RunForkDeliveryEventReplayOwner, createdAt.Add(2*time.Minute)); err != nil {
		t.Fatalf("insert replay lineage fixture: %v", err)
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
