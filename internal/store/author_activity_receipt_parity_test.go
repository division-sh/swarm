package store

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type authorActivityReceiptStore interface {
	semanticEventFixtureStore
	UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, *runtimefailures.Envelope) error
}

type authorActivityReceiptFixture struct {
	store   authorActivityReceiptStore
	db      *sql.DB
	dialect runtimeauthoractivity.Dialect
	stamp   func(context.Context, string, string) [2]string
	advance func()
}

func TestAuthorActivityDuplicateTerminalReceiptIsNoOpParity(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := tt.open(t)
			ctx := testAuthorActivityContext()
			eventID := uuid.NewString()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			agentID := "normalizer"
			event := eventtest.PersistedProjection(
				eventID, events.EventType("test.delivery_receipt"), "runtime", "", []byte(`{"text":"how are you","secret":"must-not-render"}`), 0,
				runID, "", events.EventEnvelope{}, time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC),
			)
			if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, event, []string{agentID}); err != nil {
				t.Fatalf("PersistEventWithDeliveries: %v", err)
			}
			if err := fixture.store.UpsertEventReceipt(ctx, eventID, agentID, runtimemanager.ReceiptStatusProcessed, nil); err != nil {
				t.Fatalf("first UpsertEventReceipt: %v", err)
			}

			before := listAuthorActivityForReceiptParity(t, fixture, ctx)
			if len(before) != 2 || before[0].Kind != runtimeauthoractivity.KindEventEmitted || before[1].Kind != runtimeauthoractivity.KindDeliveryLifecycle || before[1].Transition != "delivered" {
				t.Fatalf("first receipt occurrences = %#v, want emitted and delivered occurrences", before)
			}
			for _, occurrence := range before {
				if occurrence.AuthorSafeSummary != "how are you" {
					t.Fatalf("%s summary = %q, want persisted safe source summary", occurrence.Kind, occurrence.AuthorSafeSummary)
				}
				if strings.Contains(occurrence.AuthorSafeSummary, "must-not-render") {
					t.Fatalf("%s summary leaked undeclared payload", occurrence.Kind)
				}
			}
			beforeStamp := fixture.stamp(ctx, eventID, agentID)
			fixture.advance()

			if err := fixture.store.UpsertEventReceipt(ctx, eventID, agentID, runtimemanager.ReceiptStatusProcessed, nil); err != nil {
				t.Fatalf("duplicate UpsertEventReceipt: %v", err)
			}
			after := listAuthorActivityForReceiptParity(t, fixture, ctx)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("duplicate receipt changed author activity:\nbefore: %#v\nafter:  %#v", before, after)
			}
			if afterStamp := fixture.stamp(ctx, eventID, agentID); afterStamp != beforeStamp {
				t.Fatalf("duplicate receipt rewrote source timestamps: before=%v after=%v", beforeStamp, afterStamp)
			}
		})
	}
}

func TestAuthoredNodeEventProducerTypeParity(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := tt.open(t)
			ctx := testAuthorActivityContext()
			eventID := uuid.NewString()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			parentID := eventtest.UUID("authored-node-parent:" + eventID)
			parent := eventtest.RootIngress(
				parentID, events.EventType("test.node_parent"), "test-ingress", "", []byte(`{}`), 0,
				runID, "", events.EventEnvelope{}, time.Date(2026, 7, 16, 2, 59, 59, 0, time.UTC),
			)
			if err := insertCanonicalEventRecordFixture(ctx, fixture.store, parent); err != nil {
				t.Fatalf("seed authored node parent: %v", err)
			}
			event := eventtest.PersistedChildForProducer(
				eventID, events.EventType("test.node_emitted"), eventtest.Producer(events.EventProducerNode, "declarative-node"), "", []byte(`{}`), 0,
				runID, parentID, events.EventEnvelope{}, time.Date(2026, 7, 16, 3, 0, 0, 0, time.UTC),
			)

			if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, event, nil); err != nil {
				t.Fatalf("PersistEventWithDeliveries: %v", err)
			}
			producedBy, producedByType := readEventProducerIdentity(t, fixture, ctx, eventID)
			if producedBy != "declarative-node" || producedByType != "node" {
				t.Fatalf("persisted producer = %q/%q, want declarative-node/node", producedBy, producedByType)
			}
			occurrences := listAuthorActivityForReceiptParity(t, fixture, ctx)
			if len(occurrences) != 1 {
				t.Fatalf("occurrences = %#v, want one emitted occurrence", occurrences)
			}
			projection := occurrences[0].Projection
			if occurrences[0].Kind != runtimeauthoractivity.KindEventEmitted || projection.ProducerID != "declarative-node" || projection.ProducerType != "node" {
				t.Fatalf("emitted occurrence = %#v, want exact declarative-node/node producer", occurrences[0])
			}
		})
	}
}

func seedAuthorActivityReceiptRun(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID string) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`
	if fixture.dialect == runtimeauthoractivity.DialectPostgres {
		query = `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`
	}
	if _, err := fixture.db.ExecContext(ctx, query, runID, time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed author activity receipt run: %v", err)
	}
}

func readEventProducerIdentity(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, eventID string) (string, string) {
	t.Helper()
	query := `SELECT COALESCE(produced_by, ''), COALESCE(produced_by_type, '') FROM events WHERE event_id = ?`
	if fixture.dialect == runtimeauthoractivity.DialectPostgres {
		query = `SELECT COALESCE(produced_by, ''), COALESCE(produced_by_type, '') FROM events WHERE event_id = $1::uuid`
	}
	var producedBy, producedByType string
	if err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&producedBy, &producedByType); err != nil {
		t.Fatalf("read event producer identity: %v", err)
	}
	return producedBy, producedByType
}

func listAuthorActivityForReceiptParity(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context) []runtimeauthoractivity.Occurrence {
	t.Helper()
	page, err := runtimeauthoractivity.List(ctx, fixture.db, fixture.dialect, runtimeauthoractivity.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("List author activity: %v", err)
	}
	return page.Occurrences
}

func openSQLiteAuthorActivityReceiptFixture(t *testing.T) authorActivityReceiptFixture {
	t.Helper()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	store.nowFn = func() time.Time { return now }
	return authorActivityReceiptFixture{
		store: store, db: store.DB, dialect: runtimeauthoractivity.DialectSQLite,
		stamp: func(ctx context.Context, eventID, agentID string) [2]string {
			return readAuthorActivityReceiptStamps(t, ctx, store.DB, `
				SELECT CAST(d.delivered_at AS TEXT), CAST(r.processed_at AS TEXT)
				FROM event_deliveries d
				JOIN event_receipts r
				  ON r.event_id = d.event_id AND r.subscriber_type = d.subscriber_type AND r.subscriber_id = d.subscriber_id
				WHERE d.event_id = ? AND d.subscriber_type = 'agent' AND d.subscriber_id = ?
			`, eventID, agentID)
		},
		advance: func() { now = now.Add(time.Hour) },
	}
}

func openPostgresAuthorActivityReceiptFixture(t *testing.T) authorActivityReceiptFixture {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := admitTestPostgresStore(t, db)
	registerTestAuthorActivityCatalog(t, store)
	return authorActivityReceiptFixture{
		store: store, db: db, dialect: runtimeauthoractivity.DialectPostgres,
		stamp: func(ctx context.Context, eventID, agentID string) [2]string {
			return readAuthorActivityReceiptStamps(t, ctx, db, `
				SELECT d.delivered_at::text, r.processed_at::text
				FROM event_deliveries d
				JOIN event_receipts r
				  ON r.event_id = d.event_id AND r.subscriber_type = d.subscriber_type AND r.subscriber_id = d.subscriber_id
				WHERE d.event_id = $1::uuid AND d.subscriber_type = 'agent' AND d.subscriber_id = $2
			`, eventID, agentID)
		},
		advance: func() {},
	}
}

func readAuthorActivityReceiptStamps(t *testing.T, ctx context.Context, db *sql.DB, query string, args ...any) [2]string {
	t.Helper()
	var stamps [2]string
	if err := db.QueryRowContext(ctx, query, args...).Scan(&stamps[0], &stamps[1]); err != nil {
		t.Fatalf("read receipt timestamps: %v", err)
	}
	return stamps
}
