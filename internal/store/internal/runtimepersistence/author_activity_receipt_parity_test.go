package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type authorActivityReceiptStore interface {
	semanticEventFixtureStore
	runtimedelivery.Store
}

type authorActivityReceiptFixture struct {
	store   authorActivityReceiptStore
	db      *sql.DB
	dialect authoractivityfixture.Dialect
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
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID)}
			claimed, err := claimDeliveryFixture(ctx, fixture.store, event, route)
			if err != nil {
				t.Fatalf("ClaimAgentDelivery: %v", err)
			}
			if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatalf("first SettleSuccess: %v", err)
			}

			before := listAuthorActivityForReceiptParity(t, fixture, ctx)
			if len(before) != 4 || before[0].Kind != runtimeauthoractivity.KindRunLifecycle ||
				before[1].Kind != runtimeauthoractivity.KindEventEmitted ||
				before[2].Kind != runtimeauthoractivity.KindDeliveryLifecycle || before[2].Transition != "in_progress" ||
				before[3].Kind != runtimeauthoractivity.KindDeliveryLifecycle || before[3].Transition != "delivered" {
				t.Fatalf("first receipt occurrences = %#v, want run-started, emitted, in-progress, and delivered occurrences", before)
			}
			for _, occurrence := range before[1:] {
				if occurrence.AuthorSafeSummary != "how are you" {
					t.Fatalf("%s summary = %q, want persisted safe source summary", occurrence.Kind, occurrence.AuthorSafeSummary)
				}
				if strings.Contains(occurrence.AuthorSafeSummary, "must-not-render") {
					t.Fatalf("%s summary leaked undeclared payload", occurrence.Kind)
				}
			}
			beforeStamp := fixture.stamp(ctx, eventID, agentID)
			fixture.advance()

			if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); !errors.Is(err, runtimedelivery.ErrConflict) {
				t.Fatalf("duplicate SettleSuccess error = %v, want conflict", err)
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
			parent := eventtest.RunCreatingRootIngress(
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
			var emitted []runtimeauthoractivity.Occurrence
			for _, occurrence := range occurrences {
				if occurrence.Kind == runtimeauthoractivity.KindEventEmitted {
					emitted = append(emitted, occurrence)
				}
			}
			if len(emitted) != 1 {
				t.Fatalf("occurrences = %#v, want one emitted occurrence", occurrences)
			}
			projection := emitted[0].Projection
			if projection.ProducerID != "declarative-node" || projection.ProducerType != "node" {
				t.Fatalf("emitted occurrence = %#v, want exact declarative-node/node producer", emitted[0])
			}
		})
	}
}

func seedAuthorActivityReceiptRun(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID string) {
	t.Helper()
	startedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		requireRunningPostgresRunForTest(t, ctx, fixture.db, runID, startedAt)
	} else {
		requireRunningSQLiteRunForTest(t, ctx, fixture.db, runID, startedAt)
	}
}

func readEventProducerIdentity(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, eventID string) (string, string) {
	t.Helper()
	query := `SELECT COALESCE(produced_by, ''), COALESCE(produced_by_type, '') FROM events WHERE event_id = ?`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
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
	readDialect := authoractivityadapter.DialectSQLite
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		readDialect = authoractivityadapter.DialectPostgres
	}
	page, err := authoractivityadapter.List(ctx, fixture.db, readDialect, runtimeauthoractivity.ListOptions{Limit: 10})
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
		store: store, db: store.backend.ConstructionHandle(), dialect: authoractivityfixture.DialectSQLite,
		stamp: func(ctx context.Context, eventID, agentID string) [2]string {
			return readAuthorActivityReceiptStamps(t, ctx, store.backend.ConstructionHandle(), `
				SELECT CAST(d.settled_at AS TEXT), CAST(o.settled_at AS TEXT)
				FROM event_deliveries d
				JOIN event_delivery_outcomes o ON o.delivery_id = d.delivery_id
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
		store: store, db: db, dialect: authoractivityfixture.DialectPostgres,
		stamp: func(ctx context.Context, eventID, agentID string) [2]string {
			return readAuthorActivityReceiptStamps(t, ctx, db, `
				SELECT d.settled_at::text, o.settled_at::text
				FROM event_deliveries d
				JOIN event_delivery_outcomes o ON o.delivery_id = d.delivery_id
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
