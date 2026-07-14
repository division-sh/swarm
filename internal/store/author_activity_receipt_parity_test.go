package store

import (
	"context"
	"database/sql"
	"reflect"
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
	PersistEventWithDeliveries(context.Context, events.Event, []string) error
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
			ctx := context.Background()
			eventID := uuid.NewString()
			agentID := "normalizer"
			event := eventtest.PersistedProjection(
				eventID, events.EventType("test.delivery_receipt"), "runtime", "", []byte(`{}`), 0,
				"", "", events.EventEnvelope{}, time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC),
			)
			if err := fixture.store.PersistEventWithDeliveries(ctx, event, []string{agentID}); err != nil {
				t.Fatalf("PersistEventWithDeliveries: %v", err)
			}
			if err := fixture.store.UpsertEventReceipt(ctx, eventID, agentID, runtimemanager.ReceiptStatusProcessed, nil); err != nil {
				t.Fatalf("first UpsertEventReceipt: %v", err)
			}

			before := listAuthorActivityForReceiptParity(t, fixture, ctx)
			if len(before) != 1 || before[0].Kind != runtimeauthoractivity.KindDeliveryLifecycle || before[0].Transition != "delivered" {
				t.Fatalf("first receipt occurrences = %#v, want one delivered occurrence", before)
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
	store := &PostgresStore{DB: db}
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
