package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestCanonicalEventObservationPreservesCompleteRecordAndOriginalReadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			root := eventtest.RunCreatingRootIngress(uuid.NewString(), "fixture.root", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
			if err := commitSemanticEventFixture(ctx, fixture.store, root); err != nil {
				t.Fatal(err)
			}
			event := eventtest.Child(uuid.NewString(), "fixture.canonical_read", "gateway", "fixture-task", []byte(`{"value":1}`), 1, root, events.EventEnvelope{}, root.CreatedAt().Add(time.Second))
			if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
				t.Fatal(err)
			}
			record, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
			if err != nil || !found {
				t.Fatalf("original record: found=%v err=%v", found, err)
			}
			want, err := record.Decode()
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, found, err := ReadCanonicalEventRecordForTest(ctx, fixture.store, event.ID())
			if err != nil || !found || !reflect.DeepEqual(got, want.Event()) {
				t.Fatalf("complete event changed: found=%v err=%v got=%+v want=%+v", found, err, got, want.Event())
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("canonical read escaped original native coordinator: %+v", counts)
			}
			if missing, found, err := ReadCanonicalEventRecordForTest(ctx, fixture.store, uuid.NewString()); err != nil || found || !reflect.DeepEqual(missing, events.Event{}) {
				t.Fatalf("missing event yielded evidence: found=%v err=%v event=%+v", found, err, missing)
			}
			hostile := record.Clone()
			hostile.RouteSettlement = []byte(`{"unowned":true}`)
			write := func(value []byte) {
				t.Helper()
				updated := record.Clone()
				updated.RouteSettlement = value
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					writeJointSourceReadRecord(t, ctx, tx, backend.name == "postgres", updated)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			write(hostile.RouteSettlement)
			defer write(record.RouteSettlement)
			if got, found, err := ReadCanonicalEventRecordForTest(ctx, fixture.store, event.ID()); err == nil || found || !reflect.DeepEqual(got, events.Event{}) {
				t.Fatalf("malformed canonical record retained evidence: found=%v err=%v event=%+v", found, err, got)
			}
		})
	}
}

func TestCanonicalEventObservationRefusesRawCancelledAndClosedOwnershipBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, found, err := ReadCanonicalEventRecordForTest(context.Background(), owner, uuid.NewString()); err == nil || found || !reflect.DeepEqual(got, events.Event{}) {
			t.Fatalf("unsupported owner %T supplied evidence: found=%v err=%v", owner, found, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if got, found, err := ReadCanonicalEventRecordForTest(ctx, fixture.store, uuid.NewString()); !errors.Is(err, context.Canceled) || found || !reflect.DeepEqual(got, events.Event{}) {
				t.Fatalf("cancelled read supplied evidence: found=%v err=%v", found, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, found, err := ReadCanonicalEventRecordForTest(context.Background(), fixture.store, uuid.NewString()); err == nil || found || !reflect.DeepEqual(got, events.Event{}) {
				t.Fatalf("closed read supplied evidence: found=%v err=%v", found, err)
			}
		})
	}
}
