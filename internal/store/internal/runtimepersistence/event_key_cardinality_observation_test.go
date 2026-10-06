package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestEventKeyCardinalityObservationPreservesExactPhysicalPredicateBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, at := testAuthorActivityContext(), time.Now().UTC()
			ids := [3]string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
			for index, id := range ids {
				event := eventtest.RunCreatingRootIngress(id, events.EventType("key.first"), "fixture", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at)
				if index == 1 {
					event = eventtest.DiagnosticDirect(id, events.EventTypePlatformRuntimeLog, "runtime", "", []byte(`{"log_level":"warn","message":"physical key control"}`), 0, "", "", events.EventEnvelope{}, at)
					if err := commitDiagnosticRuntimeLogFixture(ctx, fixture.store.(diagnosticRuntimeLogFixtureStore), event); err != nil {
						t.Fatal(err)
					}
				} else if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
					t.Fatal(err)
				}
			}
			const key = "exact-event-key"
			if err := SeedEventKeyCardinalityObservationForTest(ctx, fixture.store, ids, key); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range []struct {
				key  string
				want int
			}{{key, 2}, {" " + key + " ", 1}, {"missing-key", 0}} {
				if count, err := ReadEventIdempotencyCardinalityForTest(ctx, fixture.store, tc.key); err != nil || count != tc.want {
					t.Fatalf("event key %q cardinality=%d want=%d err=%v", tc.key, count, tc.want, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 3 || counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("event key observation escaped original read owner: %+v", counts)
			}
		})
	}
}

func TestEventKeyCardinalityObservationRefusesInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if count, err := ReadEventIdempotencyCardinalityForTest(context.Background(), selected, "key"); err == nil || count != 0 {
			t.Fatalf("invalid owner became successful absence: count=%d err=%v", count, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if count, err := ReadEventIdempotencyCardinalityForTest(ctx, fixture.store, "key"); !errors.Is(err, context.Canceled) || count != 0 {
				t.Fatalf("cancelled observation lost its error: count=%d err=%v", count, err)
			}
			if count, err := ReadEventIdempotencyCardinalityForTest(context.Background(), fixture.store, ""); err == nil || count != 0 {
				t.Fatalf("empty key became successful absence: count=%d err=%v", count, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if count, err := ReadEventIdempotencyCardinalityForTest(context.Background(), fixture.store, "key"); err == nil || count != 0 {
				t.Fatalf("closed owner became successful absence: count=%d err=%v", count, err)
			}
		})
	}
}
