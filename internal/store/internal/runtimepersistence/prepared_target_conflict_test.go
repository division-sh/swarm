package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestPreparedTargetConflictUsesOriginalWriterAndReadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: uuid.NewString()}.Normalized()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "prepared.target.fixture", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			original := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("original")), Target: events.MustExistingEntityTarget(target)}
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{original}); err != nil {
				t.Fatal(err)
			}
			identity, err := original.Identity()
			if err != nil {
				t.Fatal(err)
			}
			originalKey := events.EncodeDeliveryRouteIdentity(identity)
			conflict := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("conflicting")), Target: events.MustMaterializingEntityTarget(target)}
			identity, err = conflict.Identity()
			if err != nil {
				t.Fatal(err)
			}
			conflictKey := events.EncodeDeliveryRouteIdentity(identity)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if changed, err := SetPreparedTargetConflictForTest(ctx, fixture.store, event.ID(), originalKey, conflict); err != nil || changed != 1 {
				t.Fatalf("exact conflict rows=%d,err=%v", changed, err)
			}
			before, err := ReadPreparedTargetConflictForTest(ctx, fixture.store, event.ID(), conflictKey)
			if err != nil || before.Count != 1 || before.SubscriberID != conflict.Recipient.ID() || before.RouteIdentity != conflictKey || before.TargetEncoding == "" {
				t.Fatalf("exact conflict read=%+v,err=%v", before, err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 1 || counts.Active != 0 {
				t.Fatalf("fault/read escaped original coordinator: %+v", counts)
			}
			if changed, err := SetPreparedTargetConflictForTest(ctx, fixture.store, uuid.NewString(), originalKey, conflict); err == nil || changed != 0 {
				t.Fatalf("foreign event mutated conflict: %d,%v", changed, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if changed, err := SetPreparedTargetConflictForTest(cancelled, fixture.store, event.ID(), conflictKey, original); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled fault returned mutation: %d,%v", changed, err)
			}
			if out, err := ReadPreparedTargetConflictForTest(cancelled, fixture.store, event.ID(), conflictKey); !errors.Is(err, context.Canceled) || out != (PreparedTargetConflictStorage{}) {
				t.Fatalf("cancelled read retained facts: %+v,%v", out, err)
			}
			if out, err := ReadPreparedTargetConflictForTest(ctx, fixture.store, event.ID(), originalKey); err == nil || out != (PreparedTargetConflictStorage{}) {
				t.Fatalf("old identity borrowed conflict: %+v,%v", out, err)
			}
			if changed, err := SetPreparedTargetConflictForTest(ctx, fixture.store, event.ID(), conflictKey, events.DeliveryRoute{}); err == nil || changed != 0 {
				t.Fatalf("invalid route returned mutation: %d,%v", changed, err)
			}
			after, err := ReadPreparedTargetConflictForTest(ctx, fixture.store, event.ID(), conflictKey)
			if err != nil || before != after {
				t.Fatalf("rejected faults changed physical row: %+v -> %+v,%v", before, after, err)
			}
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if changed, err := SetPreparedTargetConflictForTest(ctx, fixture.store, event.ID(), conflictKey, original); err == nil || changed != 0 {
				t.Fatalf("closed fault returned mutation: %d,%v", changed, err)
			}
			if out, err := ReadPreparedTargetConflictForTest(ctx, fixture.store, event.ID(), conflictKey); err == nil || out != (PreparedTargetConflictStorage{}) {
				t.Fatalf("closed read retained facts: %+v,%v", out, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if changed, err := SetPreparedTargetConflictForTest(context.Background(), invalid, uuid.NewString(), "original", events.DeliveryRoute{}); err == nil || changed != 0 {
			t.Fatalf("missing owner returned mutation: %d,%v", changed, err)
		}
		if out, err := ReadPreparedTargetConflictForTest(context.Background(), invalid, uuid.NewString(), "original"); err == nil || out != (PreparedTargetConflictStorage{}) {
			t.Fatalf("missing owner retained facts: %+v,%v", out, err)
		}
	}
}
