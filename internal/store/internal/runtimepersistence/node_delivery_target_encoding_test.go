package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestNodeDeliveryTargetEncodingPreservesExactRecipientAndOriginalReadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "node.target.encoding", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: uuid.NewString()}.Normalized()
			routes := []events.DeliveryRoute{
				{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("node_1")), Target: events.MustExistingEntityTarget(target)},
				{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("node_2")), Target: events.MustMaterializingEntityTarget(target)},
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, routes); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, route := range routes {
				raw, err := ReadNodeDeliveryTargetEncodingForTest(ctx, fixture.store, event.ID(), route.Recipient.ID())
				if err != nil {
					t.Fatal(err)
				}
				var got events.DeliveryTargetOwnership
				if err := json.Unmarshal([]byte(raw), &got); err != nil || got != route.Target {
					t.Fatalf("exact target=%#v,err=%v,want%#v", got, err, route.Target)
				}
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("target encoding escaped original snapshot: %+v", counts)
			}
			if raw, err := ReadNodeDeliveryTargetEncodingForTest(ctx, fixture.store, uuid.NewString(), routes[0].Recipient.ID()); !errors.Is(err, sql.ErrNoRows) || raw != "" {
				t.Fatalf("foreign event borrowed target: %q,%v", raw, err)
			}
			if raw, err := ReadNodeDeliveryTargetEncodingForTest(ctx, fixture.store, event.ID(), "foreign"); !errors.Is(err, sql.ErrNoRows) || raw != "" {
				t.Fatalf("foreign recipient borrowed target: %q,%v", raw, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if raw, err := ReadNodeDeliveryTargetEncodingForTest(cancelled, fixture.store, event.ID(), routes[0].Recipient.ID()); !errors.Is(err, context.Canceled) || raw != "" {
				t.Fatalf("cancelled target retained evidence: %q,%v", raw, err)
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
			if raw, err := ReadNodeDeliveryTargetEncodingForTest(ctx, fixture.store, event.ID(), routes[0].Recipient.ID()); err == nil || raw != "" {
				t.Fatalf("closed target retained evidence: %q,%v", raw, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if raw, err := ReadNodeDeliveryTargetEncodingForTest(context.Background(), invalid, uuid.NewString(), "node"); err == nil || raw != "" {
			t.Fatalf("missing owner retained target: %q,%v", raw, err)
		}
	}
}
