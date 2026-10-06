package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReceiverDeliveryStoragePreservesIndependentPhysicalOracleBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			claimed := seedDeliveryRecoveryClaim(t, fixture, ctx)
			sibling := seedDeliveryRecoveryClaim(t, fixture, ctx)
			if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			runID := claimed.Snapshot.RunID
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "receiver.inventory", "gateway", "", nil, 0, runID, events.EventEnvelope{}, time.Now().UTC())
			var routes []events.DeliveryRoute
			for _, agent := range []string{"agent-a", "agent-b"} {
				routes = append(routes, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent), AgentIdentity: mustTestAgentIdentityForRun(runID, agent, "inventory/"+agent)})
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, routes); err != nil {
				t.Fatal(err)
			}
			rows, err := fixture.db.QueryContext(ctx, `SELECT delivery_id,event_id,status,CAST(delivery_target_route AS TEXT) FROM event_deliveries WHERE run_id=$1`, runID)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]ReceiverDeliveryStorageEvidence{}
			for rows.Next() {
				var row ReceiverDeliveryStorageEvidence
				if err := rows.Scan(&row.DeliveryID, &row.EventID, &row.Status, &row.Target); err != nil {
					t.Fatal(err)
				}
				want[row.DeliveryID] = row
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadReceiverDeliveryStorageForTest(ctx, fixture.store, runID)
			if err != nil || len(got) != 3 || len(want) != 3 {
				t.Fatalf("complete receiver inventory unavailable: %#v %v", got, err)
			}
			seen, eventsSeen := map[string]ReceiverDeliveryStorageEvidence{}, map[string]int{}
			for _, row := range got {
				if _, duplicate := seen[row.DeliveryID]; duplicate {
					t.Fatal("receiver inventory repeated a delivery")
				}
				seen[row.DeliveryID] = row
				eventsSeen[row.EventID]++
			}
			if !reflect.DeepEqual(seen, want) || eventsSeen[event.ID()] != 2 || seen[claimed.Snapshot.DeliveryID].Status != "delivered" {
				t.Fatalf("physical status, target text, event multiplicity or history changed: got=%#v want=%#v", seen, want)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("receiver inventory bypassed original read owner: %+v", counts)
			}
			if got, err := ReadReceiverDeliveryStorageForTest(ctx, fixture.store, sibling.Snapshot.RunID); err != nil || len(got) != 1 || got[0].DeliveryID != sibling.Snapshot.DeliveryID {
				t.Fatalf("receiver inventory changed sibling scope: %#v %v", got, err)
			}
			if got, err := ReadReceiverDeliveryStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || got != nil {
				t.Fatalf("empty receiver inventory fabricated facts: %#v %v", got, err)
			}
			if probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatal("receiver observation mutated storage")
			}
		})
	}
}

func TestReceiverDeliveryStorageRefusesInvalidCancelledAndPartialEvidenceBothStores(t *testing.T) {
	ctx, runID := context.Background(), "abcdef01-1111-4111-8111-111111111111"
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadReceiverDeliveryStorageForTest(ctx, owner, runID); err == nil || got != nil {
			t.Fatalf("invalid owner leaked receiver evidence: %#v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			claimed := seedDeliveryRecoveryClaim(t, fixture, ctx)
			runID := claimed.Snapshot.RunID
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "receiver.refusal", "gateway", "", nil, 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{claimed.Snapshot.Route}); err != nil {
				t.Fatal(err)
			}
			before, err := ReadReceiverDeliveryStorageForTest(ctx, fixture.store, runID)
			if err != nil || len(before) != 2 {
				t.Fatalf("complete receiver history unavailable: %#v %v", before, err)
			}
			for _, invalid := range []string{"", "bad", uuid.Nil.String(), strings.ToUpper("abcdef01-1111-4111-8111-111111111111")} {
				if got, err := ReadReceiverDeliveryStorageForTest(ctx, fixture.store, invalid); err == nil || got != nil {
					t.Fatalf("invalid identity leaked receiver evidence: %#v %v", got, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadReceiverDeliveryStorageForTest(cancelled, fixture.store, runID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled receiver read leaked evidence: %#v %v", got, err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `ALTER TABLE event_deliveries RENAME TO retained_receiver_storage`); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW event_deliveries AS SELECT delivery_id,event_id,status,CASE WHEN delivery_id='%s' THEN NULL ELSE delivery_target_route END AS delivery_target_route,run_id FROM retained_receiver_storage`, before[1].DeliveryID))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			restored := false
			restore := func() error {
				if restored {
					return nil
				}
				err := runUnrevisionedEventFixtureTransactionForTest(context.WithoutCancel(ctx), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					if _, err := tx.ExecContext(ctx, `DROP VIEW event_deliveries`); err != nil {
						return err
					}
					_, err := tx.ExecContext(ctx, `ALTER TABLE retained_receiver_storage RENAME TO event_deliveries`)
					return err
				})
				if err == nil {
					restored = true
				}
				return err
			}
			defer func() {
				if err := restore(); err != nil {
					t.Error(err)
				}
			}()
			if got, err := ReadReceiverDeliveryStorageForTest(ctx, fixture.store, runID); err == nil || got != nil || !strings.Contains(strings.ToLower(err.Error()), "null") {
				t.Fatalf("NULL target defaulted or partial evidence escaped: %#v %v", got, err)
			}
			if err := restore(); err != nil {
				t.Fatal(err)
			}
			got, err := ReadReceiverDeliveryStorageForTest(ctx, fixture.store, runID)
			wantByID, gotByID := map[string]ReceiverDeliveryStorageEvidence{}, map[string]ReceiverDeliveryStorageEvidence{}
			for _, row := range before {
				wantByID[row.DeliveryID] = row
			}
			for _, row := range got {
				if _, duplicate := gotByID[row.DeliveryID]; duplicate {
					t.Fatal("failed observation repeated a receiver delivery")
				}
				gotByID[row.DeliveryID] = row
			}
			if err != nil || len(got) != len(before) || !reflect.DeepEqual(gotByID, wantByID) {
				t.Fatalf("failed observation changed receiver history: %#v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadReceiverDeliveryStorageForTest(ctx, fixture.store, runID); err == nil || got != nil {
				t.Fatalf("closed owner leaked receiver evidence: %#v %v", got, err)
			}
		})
	}
}
