package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestFanOutDrainDiagnosticsPreserveOriginalBoundAndNullableColumnsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			selected := fixture.store.(storeTestDurableEventBusStore)
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "fanout.drain", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			var routes []events.DeliveryRoute
			for i := 0; i < 34; i++ {
				routes = append(routes, testEntitylessNodeDeliveryRoute(mustPersistenceRootNode(fmt.Sprintf("drain_%02d", i)).Key()))
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
				t.Fatal(err)
			}
			var snapshots []runtimedelivery.Snapshot
			for _, route := range routes {
				id, err := runtimedelivery.DeliveryID(event.ID(), canonicalDeliveryFixtureRouteValue(event.RunID(), route))
				if err != nil {
					t.Fatal(err)
				}
				stored, err := selected.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				snapshots = append(snapshots, stored)
			}
			sort.Slice(snapshots, func(i, j int) bool {
				if snapshots[i].CreatedAt.Equal(snapshots[j].CreatedAt) {
					return snapshots[i].DeliveryID < snapshots[j].DeliveryID
				}
				return snapshots[i].CreatedAt.Before(snapshots[j].CreatedAt)
			})
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			rows, err := ReadFanOutDrainDeliveryStorageForTest(ctx, selected, event.RunID())
			if err != nil || len(rows) != 33 {
				t.Fatalf("original 32 rows plus truncation witness changed: rows=%d/%v", len(rows), err)
			}
			var ids []string
			for _, row := range rows {
				if !row.DeliveryID.Valid || !row.EventID.Valid || row.EventID.String != event.ID() || row.EventName.String != "fanout.drain" ||
					row.SubscriberType.String != "node" || !row.SubscriberID.Valid || row.Status.String != "pending" || row.ClaimVersion.String != "0" ||
					row.RetryCount.String != "0" || !row.UpdatedAt.Valid || row.ContinuationHandoffAt.Valid || row.AttemptClosure.Valid || row.LeaseExpiresAt.Valid || row.AttemptOutcome.Valid {
					t.Fatalf("original column/NULL rendering changed: %+v", row)
				}
				ids = append(ids, row.DeliveryID.String)
			}
			var expectedIDs []string
			for _, snapshot := range snapshots[:33] {
				expectedIDs = append(expectedIDs, snapshot.DeliveryID)
			}
			if !reflect.DeepEqual(ids, expectedIDs) {
				t.Fatalf("persisted created-at/delivery-ID ordering changed: got=%v want=%v", ids, expectedIDs)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("drain observation escaped original read: %+v", counts)
			}
			settleFanOutBarrierRouteSuccess(t, ctx, selected, event, routes[0])
			rows, err = ReadFanOutDrainDeliveryStorageForTest(ctx, selected, event.RunID())
			if err != nil || len(rows) != 33 {
				t.Fatalf("terminal delivery filtering changed: %d/%v", len(rows), err)
			}
			for _, row := range rows {
				if row.SubscriberID.String == routes[0].Recipient.ID() {
					t.Fatal("delivered row survived the original unfinished predicate")
				}
			}
			claimed, err := claimDeliveryFixture(ctx, selected, event, routes[1])
			if err != nil {
				t.Fatal(err)
			}
			rows, err = ReadFanOutDrainDeliveryStorageForTest(ctx, selected, event.RunID())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range rows {
				if row.SubscriberID.String == routes[1].Recipient.ID() {
					found = true
					if row.Status.String != "in_progress" || row.ClaimVersion.String != "1" || !row.LeaseExpiresAt.Valid || !row.AttemptClosure.Valid || row.AttemptClosure.String != "open" || row.AttemptOutcome.Valid {
						t.Fatalf("exact current attempt join changed: %+v", row)
					}
				}
			}
			if !found {
				t.Fatal("current claimed node disappeared from drain evidence")
			}
			if _, err := selected.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			if foreign, err := ReadFanOutDrainDeliveryStorageForTest(ctx, selected, uuid.NewString()); err != nil || len(foreign) != 0 {
				t.Fatalf("foreign run borrowed drain evidence: %+v/%v", foreign, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadFanOutDrainDeliveryStorageForTest(cancelled, selected, event.RunID()); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled drain retained evidence: %+v/%v", got, err)
			}
			if got, err := ReadFanOutDrainDeliveryStorageForTest(ctx, selected, "invalid"); err == nil || got != nil {
				t.Fatalf("invalid run supplied drain evidence: %+v/%v", got, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadFanOutDrainDeliveryStorageForTest(ctx, selected, event.RunID()); err == nil || got != nil {
				t.Fatalf("closed drain retained evidence: %+v/%v", got, err)
			}
		})
	}
}

func TestLifecycleProjectionPayloadObservationRetainsGlobalDuplicateLogsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			selected := fixture.store.(eventRecordContractStore)
			var expected []string
			for i := 0; i < 2; i++ {
				root := runForkSnapshotFixtureEvent(t, ctx, selected)
				payload := []byte(fmt.Sprintf(`{"log_level":"info","message":"projection %d","details":{"outbox_id":"same"}}`, i))
				event := eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, root.RunID(), "", events.EventEnvelope{}, time.Now().UTC())
				if err := commitDiagnosticRuntimeLogFixture(ctx, selected, event); err != nil {
					t.Fatal(err)
				}
			}
			physical, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			eventRows, err := DecodeSelectedForkSnapshotRowsForTest(physical["events"])
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range eventRows {
				name, err := row["event_name"].Text()
				if err != nil {
					t.Fatal(err)
				}
				if name == string(events.EventTypePlatformRuntimeLog) {
					payload, err := row["payload"].Text()
					if err != nil {
						t.Fatal(err)
					}
					expected = append(expected, payload)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			payloads, err := ReadLifecycleProjectionLogPayloadsForTest(ctx, selected)
			if err != nil || len(payloads) != 2 {
				t.Fatalf("global cross-run duplicate logs changed: %d/%v", len(payloads), err)
			}
			var got []string
			for _, payload := range payloads {
				var value map[string]any
				if err := json.Unmarshal(payload, &value); err != nil || value["details"].(map[string]any)["outbox_id"] != "same" {
					t.Fatalf("original payload details changed: %s/%v", payload, err)
				}
				got = append(got, string(payload))
			}
			sort.Strings(got)
			sort.Strings(expected)
			if !reflect.DeepEqual(expected, got) {
				t.Fatalf("native payload bytes changed: got=%q want=%q", got, expected)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("payload observation escaped original read: %+v", counts)
			}
			payloads[0][0] = '!'
			again, err := ReadLifecycleProjectionLogPayloadsForTest(ctx, selected)
			if err != nil || len(again) != 2 || again[0][0] == '!' {
				t.Fatalf("returned payload alias changed durable evidence: %q/%v", again, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadLifecycleProjectionLogPayloadsForTest(cancelled, selected); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled payloads retained evidence: %q/%v", got, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadLifecycleProjectionLogPayloadsForTest(ctx, selected); err == nil || got != nil {
				t.Fatalf("closed payload read retained evidence: %q/%v", got, err)
			}
		})
	}
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadLifecycleProjectionLogPayloadsForTest(context.Background(), selected); err == nil || got != nil {
			t.Fatalf("unselected owner %T supplied log evidence: %q/%v", selected, got, err)
		}
		if got, err := ReadFanOutDrainDeliveryStorageForTest(context.Background(), selected, uuid.NewString()); err == nil || got != nil {
			t.Fatalf("unselected owner %T supplied drain evidence: %+v/%v", selected, got, err)
		}
	}
}
