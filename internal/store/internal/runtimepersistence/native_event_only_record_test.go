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
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestNativeCanonicalEventRecordKeepsEventOnlyRevisionAndExactReplayBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			id, at := uuid.NewString(), time.Unix(1700000320, 123456000).UTC()
			makeEvent := func(payload string) (events.Event, events.AdmittedEvent) {
				entity := "11111111-1111-4111-8111-111111111111"
				event := eventtest.ExistingRunRootIngressWithRoutingSource(id, "producer/inst-1/scan.requested", "producer-node", "", []byte(payload), 0, root.RunID(), events.EventEnvelope{},
					eventtest.ConcreteTemplateRoutingSource("producer", "producer/inst-1", entity), at)
				bound, err := eventfixture.BindPayload(event)
				if err != nil {
					t.Fatal(err)
				}
				admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
				if err != nil {
					t.Fatal(err)
				}
				return admitted.Event(), admitted
			}
			event, admitted := makeEvent(`{"value":"exact"}`)
			settlement := testRouteSettlement(event, nil)
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if inserted, err := InsertNativeCanonicalEventRecordForTest(ctx, fixture.store, admitted, settlement); err != nil || !inserted {
				t.Fatalf("first event-only append: inserted=%v err=%v", inserted, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 1 {
				t.Fatalf("record escaped the original revisioned writer: %+v", counts)
			}
			assertExactEventRecord(t, ctx, fixture, event)
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"committed_replay_scopes", "event_deliveries", "event_receipts", "flow_instances", "entity_state", "run_fork_selected_contract_bindings"} {
				if !reflect.DeepEqual(before[table], after[table]) {
					t.Fatalf("event-only record manufactured %s", table)
				}
			}
			if inserted, err := InsertNativeCanonicalEventRecordForTest(ctx, fixture.store, admitted, settlement); err != nil || inserted {
				t.Fatalf("exact replay did not return the existing record: inserted=%v err=%v", inserted, err)
			}
			_, conflict := makeEvent(`{"value":"changed"}`)
			if inserted, err := InsertNativeCanonicalEventRecordForTest(ctx, fixture.store, conflict, settlement); err == nil || inserted || !strings.Contains(err.Error(), "conflicts with its persisted record") {
				t.Fatalf("changed same-ID record did not refuse: inserted=%v err=%v", inserted, err)
			}
			for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
				if inserted, err := InsertNativeCanonicalEventRecordForTest(ctx, owner, admitted, settlement); err == nil || inserted {
					t.Fatalf("raw/foreign owner %T supplied persistence authority: %v", owner, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if inserted, err := InsertNativeCanonicalEventRecordForTest(cancelled, fixture.store, admitted, settlement); !errors.Is(err, context.Canceled) || inserted {
				t.Fatalf("cancelled writer supplied a receipt: inserted=%v err=%v", inserted, err)
			}
			if inserted, err := InsertNativeCanonicalEventRecordForTest(ctx, fixture.store, events.AdmittedEvent{}, settlement); err == nil || inserted {
				t.Fatalf("unadmitted record supplied a receipt: inserted=%v err=%v", inserted, err)
			}
			lineage, err := events.NewSelectedForkLineage(uuid.NewString(), root.RunID(), root.ID(), "selection:event-only-refusal", "fork-task", executionmode.Live)
			if err != nil {
				t.Fatal(err)
			}
			replay := eventtest.SelectedForkReplay(uuid.NewString(), "fork.replay", eventtest.Producer(events.EventProducerNode, "selected-node"), "fork-task", []byte(`{}`), 0, lineage, events.EventEnvelope{}, at)
			replay, err = eventfixture.BindPayload(replay)
			if err != nil {
				t.Fatal(err)
			}
			admittedReplay, err := events.AdmitForPersistence(replay, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			if inserted, err := InsertNativeCanonicalEventRecordForTest(ctx, fixture.store, admittedReplay, settlement); err == nil || inserted || !strings.Contains(err.Error(), "requires exact lineage persistence") {
				t.Fatalf("selected-fork replay bypassed its exact lineage owner: inserted=%v err=%v", inserted, err)
			}
			again, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(after, again) {
				t.Fatalf("replay or refusal changed complete history, ownership or clocks: %v", err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if inserted, err := InsertNativeCanonicalEventRecordForTest(ctx, fixture.store, admitted, settlement); err == nil || inserted {
				t.Fatalf("closed writer supplied a receipt: inserted=%v err=%v", inserted, err)
			}
		})
	}
}
