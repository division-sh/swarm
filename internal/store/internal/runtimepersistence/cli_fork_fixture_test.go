package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestNativeExistingRunRootEventKeepsEventOnlyHistoryAndOriginalWriterBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			selected := fixture.store
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			id, at := uuid.NewString(), time.Unix(1700000300, 123456000).UTC()
			envelope := events.EventEnvelope{EntityID: root.RunID(), FlowInstance: root.RunID(), Scope: events.EventScopeEntity}
			event, err := InsertNativeExistingRunRootEventForTest(ctx, selected, id, root.RunID(), "fork.cli", eventtest.Producer(events.EventProducerExternal, "test"), []byte(`{"value":"exact"}`), envelope, at)
			if err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 1 || counts.Active != 0 {
				t.Fatalf("event-only append escaped original revisioned writer: %+v", counts)
			}
			assertExactEventRecord(t, ctx, fixture, event)
			if event.ID() != id || event.RunID() != root.RunID() || !event.CreatedAt().Equal(at) || !reflect.DeepEqual(event.Envelope(), envelope) {
				t.Fatal("event-only history changed supplied identity, envelope or clock")
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"committed_replay_scopes", "event_deliveries", "event_receipts", "flow_instances", "entity_state"} {
				if !reflect.DeepEqual(before[table], after[table]) {
					t.Fatalf("event-only fixture manufactured %s", table)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &sql.DB{}, &sql.Tx{}} {
				got, err := InsertNativeExistingRunRootEventForTest(ctx, owner, uuid.NewString(), root.RunID(), "fork.cli", event.Producer(), event.Payload(), envelope, at)
				if err == nil || got.ID() != "" {
					t.Fatalf("foreign %T supplied an event: %v", owner, err)
				}
			}
			got, err := InsertNativeExistingRunRootEventForTest(cancelled, selected, uuid.NewString(), root.RunID(), "fork.cli", event.Producer(), event.Payload(), envelope, at)
			if !errors.Is(err, context.Canceled) || got.ID() != "" {
				t.Fatalf("cancelled append supplied a receipt: %v", err)
			}
			got, err = InsertNativeExistingRunRootEventForTest(ctx, selected, uuid.NewString(), uuid.NewString(), "fork.cli", event.Producer(), event.Payload(), envelope, at)
			if err == nil || got.ID() != "" {
				t.Fatalf("absent run supplied a receipt: %v", err)
			}
			again, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || !reflect.DeepEqual(after, again) {
				t.Fatalf("refused event changed history: %v", err)
			}
		})
	}
}

func TestCLIMaterializationHistoryKeepsHeaderlessRowsClocksAndIndependentCuts(t *testing.T) {
	fixture, ctx := openPostgresAuthorActivityReceiptFixture(t), testAuthorActivityContext()
	selected := fixture.store
	root := runForkSnapshotFixtureEvent(t, ctx, selected)
	entity, at := uuid.NewString(), time.Unix(1700000310, 123456000).UTC()
	probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	if err := SeedPostgresCLIMaterializationHistoryForTest(ctx, selected, root.RunID(), entity, root.ID(), at); err != nil {
		t.Fatal(err)
	}
	if counts := probe.Snapshot(); counts.Total.WriteCommits != 2 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
		t.Fatalf("CLI history collapsed independent unrevisioned commits: %+v", counts)
	}
	snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	assertSelectedSourceEntityFixtureRow(t, snapshot["entity_state"], map[string]string{
		"run_id": root.RunID(), "entity_id": entity, "flow_instance": "flow-a/1", "entity_type": "default", "name": "CLI Entity",
		"current_state": "ready", "gates": "{}", "accumulator": "{}",
	}, at, "entered_state_at", "created_at", "updated_at")
	rows, err := DecodeSelectedForkSnapshotRowsForTest(snapshot["entity_state"])
	if err != nil || len(rows) != 1 {
		t.Fatalf("CLI state rows: %d/%v", len(rows), err)
	}
	fields, err := rows[0]["fields"].Text()
	var decodedFields map[string]string
	if err != nil || json.Unmarshal([]byte(fields), &decodedFields) != nil || !reflect.DeepEqual(decodedFields, map[string]string{"name": "CLI Entity"}) || string(rows[0]["revision"].Value) != "1" {
		t.Fatalf("CLI state fields/revision changed: %s/%v", fields, err)
	}
	mutations, err := DecodeSelectedForkSnapshotRowsForTest(snapshot["entity_mutations"])
	if err != nil || len(mutations) != 2 {
		t.Fatalf("CLI mutation pair: %d/%v", len(mutations), err)
	}
	expectedMutations := map[string]string{"lifecycle_state/": `"ready"`, "authored_field/name": `"CLI Entity"`}
	for _, row := range mutations {
		for key, want := range map[string]string{"run_id": root.RunID(), "entity_id": entity, "caused_by_event": root.ID(), "writer_type": "platform", "writer_id": "cli-test", "handler_step": "seed"} {
			got, err := row[key].Text()
			if err != nil || got != want {
				t.Fatalf("CLI journal %s changed: %s/%v", key, got, err)
			}
		}
		domain, domainErr := row["domain"].Text()
		path, pathErr := row["path"].Text()
		oldValue, oldErr := row["old_value"].Text()
		newValue, newErr := row["new_value"].Text()
		want, found := expectedMutations[domain+"/"+path]
		if domainErr != nil || pathErr != nil || oldErr != nil || newErr != nil || oldValue != "null" || !found || newValue != want {
			t.Fatalf("CLI journal changed domain/path or old/new JSON: %+v", row)
		}
		delete(expectedMutations, domain+"/"+path)
		var clock time.Time
		if err := json.Unmarshal(row["created_at"].Value, &clock); err != nil || !clock.Equal(at) {
			t.Fatalf("CLI mutation clock changed: %v/%v", clock, err)
		}
	}
	if len(expectedMutations) != 0 {
		t.Fatal("CLI journal pair lost a distinct historical mutation")
	}
	if state, err := ReadCLIMaterializedEntityStateForTest(ctx, selected, root.RunID(), entity); err != nil || state != "ready" {
		t.Fatalf("non-discovery state predicate changed: %q/%v", state, err)
	}
	for _, table := range []string{"flow_instances", "flow_activation_attempts"} {
		if len(snapshot[table].Rows) != 0 {
			t.Fatalf("historical CLI state manufactured %s", table)
		}
	}
}

func TestCLIMaterializationHistoryStateFailurePreservesCommittedJournal(t *testing.T) {
	fixture, ctx := openPostgresAuthorActivityReceiptFixture(t), testAuthorActivityContext()
	selected := fixture.store
	root := runForkSnapshotFixtureEvent(t, ctx, selected)
	entity, at := uuid.NewString(), time.Unix(1700000310, 0).UTC()
	if err := SeedPostgresCLIMaterializationHistoryForTest(ctx, selected, root.RunID(), entity, root.ID(), at); err != nil {
		t.Fatal(err)
	}
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	if err := SeedPostgresCLIMaterializationHistoryForTest(ctx, selected, root.RunID(), entity, root.ID(), at.Add(time.Second)); err == nil {
		t.Fatal("duplicate state insert did not refuse")
	}
	if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.RollbackAttempts != 1 || counts.Active != 0 {
		t.Fatalf("late refusal did not retain first committed cut: %+v", counts)
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before["entity_state"], after["entity_state"]) || len(after["entity_mutations"].Rows) != len(before["entity_mutations"].Rows)+2 {
		t.Fatal("late state refusal rolled back journal or changed existing state")
	}
}
