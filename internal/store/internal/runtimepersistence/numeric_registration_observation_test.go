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

func TestNumericRegistrationObservationPreservesPhysicalScopeOrderAndPayloadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			selected := fixture.store.(eventRecordContractStore)
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			sibling := runForkSnapshotFixtureEvent(t, ctx, selected)
			if rows, err := ReadNumericRegistrationStorageForTest(ctx, selected, root.RunID()); err != nil || len(rows) != 0 {
				t.Fatalf("empty physical registration cut fabricated evidence: %+v/%v", rows, err)
			}
			at := time.Now().UTC().Truncate(time.Microsecond)
			ids := []string{"ffffffff-ffff-4fff-8fff-ffffffffffff", "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"}
			insert := func(id, run, name string, when time.Time) {
				t.Helper()
				payload := []byte(`{"account_id":"same-account", "eng_roles":7, "gem_score":3.25}`)
				event := eventtest.ExistingRunRootIngressWithRoutingSource(id, events.EventType(name), "fixture", "", payload, 0, run, events.EventEnvelope{}, eventtest.RootRoutingSource(run), when)
				if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
					t.Fatal(err)
				}
			}
			insert(ids[0], root.RunID(), "portfolio/account.registered", at)
			insert(ids[1], root.RunID(), "portfolio/account.registered", at)
			insert(ids[2], root.RunID(), "portfolio/account.registered", at.Add(time.Second))
			insert(uuid.NewString(), sibling.RunID(), "portfolio/account.registered", at)
			insert(uuid.NewString(), root.RunID(), "portfolio/account.registered.other", at)
			physical, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := DecodeSelectedForkSnapshotRowsForTest(physical["events"])
			if err != nil {
				t.Fatal(err)
			}
			payloads := map[string]string{}
			for _, row := range stored {
				id, err := row["event_id"].Text()
				if err != nil {
					t.Fatal(err)
				}
				payload, err := row["payload"].Text()
				if err != nil {
					t.Fatal(err)
				}
				payloads[id] = payload
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			rows, err := ReadNumericRegistrationStorageForTest(ctx, selected, root.RunID())
			if err != nil || len(rows) != 3 {
				t.Fatalf("physical same-account rows were filtered or deduplicated: %+v/%v", rows, err)
			}
			var got []string
			for _, row := range rows {
				got = append(got, row.ID)
				if string(row.Payload) != payloads[row.ID] {
					t.Fatalf("physical payload normalized: id=%s got=%s want=%s", row.ID, row.Payload, payloads[row.ID])
				}
			}
			if !reflect.DeepEqual(got, []string{ids[1], ids[0], ids[2]}) {
				t.Fatalf("native created-at/event-ID ordering changed: %v", got)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("physical registrations escaped original read coordinator: %+v", counts)
			}
			rows[0].Payload[0] = '!'
			again, err := ReadNumericRegistrationStorageForTest(ctx, selected, root.RunID())
			if err != nil || string(again[0].Payload) != payloads[ids[1]] {
				t.Fatalf("detached payload mutation escaped: %+v/%v", again, err)
			}
			if foreign, err := ReadNumericRegistrationStorageForTest(ctx, selected, uuid.NewString()); err != nil || len(foreign) != 0 {
				t.Fatalf("foreign run borrowed registration evidence: %+v/%v", foreign, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if rows, err := ReadNumericRegistrationStorageForTest(cancelled, selected, root.RunID()); !errors.Is(err, context.Canceled) || rows != nil {
				t.Fatalf("cancelled read retained registration evidence: %+v/%v", rows, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if rows, err := ReadNumericRegistrationStorageForTest(ctx, selected, root.RunID()); err == nil || rows != nil {
				t.Fatalf("closed owner supplied registration evidence: %+v/%v", rows, err)
			}
		})
	}
}

func TestNumericRegistrationObservationRefusesRawAndInvalidOwnership(t *testing.T) {
	for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if rows, err := ReadNumericRegistrationStorageForTest(context.Background(), owner, uuid.NewString()); err == nil || rows != nil {
			t.Fatalf("foreign owner %T supplied registration evidence: %+v/%v", owner, rows, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t).store
			for _, runID := range []string{"", "bad", uuid.Nil.String()} {
				if rows, err := ReadNumericRegistrationStorageForTest(testAuthorActivityContext(), selected, runID); err == nil || rows != nil {
					t.Fatalf("invalid run supplied registration evidence: %+v/%v", rows, err)
				}
			}
		})
	}
}
