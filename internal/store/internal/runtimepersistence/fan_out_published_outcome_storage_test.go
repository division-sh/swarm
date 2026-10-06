package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestFanOutPublishedOutcomeStoragePreservesOutcomeRunOrdinalAndPhysicalPayloadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			active, sibling := seedDeliveryRecoveryClaim(t, fixture, ctx), seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, sibling, 1)
			expectedPayloads := make(map[string]string)
			now := time.Now().UTC().Truncate(time.Microsecond)
			// Physical controls preserve ordinal ties, an event from a different
			// run, the projected JSON column and a NULL-event rejection. They do
			// not simulate lawful issuance or claim publication correctness.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents
					(run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_event_id,
					source_field,cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at)
					VALUES ($1,$2,'physical','fan_out','physical.other',$3,$4,'event_payload_field',$5,'items',1,1,'closed',1,'{}',$6,$6)`,
					active.Claim.RunID(), active.Snapshot.DeliveryID, "bundle-v2:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), active.Snapshot.EventID, now); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes
					(outcome_id,run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id,created_at)
					VALUES ($1,$2,$3,'physical','fan_out','physical.other',0,'committed',$4,$5)`,
					uuid.NewString(), active.Claim.RunID(), active.Snapshot.DeliveryID, sibling.Snapshot.EventID, now); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET cardinality=3,cursor=3
					WHERE run_id=$1 AND semantic_path='physical.source'`, active.Claim.RunID()); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes
					(outcome_id,run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,failure,created_at)
					VALUES ($1,$2,$3,'physical','fan_out','physical.source',2,'semantic_rejected','{"class":"internal_failure"}',$4)`,
					uuid.NewString(), active.Claim.RunID(), active.Snapshot.DeliveryID, now); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `UPDATE events SET payload=$1 WHERE event_id=$2`,
					`{" a ": [1.0], "z": " padded "}`, sibling.Snapshot.EventID); err != nil {
					return err
				}
				rows, err := tx.QueryContext(ctx, `SELECT event_id,CAST(payload AS TEXT) FROM events WHERE run_id IN ($1,$2)`, active.Claim.RunID(), sibling.Claim.RunID())
				if err != nil {
					return err
				}
				defer rows.Close()
				for rows.Next() {
					var id, payload string
					if err := rows.Scan(&id, &payload); err != nil {
						return err
					}
					expectedPayloads[id] = payload
				}
				return rows.Err()
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			rows, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, active.Claim.RunID())
			if err != nil || len(rows) != 3 {
				t.Fatalf("outcome run/ordinal ties/NULL exclusion: %+v %v", rows, err)
			}
			seen := make(map[string]bool)
			for i, row := range rows {
				if row.Kind != "committed" || seen[row.EventID] || string(row.Payload) != expectedPayloads[row.EventID] || i > 0 && row.Ordinal < rows[i-1].Ordinal {
					t.Fatalf("physical outcome payload, identity or order changed: %+v", rows)
				}
				seen[row.EventID] = true
			}
			if rows[0].Ordinal != 0 || rows[1].Ordinal != 0 || rows[2].Ordinal != 1 || !seen[sibling.Snapshot.EventID] || !strings.Contains(expectedPayloads[sibling.Snapshot.EventID], "1.0") {
				t.Fatalf("lost duplicate ordinal, cross-run event or physical JSON lexeme: %+v", rows)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("publication observation bypassed original snapshot: %+v", counts)
			}
			if other, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, sibling.Claim.RunID()); err != nil || len(other) != 1 || other[0].EventID == sibling.Snapshot.EventID {
				t.Fatalf("event-run association replaced outcome-run association: %+v %v", other, err)
			}
			if absent, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || len(absent) != 0 {
				t.Fatalf("absent run fabricated outcomes: %+v %v", absent, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadFanOutPublishedOutcomeStorageForTest(cancelled, fixture.store, active.Claim.RunID()); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled publication evidence returned rows: %+v %v", got, err)
			}
			if after, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, active.Claim.RunID()); err != nil || !reflect.DeepEqual(after, rows) || probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatalf("observation changed publication history: %+v %v", after, err)
			}
		})
	}
}

func TestFanOutPublishedOutcomeStorageRejectsInvalidOwnersAndUnavailableEvidenceBothStores(t *testing.T) {
	ctx, runID := context.Background(), uuid.NewString()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, owner, runID); err == nil || got != nil {
			t.Errorf("invalid owner %T returned outcomes: %+v %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			active := seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			for _, invalid := range []string{"", "bad", uuid.Nil.String(), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
				if got, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, invalid); err == nil || got != nil {
					t.Errorf("invalid run returned outcomes: %+v %v", got, err)
				}
			}
			before, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, active.Claim.RunID())
			if err != nil || len(before) != 2 {
				t.Fatalf("unavailable-evidence checkpoint lacks outcomes: %+v %v", before, err)
			}
			rename := func(from, to string) {
				t.Helper()
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "ALTER TABLE "+from+" RENAME TO "+to)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			rename("events", "unavailable_fan_out_publication_storage_events")
			restored := false
			defer func() {
				if !restored {
					rename("unavailable_fan_out_publication_storage_events", "events")
				}
			}()
			if got, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, active.Claim.RunID()); err == nil || got != nil {
				t.Fatalf("unavailable event family returned partial outcomes: %+v %v", got, err)
			}
			rename("unavailable_fan_out_publication_storage_events", "events")
			restored = true
			if got, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, active.Claim.RunID()); err != nil || !reflect.DeepEqual(got, before) {
				t.Fatalf("unavailable observation changed outcomes: %+v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadFanOutPublishedOutcomeStorageForTest(ctx, fixture.store, active.Claim.RunID()); err == nil || got != nil {
				t.Fatalf("closed owner returned outcomes: %+v %v", got, err)
			}
		})
	}
}
