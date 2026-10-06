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

func TestFanOutRunProgressStoragePreservesAllIntentsAndOutcomeHistoryBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			active, sibling := seedDeliveryRecoveryClaim(t, fixture, ctx), seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, sibling, 1)
			var activeMutation, siblingMutation, activeEntity, siblingEntity string
			now := time.Now().UTC().Truncate(time.Microsecond)
			// These physical rows qualify unfiltered progress evidence, not lawful
			// issuance, source selection or retry/claim eligibility.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				for _, scope := range []struct {
					runID    string
					mutation *string
					entity   *string
				}{{active.Claim.RunID(), &activeMutation, &activeEntity}, {sibling.Claim.RunID(), &siblingMutation, &siblingEntity}} {
					if err := tx.QueryRowContext(ctx, `SELECT mutation_id,entity_id FROM entity_mutations WHERE run_id=$1 ORDER BY mutation_id LIMIT 1`, scope.runID).Scan(scope.mutation, scope.entity); err != nil {
						return err
					}
					if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET source_kind='entity_field_revision',source_event_id=NULL,
						source_run_id=$2,source_entity_id=$3,source_mutation_id=$1 WHERE run_id=$2`, *scope.mutation, scope.runID, *scope.entity); err != nil {
						return err
					}
				}
				if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET cursor=2,cardinality=3,status='open' WHERE run_id=$1`, active.Claim.RunID()); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents
					(run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_run_id,source_entity_id,
					source_field,source_mutation_id,cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at)
					VALUES ($1,$2,'physical','fan_out','physical.other',$3,$4,'entity_field_revision',$1,$5,'items',$6,0,0,'closed',1,'{}',$7,$7)`,
					active.Claim.RunID(), active.Snapshot.DeliveryID, "bundle-v2:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), activeEntity, activeMutation, now); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes
					(outcome_id,run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,failure,created_at)
					VALUES ($1,$2,$3,'physical','fan_out','physical.source',2,'semantic_rejected','{"class":"internal_failure"}',$4)`,
					uuid.NewString(), active.Claim.RunID(), active.Snapshot.DeliveryID, now)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			read := func(runID string) FanOutRunProgressStorageEvidence {
				t.Helper()
				before := probe.Snapshot()
				got, err := ReadFanOutRunProgressStorageForTest(ctx, fixture.store, runID)
				if err != nil {
					t.Fatal(err)
				}
				after := probe.Snapshot()
				if after.Total.Begun != before.Total.Begun+1 || after.Total.ReadCommits != before.Total.ReadCommits+1 || after.Total.WriteCommits != before.Total.WriteCommits || after.Active != 0 {
					t.Fatalf("progress observation bypassed original read snapshot: before=%+v after=%+v", before, after)
				}
				return got
			}
			before := read(active.Claim.RunID())
			want := map[FanOutProgressIntentStorageEvidence]int{
				{Cursor: 2, Cardinality: 3, Status: "open", SourceMutationID: activeMutation}:   1,
				{Cursor: 0, Cardinality: 0, Status: "closed", SourceMutationID: activeMutation}: 1,
			}
			observed := make(map[FanOutProgressIntentStorageEvidence]int)
			for _, row := range before.Intents {
				observed[row]++
			}
			if !reflect.DeepEqual(observed, want) || before.Outcomes != 3 {
				t.Fatalf("lost multiple intents, closed history or NULL-event rejection: %+v", before)
			}
			wantSibling := FanOutRunProgressStorageEvidence{
				Intents: []FanOutProgressIntentStorageEvidence{{Cursor: 1, Cardinality: 1, Status: "closed", SourceMutationID: siblingMutation}}, Outcomes: 1,
			}
			if got := read(sibling.Claim.RunID()); !reflect.DeepEqual(got, wantSibling) {
				t.Fatalf("progress mixed sibling scopes: %+v want %+v", got, wantSibling)
			}
			if got := read(uuid.NewString()); len(got.Intents) != 0 || got.Outcomes != 0 {
				t.Fatalf("absent run fabricated progress: %+v", got)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadFanOutRunProgressStorageForTest(cancelled, fixture.store, active.Claim.RunID()); !errors.Is(err, context.Canceled) || got.Intents != nil || got.Outcomes != 0 {
				t.Fatalf("cancelled observation returned progress: %+v %v", got, err)
			}
			if got := read(active.Claim.RunID()); !reflect.DeepEqual(got, before) || probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatalf("observation changed progress history: %+v want %+v", got, before)
			}
		})
	}
}

func TestFanOutRunProgressStorageRejectsInvalidOwnersAndPartialEvidenceBothStores(t *testing.T) {
	ctx, runID := context.Background(), uuid.NewString()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadFanOutRunProgressStorageForTest(ctx, owner, runID); err == nil || got.Intents != nil || got.Outcomes != 0 {
			t.Errorf("invalid owner %T returned progress: %+v %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			active := seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			for _, invalid := range []string{"", "bad", uuid.Nil.String(), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
				if got, err := ReadFanOutRunProgressStorageForTest(ctx, fixture.store, invalid); err == nil || got.Intents != nil || got.Outcomes != 0 {
					t.Errorf("invalid identity returned progress: %+v %v", got, err)
				}
			}
			if got, err := ReadFanOutRunProgressStorageForTest(ctx, fixture.store, active.Claim.RunID()); err == nil || got.Intents != nil || got.Outcomes != 0 {
				t.Fatalf("NULL source mutation fabricated progress identity: %+v %v", got, err)
			}
			var mutation, entityID string
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if err := tx.QueryRowContext(ctx, `SELECT mutation_id,entity_id FROM entity_mutations WHERE run_id=$1 ORDER BY mutation_id LIMIT 1`, active.Claim.RunID()).Scan(&mutation, &entityID); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET source_kind='entity_field_revision',source_event_id=NULL,
					source_run_id=$2,source_entity_id=$3,source_mutation_id=$1 WHERE run_id=$2`, mutation, active.Claim.RunID(), entityID)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			before, err := ReadFanOutRunProgressStorageForTest(ctx, fixture.store, active.Claim.RunID())
			if err != nil || len(before.Intents) != 1 || before.Intents[0].SourceMutationID != mutation || before.Outcomes != 2 {
				t.Fatalf("unavailable-family checkpoint lacks nonzero progress: %+v %v", before, err)
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
			rename("fan_out_outcomes", "unavailable_fan_out_progress_storage_outcomes")
			restored := false
			defer func() {
				if !restored {
					rename("unavailable_fan_out_progress_storage_outcomes", "fan_out_outcomes")
				}
			}()
			if got, err := ReadFanOutRunProgressStorageForTest(ctx, fixture.store, active.Claim.RunID()); err == nil || got.Intents != nil || got.Outcomes != 0 {
				t.Fatalf("unavailable outcome family returned partial intents: %+v %v", got, err)
			}
			rename("unavailable_fan_out_progress_storage_outcomes", "fan_out_outcomes")
			restored = true
			if got, err := ReadFanOutRunProgressStorageForTest(ctx, fixture.store, active.Claim.RunID()); err != nil || !reflect.DeepEqual(got, before) {
				t.Fatalf("failed observation changed progress: %+v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadFanOutRunProgressStorageForTest(ctx, fixture.store, active.Claim.RunID()); err == nil || got.Intents != nil || got.Outcomes != 0 {
				t.Fatalf("closed owner returned progress: %+v %v", got, err)
			}
		})
	}
}
