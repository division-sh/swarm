package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestActivityAttemptStorageUsesOriginalSnapshotAndPhysicalInventoryBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, runID, otherRun, source := testAuthorActivityContext(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, run := range []string{runID, otherRun} {
				seedAuthorActivityReceiptRun(t, fixture, ctx, run)
			}
			journal := fixture.store.(pipeline.ActivityAttemptJournal)
			at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
			var want []ActivityAttemptStorageEvidence
			for index, identity := range []struct{ run, tool, source string }{
				{runID, "slack.post_message", source},
				{runID, "telegram.send_message", source},
				{runID, "telegram.send_message", source},
				{runID, "telegram.send_message", ""},
				{otherRun, "telegram.send_message", source},
			} {
				record := pipeline.ActivityAttemptRecord{RequestEventID: uuid.NewString(), RunID: identity.run,
					ExecutionMode: executionmode.Live, SourceEventID: identity.source, ActivityID: uuid.NewString(),
					Tool: identity.tool, EffectClass: "non_idempotent_write", Attempt: 1,
					SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed", InputHash: "exact-storage-input",
					StartedAt: at.Add(time.Duration(index) * time.Minute)}
				stored, inserted, err := journal.StartActivityAttempt(ctx, record)
				if err != nil || !inserted {
					t.Fatalf("start actual journal: inserted=%t err=%v", inserted, err)
				}
				if index == 2 {
					stored.Status, stored.ResultEventID, stored.ResultEventType = pipeline.ActivityAttemptStatusSucceeded, uuid.NewString(), record.SuccessEvent
					stored.ResultPayload = map[string]any{"ok": true}
					var acknowledged bool
					stored, acknowledged, err = journal.CompleteActivityAttempt(ctx, stored)
					if err != nil || !acknowledged {
						t.Fatalf("complete actual journal: ack=%t err=%v", acknowledged, err)
					}
				}
				if identity.run == runID {
					want = append(want, ActivityAttemptStorageEvidence{stored.RequestEventID, stored.Tool, stored.SourceEventID, stored.Status})
				}
				var persistedID string
				if err := fixture.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE event_id=$1`, stored.RequestEventID).Scan(&persistedID); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("request-event absence control: id=%s err=%v; inventory must not rely on event survival", persistedID, err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadActivityAttemptStorageForTest(ctx, fixture.store, runID); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("journal rows/order/status/cardinality: %#v %v, want %#v", got, err, want)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("journal inventory bypassed original snapshot: %+v", counts)
			}
			if got, err := ReadActivityAttemptStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || len(got) != 0 {
				t.Fatalf("absent run returned journal rows: %#v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadActivityAttemptStorageForTest(cancelled, fixture.store, runID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled journal inventory returned evidence: %#v %v", got, err)
			}
			if got, err := ReadActivityAttemptStorageForTest(ctx, fixture.store, runID); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("read/cancellation changed actual journal rows: %#v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadActivityAttemptStorageForTest(ctx, fixture.store, runID); err == nil || got != nil {
				t.Fatalf("closed journal owner returned evidence: %#v %v", got, err)
			}
		})
	}
}

func TestActivityAttemptStorageRejectsForeignOwnersAndUnavailableStorage(t *testing.T) {
	ctx, runID := context.Background(), uuid.NewString()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadActivityAttemptStorageForTest(ctx, owner, runID); err == nil || got != nil {
			t.Fatalf("foreign journal owner %T returned evidence: %#v %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
				if got, err := ReadActivityAttemptStorageForTest(ctx, fixture.store, invalid); err == nil || got != nil {
					t.Fatalf("invalid journal run identity returned evidence: %#v %v", got, err)
				}
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE activity_attempts RENAME TO unavailable_activity_attempts`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_activity_attempts RENAME TO activity_attempts`)
					return err
				}); err != nil {
					t.Error(err)
				}
			}()
			if got, err := ReadActivityAttemptStorageForTest(ctx, fixture.store, runID); err == nil || got != nil {
				t.Fatalf("unavailable journal storage returned evidence: %#v %v", got, err)
			}
		})
	}
}
