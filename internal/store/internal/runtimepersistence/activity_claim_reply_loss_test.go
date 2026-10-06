package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestActivityClaimReplyLossUsesExactNativeCommitAndCannotRearmBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			run, sibling := uuid.NewString(), uuid.NewString()
			for _, id := range []string{run, sibling} {
				seedAuthorActivityReceiptRun(t, fixture, ctx, id)
			}
			target := claimReplyLossStartForTest(run)
			fault := errors.New("exact native claim reply lost")
			journal, err := activityClaimReplyLossFaultForTest(ctx, fixture.store, run, target.RequestEventID, fault)
			if err != nil || journal == nil || !pipeline.NewWorkflowPersistence(journal).Valid() {
				t.Fatalf("install native claim reply loss: %v", err)
			}
			lost := journal.lost.Load
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, inserted, err := journal.ClaimActivityAttemptForLoopGeneration(cancelled, target); inserted || !errors.Is(err, context.Canceled) || errors.Is(err, fault) || lost() != 0 {
				t.Fatalf("cancelled claim invented reply loss: inserted=%t err=%v lost=%d", inserted, err, lost())
			}
			other := claimReplyLossStartForTest(sibling)
			if _, inserted, err := journal.ClaimActivityAttemptForLoopGeneration(ctx, other); err != nil || !inserted || lost() != 0 {
				t.Fatalf("unselected claim lost its receipt: inserted=%t err=%v lost=%d", inserted, err, lost())
			}
			got, inserted, err := journal.ClaimActivityAttemptForLoopGeneration(ctx, target)
			if !errors.Is(err, fault) || inserted || !reflect.DeepEqual(got, pipeline.ActivityAttemptRecord{}) || lost() != 1 {
				t.Fatalf("claim reply-loss cut was not exact: got=%+v inserted=%t err=%v lost=%d", got, inserted, err, lost())
			}
			stored, found, err := fixture.store.(pipeline.ActivityAttemptJournal).LoadActivityAttempt(ctx, target.RequestEventID)
			if err != nil || !found || stored.Status != pipeline.ActivityAttemptStatusStarted || stored.RunID != run {
				t.Fatalf("lost reply had no durable native claim: %+v found=%t err=%v", stored, found, err)
			}
			if duplicate, inserted, err := journal.ClaimActivityAttemptForLoopGeneration(ctx, target); err != nil || inserted || !reflect.DeepEqual(duplicate, stored) || lost() != 1 {
				t.Fatalf("duplicate repeated reply loss or changed receipt: %+v inserted=%t err=%v lost=%d", duplicate, inserted, err, lost())
			}
			if next, count, err := ActivityClaimReplyLossPersistenceFaultForTest(ctx, fixture.store, run, target.RequestEventID, fault); err == nil || next.Configured() || count != nil {
				t.Fatal("existing receipt rearmed a historical reply loss")
			}
			want := []string{target.RequestEventID + ":started", other.RequestEventID + ":started"}
			sort.Strings(want)
			if got, err := ReadWorkflowActivityAttemptStatusesForTest(ctx, fixture.store); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("global diagnostic omitted or reordered a sibling: got=%v want=%v err=%v", got, want, err)
			}
			// Each of the three native claim calls, including the duplicate,
			// settles its original writer transaction; only two insert rows.
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 3 || counts.Active != 0 {
				t.Fatalf("claim fault escaped original writer or leaked work: %+v", counts)
			}
			if got, err := ReadWorkflowActivityAttemptStatusesForTest(cancelled, fixture.store); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled diagnostic supplied evidence: %v %v", got, err)
			}
		})
	}
}

func TestWorkflowActivityAttemptStatusDiagnosticDiscardsPartialRowsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				for _, statement := range []string{
					`ALTER TABLE activity_attempts RENAME TO saved_activity_status_diagnostic`,
					`CREATE TABLE activity_attempts (request_event_id TEXT, status TEXT)`,
					`INSERT INTO activity_attempts VALUES ('00000000-0000-4000-8000-000000000001', 'started'), ('00000000-0000-4000-8000-000000000002', NULL)`,
				} {
					if _, err := tx.ExecContext(ctx, statement); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					if _, err := tx.ExecContext(ctx, `DROP TABLE activity_attempts`); err != nil {
						return err
					}
					_, err := tx.ExecContext(ctx, `ALTER TABLE saved_activity_status_diagnostic RENAME TO activity_attempts`)
					return err
				}); err != nil {
					t.Error(err)
				}
			}()
			if got, err := ReadWorkflowActivityAttemptStatusesForTest(ctx, fixture.store); err == nil || got != nil {
				t.Fatalf("late scan failure leaked first diagnostic row: %v %v", got, err)
			}
		})
	}
}

func claimReplyLossStartForTest(run string) pipeline.ActivityAttemptRecord {
	return pipeline.ActivityAttemptRecord{
		RequestEventID: uuid.NewString(), RunID: run, ExecutionMode: executionmode.Live,
		ActivityID: uuid.NewString(), Tool: "provider_write", EffectClass: "non_idempotent_write",
		Attempt: 1, SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed", InputHash: "exact",
		StartedAt: time.Now().UTC(),
	}
}

func TestActivityClaimReplyLossAndDiagnosticRefuseInvalidAndClosedOwnersBothStores(t *testing.T) {
	ctx, fault := context.Background(), errors.New("reply loss")
	for _, invalid := range []any{nil, (*SQLiteRuntimeStore)(nil), (*PostgresStore)(nil), &SQLiteRuntimeStore{}, &PostgresStore{}, &sql.DB{}, &sql.Tx{}} {
		if value, count, err := ActivityClaimReplyLossPersistenceFaultForTest(ctx, invalid, uuid.NewString(), uuid.NewString(), fault); err == nil || value.Configured() || count != nil {
			t.Fatalf("invalid claim owner granted fault authority: %T", invalid)
		}
		if got, err := ReadWorkflowActivityAttemptStatusesForTest(ctx, invalid); err == nil || got != nil {
			t.Fatalf("invalid diagnostic owner granted evidence: %T", invalid)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			for _, key := range []string{"", "invalid", uuid.Nil.String()} {
				if value, count, err := ActivityClaimReplyLossPersistenceFaultForTest(ctx, fixture.store, key, uuid.NewString(), fault); err == nil || value.Configured() || count != nil {
					t.Fatal("invalid run accepted")
				}
				if value, count, err := ActivityClaimReplyLossPersistenceFaultForTest(ctx, fixture.store, uuid.NewString(), key, fault); err == nil || value.Configured() || count != nil {
					t.Fatal("invalid request accepted")
				}
			}
			if value, count, err := ActivityClaimReplyLossPersistenceFaultForTest(ctx, fixture.store, uuid.NewString(), uuid.NewString(), nil); err == nil || value.Configured() || count != nil {
				t.Fatal("missing fault accepted")
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if value, count, err := ActivityClaimReplyLossPersistenceFaultForTest(ctx, fixture.store, uuid.NewString(), uuid.NewString(), fault); err == nil || value.Configured() || count != nil {
				t.Fatal("closed owner granted reply-loss authority")
			}
			if got, err := ReadWorkflowActivityAttemptStatusesForTest(ctx, fixture.store); err == nil || got != nil {
				t.Fatal("closed owner granted diagnostics")
			}
		})
	}
}
