package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"github.com/google/uuid"
	"testing"
)

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
