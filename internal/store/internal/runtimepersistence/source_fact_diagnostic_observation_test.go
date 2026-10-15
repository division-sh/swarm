package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func assertSourceFactDiagnosticReadFailure(t *testing.T, ctx context.Context, owner any, runID string, cause error) {
	t.Helper()
	check := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
	if got, err := ReadLatestFanOutObligationFactStorageForTest(ctx, owner, runID); !check(err) || !reflect.DeepEqual(got, FanOutObligationHistoryStorage{}) {
		t.Fatalf("unavailable latest history supplied evidence: %+v %v", got, err)
	}
	if got, err := ReadRunRevisionFactStorageForTest(ctx, owner, runID, 1); !check(err) || !reflect.DeepEqual(got, RunRevisionFactStorage{}) {
		t.Fatalf("unavailable exact revision supplied evidence: %+v %v", got, err)
	}
	if got, err := ReadFanOutRunBlockedClaimStorageForTest(ctx, owner, runID); !check(err) || !reflect.DeepEqual(got, FanOutRunBlockedClaimStorage{}) {
		t.Fatalf("unavailable blocked claim supplied evidence: %+v %v", got, err)
	}
}

func TestSourceFactDiagnosticObserversRefuseForeignInvalidRawAndClosedOwnersBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertSourceFactDiagnosticReadFailure(t, context.Background(), owner, uuid.NewString(), nil)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			assertSourceFactDiagnosticReadFailure(t, context.Background(), f.store, uuid.NewString(), sql.ErrNoRows)
			assertSourceFactDiagnosticReadFailure(t, context.Background(), f.store, "invalid-run", nil)
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			assertSourceFactDiagnosticReadFailure(t, cancelled, f.store, uuid.NewString(), context.Canceled)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertSourceFactDiagnosticReadFailure(t, context.Background(), f.store, uuid.NewString(), nil)
		})
	}
}
