package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestRunFanOutBarrierStatusRefusesMissingInvalidAndClosedEvidenceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			runID := uuid.NewString()
			if status, err := ReadRunFanOutBarrierStatusStorageForTest(ctx, selected, runID); !errors.Is(err, sql.ErrNoRows) || status != "" {
				t.Fatalf("missing barrier fabricated status: %q/%v", status, err)
			}
			for _, runID := range []string{"", "bad", uuid.Nil.String()} {
				if status, err := ReadRunFanOutBarrierStatusStorageForTest(ctx, selected, runID); err == nil || status != "" {
					t.Fatalf("invalid run supplied status: %q/%v", status, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if status, err := ReadRunFanOutBarrierStatusStorageForTest(cancelled, selected, runID); !errors.Is(err, context.Canceled) || status != "" {
				t.Fatalf("cancelled observation fabricated status: %q/%v", status, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if status, err := ReadRunFanOutBarrierStatusStorageForTest(ctx, selected, runID); err == nil || status != "" {
				t.Fatalf("closed owner supplied status: %q/%v", status, err)
			}
		})
	}
	for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if status, err := ReadRunFanOutBarrierStatusStorageForTest(context.Background(), owner, uuid.NewString()); err == nil || status != "" {
			t.Fatalf("raw owner %T supplied status: %q/%v", owner, status, err)
		}
	}
}
