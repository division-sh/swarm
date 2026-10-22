package runtimepersistence

import (
	"context"
	"database/sql"

	"errors"

	"testing"

	"github.com/google/uuid"
)

func TestSelectedForkLifecycleStorageDiscardsEveryPartialReadBothStores(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		storage, err := ReadSelectedForkLifecycleDiagnosticStorageForTest(ctx, owner, uuid.NewString())
		if err == nil || storage.Receipts != nil || storage.Logs != nil {
			t.Fatalf("unowned diagnostic evidence: %+v %v", storage, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"invalid-run", "cancelled", "closed", "receipt-read-failed", "log-read-failed"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, _ := selectedForkDiscardTestStore(t, backend)
				item := enqueueDiagnosticTestAgent(t, selected.(diagnosticProjectionTestStore), "selected-diagnostic")
				runID, readCtx := item.Identity.RunID, ctx
				switch cut {
				case "invalid-run":
					runID = "not-canonical"
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = cancelled
				case "closed":
					if err := selected.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "receipt-read-failed":
					if _, err := db.ExecContext(ctx, `ALTER TABLE agent_lifecycle_diagnostic_outbox RENAME TO unavailable_selected_diagnostic_receipts`); err != nil {
						t.Fatal(err)
					}
				case "log-read-failed":
					if _, err := db.ExecContext(ctx, `ALTER TABLE events RENAME TO unavailable_selected_diagnostic_logs`); err != nil {
						t.Fatal(err)
					}
				}
				storage, err := ReadSelectedForkLifecycleDiagnosticStorageForTest(readCtx, selected, runID)
				if err == nil || storage.Receipts != nil || storage.Logs != nil || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed diagnostic read retained partial evidence: %+v %v", storage, err)
				}
			})
		}
	}
}
