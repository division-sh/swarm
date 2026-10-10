package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestExternalAttemptStorageUsesOriginalReadOwnerAndFailsClosedBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if evidence, err := ReadExternalAttemptStorageForTest(context.Background(), selected); err == nil || evidence != nil {
			t.Fatalf("invalid selected owner supplied evidence: %T %+v/%v", selected, evidence, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"empty", "cancelled", "closed", "unavailable"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				f, ctx := backend.open(t), testAuthorActivityContext()
				probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				switch cut {
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "closed":
					if err := f.store.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "unavailable":
					if err := renameExternalAttemptObservationTable(ctx, f.store, false); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := renameExternalAttemptObservationTable(ctx, f.store, true); err != nil {
							t.Error(err)
						}
					}()
				}
				evidence, err := ReadExternalAttemptStorageForTest(ctx, f.store)
				if cut == "empty" {
					counts := probe.Snapshot()
					if err != nil || len(evidence) != 0 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
						t.Fatalf("empty inventory escaped original read owner: %+v/%v counts=%+v", evidence, err, counts)
					}
				} else if err == nil || evidence != nil || cut == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("failed read supplied evidence: %+v/%v", evidence, err)
				}
			})
		}
	}
}

func renameExternalAttemptObservationTable(ctx context.Context, selected any, restore bool) error {
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		statement := `ALTER TABLE runtime_external_effect_attempts RENAME TO unavailable_external_attempt_observation`
		if restore {
			statement = `ALTER TABLE unavailable_external_attempt_observation RENAME TO runtime_external_effect_attempts`
		}
		_, err := tx.ExecContext(ctx, statement)
		return err
	})
}
