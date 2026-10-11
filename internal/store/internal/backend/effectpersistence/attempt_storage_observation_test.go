package effectpersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func TestExternalAttemptStoragePreservesPhysicalMultiplicityAndState(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "attempts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE runtime_external_effect_attempts(attempt_id TEXT,operation_id TEXT,state TEXT)`); err != nil {
		t.Fatal(err)
	}
	for _, row := range [][3]string{{"1", "operation-a", "settled"}, {"2", "operation-a", "settled"}, {"3", "operation-b", "authorized"}} {
		if _, err := db.Exec(`INSERT INTO runtime_external_effect_attempts VALUES(?,?,?)`, row[0], row[1], row[2]); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner := &EffectSQLiteOwner{backend: backend, requireCurrent: func() error { return nil }}
	got, err := owner.ReadExternalAttemptStorageForTest(context.Background())
	want := []ExternalAttemptStorage{{"operation-a", "settled"}, {"operation-a", "settled"}, {"operation-b", "authorized"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("physical attempts were filtered, deduplicated or normalized: %+v/%v", got, err)
	}
	stale := errors.New("schema is stale")
	owner.requireCurrent = func() error { return stale }
	if got, err := owner.ReadExternalAttemptStorageForTest(context.Background()); !errors.Is(err, stale) || got != nil {
		t.Fatalf("stale schema supplied evidence: %+v/%v", got, err)
	}
}

func TestSelectedCompletionCompetitionCountsPreserveJoinAndAttemptScope(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "competition.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`CREATE TABLE runtime_external_effect_operations(operation_id TEXT,selected_execution_id TEXT)`,
		`CREATE TABLE runtime_external_effect_attempts(attempt_id TEXT,operation_id TEXT,state TEXT)`,
		`CREATE TABLE runtime_effect_budget_reservations(attempt_id TEXT)`,
		`INSERT INTO runtime_external_effect_operations VALUES('op-a','selected-a'),('op-b','selected-a'),('op-c','selected-b')`,
		`INSERT INTO runtime_external_effect_attempts VALUES('attempt-a','op-a','settled'),('attempt-b','op-b','settled'),('attempt-c','op-b','authorized'),('attempt-d','op-c','settled'),('attempt-e','missing-op','settled')`,
		`INSERT INTO runtime_effect_budget_reservations VALUES('attempt-a'),('attempt-a'),('attempt-c')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	err = backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		for _, check := range []struct {
			id   string
			want int
			read func(context.Context, *sql.Tx, string) (int, error)
		}{
			{"selected-a", 2, CountSelectedExecutionSettledAttemptsStorageTx},
			{"selected-b", 1, CountSelectedExecutionSettledAttemptsStorageTx},
			{"absent", 0, CountSelectedExecutionSettledAttemptsStorageTx},
			{"attempt-a", 2, CountCompletionAttemptBudgetReservationsStorageTx},
			{"attempt-b", 0, CountCompletionAttemptBudgetReservationsStorageTx},
			{"attempt-c", 1, CountCompletionAttemptBudgetReservationsStorageTx},
		} {
			if got, err := check.read(ctx, tx, check.id); err != nil || got != check.want {
				t.Fatalf("physical competition scope changed: %s/%d/%v want%d", check.id, got, err, check.want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
