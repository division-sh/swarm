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

func TestSelectedAuthoredHTTPCountScopesRemainDistinctAndPhysical(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "http-evidence.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`CREATE TABLE runtime_external_effect_operations(operation_id TEXT,selected_execution_id TEXT,authority_kind TEXT,effect_kind TEXT)`,
		`CREATE TABLE runtime_external_effect_attempts(attempt_id TEXT,operation_id TEXT,adapter TEXT,capability_surface_id TEXT,state TEXT)`,
		`INSERT INTO runtime_external_effect_operations VALUES('op1','exec-a','selected_contract_fork','http_tool_target'),('op2','exec-a','normal','http_tool_target'),('op3','exec-a','selected_contract_fork','other-effect'),('op4','exec-b','selected_contract_fork','http_tool_target')`,
		`INSERT INTO runtime_external_effect_attempts VALUES('a1','op1','authored_http_tool','surface-a','settled'),('a2','op1','authored_http_tool','surface-a','settled'),('a3','op1','authored_http_tool','surface-a','authorized'),('a4','op1','authored_http_tool','surface-b','settled'),('a5','op1','other-adapter','surface-a','settled'),('a6','op2','authored_http_tool','surface-a','settled'),('a7','op3','authored_http_tool','surface-a','settled'),('a8','op4','authored_http_tool','surface-a','settled'),('a9','absent-operation','authored_http_tool','surface-a','settled')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		for _, check := range []struct {
			execution string
			want      int
			read      func(context.Context, *sql.Tx, string) (int, error)
		}{
			{"exec-a", 7, CountSelectedExecutionAttemptsStorageTx},
			{"exec-b", 1, CountSelectedExecutionAttemptsStorageTx},
			{"absent", 0, CountSelectedExecutionAttemptsStorageTx},
			{"exec-a", 5, CountSelectedAuthoredHTTPAttemptsStorageTx},
			{"exec-b", 1, CountSelectedAuthoredHTTPAttemptsStorageTx},
			{"absent", 0, CountSelectedAuthoredHTTPAttemptsStorageTx},
		} {
			if got, err := check.read(ctx, tx, check.execution); err != nil || got != check.want {
				t.Fatalf("distinct execution/http scope changed: %s/%d/%v want%d", check.execution, got, err, check.want)
			}
		}
		for _, check := range []struct {
			execution, surface string
			want               int
		}{
			{"exec-a", "surface-a", 2}, {"exec-a", "surface-b", 1},
			{"exec-b", "surface-a", 1}, {"exec-a", "absent", 0}, {"absent", "surface-a", 0},
		} {
			if got, err := CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorageTx(ctx, tx, check.execution, check.surface); err != nil || got != check.want {
				t.Fatalf("settled authority/surface scope changed: %s/%s/%d/%v want%d", check.execution, check.surface, got, err, check.want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
