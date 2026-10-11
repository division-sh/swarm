package runforkpersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func TestClosedFirstSelectedExecutionCountPreservesGenerationStateAndFork(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "competition.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE run_fork_selected_contract_runtime_executions(fork_run_id TEXT,generation INTEGER,state TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES('fork-a',1,'closed'),('fork-a',1,'closed'),('fork-a',2,'closed'),('fork-a',1,'running'),('fork-b',1,'closed')`); err != nil {
		t.Fatal(err)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		for id, want := range map[string]int{"fork-a": 2, "fork-b": 1, "absent": 0} {
			if count, err := CountClosedFirstSelectedRuntimeExecutionStorageTx(ctx, tx, id); err != nil || count != want {
				t.Fatalf("closed-first physical scope changed: %s/%d/%v want%d", id, count, err, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedForkSourceEventLineageCountPreservesExactCoordinatesAndMultiplicity(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "lineage-scope.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE run_fork_selected_contract_executions(fork_run_id TEXT,source_run_id TEXT,source_event_id TEXT,fork_event_id TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_executions VALUES('fork-a','source-a','event-a','copy-a'),('fork-a','source-b','event-a','copy-b'),('fork-b','source-a','event-a','copy-a'),('fork-a','source-a','event-b','copy-a')`); err != nil {
		t.Fatal(err)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		for _, scope := range []struct {
			fork, event string
			want        int
		}{
			{"fork-a", "event-a", 2}, {"fork-b", "event-a", 1}, {"fork-a", "event-b", 1},
			{"absent", "event-a", 0}, {"fork-a", "absent", 0},
		} {
			if got, err := CountSelectedForkSourceEventLineageStorageTx(ctx, tx, scope.fork, scope.event); err != nil || got != scope.want {
				t.Fatalf("lineage changed physical scope: %+v got%d/%v", scope, got, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
