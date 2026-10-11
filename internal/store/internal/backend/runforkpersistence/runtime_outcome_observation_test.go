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
