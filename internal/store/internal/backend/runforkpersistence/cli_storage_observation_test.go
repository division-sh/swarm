package runforkpersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func TestCLIForkStoragePredicatesRetainExactCoordinatesAndMultiplicity(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "cli-scope.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		`CREATE TABLE run_fork_selected_contract_executions(fork_run_id TEXT, source_run_id TEXT, source_event_id TEXT, fork_event_id TEXT)`,
		`CREATE TABLE run_fork_selected_contract_bindings(fork_run_id TEXT, source_run_id TEXT, fork_event_id TEXT, mode TEXT)`,
		`CREATE TABLE run_fork_delivery_event_replays(fork_run_id TEXT)`,
		`INSERT INTO run_fork_selected_contract_executions VALUES('fork','source-a','event','copy'),('fork','source-b','event','copy'),('other','source-a','event','copy'),('fork','source-a','other','copy'),('fork','source-a','event','other')`,
		`INSERT INTO run_fork_selected_contract_bindings VALUES('fork','source','copy','bundle_hash'),('other','source','copy','foreign-fork'),('fork','other','copy','foreign-source'),('fork','source','other','foreign-event')`,
		`INSERT INTO run_fork_delivery_event_replays VALUES('fork'),('fork'),('other')`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		for _, scope := range []struct {
			fork, source, event string
			want                int
		}{{"fork", "event", "copy", 2}, {"other", "event", "copy", 1}, {"fork", "other", "copy", 1}, {"fork", "event", "other", 1}, {"absent", "event", "copy", 0}} {
			if got, err := CountSelectedForkExactExecutionStorageTx(ctx, tx, scope.fork, scope.source, scope.event); err != nil || got != scope.want {
				t.Fatalf("CLI lineage scope/multiplicity changed: %+v/%d/%v", scope, got, err)
			}
		}
		if mode, err := ReadSelectedForkBindingModeStorageTx(ctx, tx, "fork", "source", "copy"); err != nil || mode != "bundle_hash" {
			t.Fatalf("CLI binding lost a coordinate: %q/%v", mode, err)
		}
		if got, err := CountSelectedForkDeliveryReplayStorageTx(ctx, tx, "fork"); err != nil || got != 2 {
			t.Fatalf("CLI replay scope/multiplicity changed: %d/%v", got, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
