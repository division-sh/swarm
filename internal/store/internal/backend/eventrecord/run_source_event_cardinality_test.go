package eventrecord

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	_ "modernc.org/sqlite"
)

func TestRunEventOrSourceEventCountPreservesUnionRunAndMultiplicity(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "event-scope.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE events(run_id TEXT,event_id TEXT,source_event_id TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events VALUES('run-a','event-a',NULL),('run-a','copy-a','event-a'),('run-a','both','both'),('run-a','copy-a','event-a'),('run-b','event-a','event-a'),('run-a','other','foreign')`); err != nil {
		t.Fatal(err)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		for _, scope := range []struct {
			run, event string
			want       int
		}{
			{"run-a", "event-a", 3}, {"run-b", "event-a", 1}, {"run-a", "both", 1},
			{"run-a", "foreign", 1}, {"absent", "event-a", 0}, {"run-a", "absent", 0},
		} {
			if got, err := CountRunEventOrSourceEventStorageTx(ctx, tx, scope.run, scope.event); err != nil || got != scope.want {
				t.Fatalf("event/source union changed physical scope: %+v got%d/%v", scope, got, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
