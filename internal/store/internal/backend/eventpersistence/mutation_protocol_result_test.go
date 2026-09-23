package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	_ "modernc.org/sqlite"
)

func TestSQLiteEventMutationResultRetainsProtocolAcknowledgement(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE author_activity_order (singleton_id INTEGER PRIMARY KEY, last_sequence INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner := &EventSQLiteOwner{backend: backend, requireCurrent: func() error { return nil }}
	ctx := context.Background()
	committed := runSQLiteEventMutationResult(ctx, owner, "event result acknowledgement", false, func(context.Context, *mutationprotocol.Attempt) (string, error) {
		return "committed", nil
	})
	if value, acknowledged := committed.Value(); !acknowledged || value != "committed" || committed.Err() != nil {
		t.Fatalf("committed result = value %q acknowledged %t error %v", value, acknowledged, committed.Err())
	}
	fault := errors.New("rejected event write")
	rejected := runSQLiteEventMutationResult(ctx, owner, "event result rejection", false, func(context.Context, *mutationprotocol.Attempt) (string, error) {
		return "uncommitted", fault
	})
	if value, acknowledged := rejected.Value(); acknowledged || value != "" || !errors.Is(rejected.Err(), fault) {
		t.Fatalf("rejected result = value %q acknowledged %t error %v", value, acknowledged, rejected.Err())
	}
}
