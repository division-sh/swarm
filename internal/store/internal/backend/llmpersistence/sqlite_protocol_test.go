package llmpersistence

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	_ "modernc.org/sqlite"
)

func TestSQLiteSessionResetRejectsStaleSchemaBeforeMutation(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "sessions.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	stale := errors.New("selected schema is stale")
	owner := &LLMSQLiteOwner{
		backend:        backend,
		requireCurrent: func() error { return stale },
		nowFn:          time.Now,
	}
	if _, err := owner.ResetAll(runtimesessions.ResetMetadata{}); !errors.Is(err, stale) {
		t.Fatalf("reset must refuse before opening a mutation: %v", err)
	}
}
