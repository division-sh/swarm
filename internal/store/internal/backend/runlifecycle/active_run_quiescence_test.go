package runlifecycle

import (
	"database/sql"

	"testing"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"

	_ "modernc.org/sqlite"
)

func quiescenceTestBackend(t *testing.T, schema string) (*sql.DB, *sqlitebackend.Backend) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	return db, backend
}
