package pipelinepersistence

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func sqliteRouteStatementFixture(t testing.TB, runID string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "descriptor.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	requireSQLiteStatementRuns(t, db, runID)
	return db
}
