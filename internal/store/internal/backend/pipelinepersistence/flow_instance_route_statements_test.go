package pipelinepersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

func sqliteRouteStatementFixture(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "descriptor.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	for _, query := range []string{
		`CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT, bundle_hash TEXT,
		 origin_kind TEXT, trigger_event_id TEXT, trigger_event_type TEXT,
		 origin_service_id TEXT, origin_generation INTEGER, forked_from_run_id TEXT,
		 forked_from_event_id TEXT, started_at TIMESTAMP)`,
		`CREATE TABLE source_artifacts (bundle_hash TEXT PRIMARY KEY, source_blob BLOB,
		 member_count INTEGER, total_bytes INTEGER, created_at TIMESTAMP)`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	runlifecyclefixture.RequireSQLite(t, context.Background(), db, runlifecyclefixture.Fixture{
		RunID: "run", Origin: runlifecyclefixture.ScenarioSetupOrigin(),
	})
	return db
}
