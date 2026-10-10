package pipelinepersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

const postgresStatementRunID = "00000000-0000-4000-8000-000000000001"

func postgresRouteStatementFixture(t *testing.T) *sql.DB {
	t.Helper()
	_, db, _ := testutil.StartEmptyPostgres(t)
	for _, query := range []string{
		`CREATE TABLE runs (run_id UUID PRIMARY KEY, status TEXT, bundle_hash TEXT,
		 origin_kind TEXT, trigger_event_id UUID, trigger_event_type TEXT,
		 origin_service_id UUID, origin_generation BIGINT, forked_from_run_id UUID,
		 forked_from_event_id UUID, started_at TIMESTAMPTZ)`,
		`CREATE TABLE source_artifacts (bundle_hash TEXT PRIMARY KEY, source_blob BYTEA,
		 member_count INTEGER, total_bytes BIGINT, created_at TIMESTAMPTZ)`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	runlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{
		RunID: postgresStatementRunID, Origin: runlifecyclefixture.ScenarioSetupOrigin(),
	})
	return db
}
