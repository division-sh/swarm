package llmpersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// This isolated schema is only a query witness. It admits no runtime or lease;
// the conformance consumers retain the actual managed turn and provider paths.
func TestPostgresLiveSessionStorageKeepsExactFlowLineageAndGlobalCount(t *testing.T) {
	_, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE agent_sessions (session_id UUID, run_id UUID, agent_id TEXT, status TEXT, flow_instance TEXT, memory_enabled BOOLEAN)`); err != nil {
		t.Fatal(err)
	}
	run, other, target, empty := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	if _, err := db.Exec(`INSERT INTO agent_sessions VALUES
		($1, $2, 'agent-1', 'active', 'support/inst-1', TRUE),
		($3, $2, 'agent-1', NULL, NULL, NULL),
		(NULL, $4, 'agent-1', 'active', 'support/inst-1', TRUE),
		(NULL, $2, 'agent-2', 'active', 'support/inst-1', TRUE),
		(NULL, $2, 'agent-1', 'terminated', 'support/inst-1', TRUE),
		(NULL, $2, 'agent-1', 'active', 'support/inst-2', TRUE),
		(NULL, $2, 'agent-1', 'active', 'support/inst-1', FALSE),
		(NULL, $2, 'agent-1', 'active', 'support/inst-1', NULL)`, target, run, empty, other); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	want := PostgresSessionFlowStorage{Status: "active", FlowInstance: "support/inst-1"}
	if got, err := ReadPostgresSessionFlowStorageTx(context.Background(), tx, target); got != want || err != nil {
		t.Fatalf("exact session flow tuple changed: %+v err=%v", got, err)
	}
	if got, err := ReadPostgresSessionFlowStorageTx(context.Background(), tx, empty); got != (PostgresSessionFlowStorage{}) || err != nil {
		t.Fatalf("physical session NULL defaults changed: %+v err=%v", got, err)
	}
	if got, err := ReadPostgresSessionFlowStorageTx(context.Background(), tx, uuid.NewString()); got != (PostgresSessionFlowStorage{}) || !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing session supplied flow evidence: %+v err=%v", got, err)
	}
	if count, err := CountPostgresAgentOneSupportLiveSessionsTx(context.Background(), tx, run); count != 1 || err != nil {
		t.Fatalf("exact run/agent/flow/memory/active lineage changed: %d err=%v", count, err)
	}
	if count, err := CountPostgresAgentOneSupportLiveSessionsTx(context.Background(), tx, uuid.NewString()); count != 0 || err != nil {
		t.Fatalf("empty exact-run lineage changed: %d err=%v", count, err)
	}
	if count, err := CountPostgresAgentOneSessionsTx(context.Background(), tx); count != 7 || err != nil {
		t.Fatalf("global agent count lost historical/foreign-run/flow/memory/NULL rows: %d err=%v", count, err)
	}
}
