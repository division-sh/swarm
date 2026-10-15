package llmpersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Only the fixed physical tuple is exercised here; this isolated schema issues
// no lease, admits no runtime, and cannot stand in for the native reset proof.
func TestPostgresSessionTerminationStorageKeepsExactTupleAndNullDefaults(t *testing.T) {
	_, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE agent_sessions (session_id UUID, status TEXT, termination_reason TEXT, termination_detail TEXT)`); err != nil {
		t.Fatal(err)
	}
	target, sibling, empty := uuid.NewString(), uuid.NewString(), uuid.NewString()
	if _, err := db.Exec(`INSERT INTO agent_sessions VALUES
		($1, 'terminated', 'orphaned', 'admin_cli'),
		($2, 'active', 'normal', 'sibling'),
		($3, NULL, NULL, NULL)`, target, sibling, empty); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	want := PostgresSessionTerminationStorage{Status: "terminated", Reason: "orphaned", Detail: "admin_cli"}
	if row, err := ReadPostgresSessionTerminationStorageTx(context.Background(), tx, target); row != want || err != nil {
		t.Fatalf("exact session tuple changed: %+v err=%v", row, err)
	}
	if row, err := ReadPostgresSessionTerminationStorageTx(context.Background(), tx, empty); row != (PostgresSessionTerminationStorage{}) || err != nil {
		t.Fatalf("physical NULL defaults changed: %+v err=%v", row, err)
	}
	if row, err := ReadPostgresSessionTerminationStorageTx(context.Background(), tx, uuid.NewString()); row != (PostgresSessionTerminationStorage{}) || !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing session inferred success/partial evidence: %+v err=%v", row, err)
	}
}
