package delivery

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// This unadmitted schema exercises the exact physical join, not delivery claim
// issuance, live session acquisition or runtime construction.
func TestPostgresLiveSessionDeliveryReadKeepsExactEventOpenVersionAndAgent(t *testing.T) {
	_, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE event_deliveries (delivery_id UUID, event_id UUID, current_attempt_version INTEGER, subscriber_type TEXT, subscriber_id TEXT, status TEXT);
CREATE TABLE event_delivery_attempts (delivery_id UUID, claim_version INTEGER, open_marker BOOLEAN, active_session_id UUID)`); err != nil {
		t.Fatal(err)
	}
	target, foreignEvent, row, foreignAgent, node := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	session, stale := uuid.NewString(), uuid.NewString()
	if _, err := db.Exec(`INSERT INTO event_deliveries VALUES
		($1, $2, 2, 'agent', 'agent-1', 'in_progress'),
		($3, $2, 2, 'agent', 'agent-2', 'pending'),
		($4, $2, 2, 'node', 'agent-1', 'failed')`, row, target, foreignAgent, node); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO event_delivery_attempts VALUES
		($1, 1, TRUE, $2), ($1, 2, FALSE, $2), ($1, 2, TRUE, $3),
		($4, 2, TRUE, $2), ($5, 2, TRUE, $2)`, row, stale, session, foreignAgent, node); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	want := PostgresAgentOneDeliverySessionStorage{Status: "in_progress", ActiveSessionID: session}
	if got, err := ReadPostgresAgentOneDeliverySessionStorageTx(context.Background(), tx, target); got != want || err != nil {
		t.Fatalf("exact current-open agent/event join changed: got=%+v want=%+v err=%v", got, want, err)
	}
	if got, err := ReadPostgresAgentOneDeliverySessionStorageTx(context.Background(), tx, foreignEvent); got != (PostgresAgentOneDeliverySessionStorage{}) || !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("foreign event supplied a session frontier: %+v err=%v", got, err)
	}
	if _, err := tx.Exec(`UPDATE event_deliveries SET status=NULL WHERE delivery_id=$1`, row); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE event_delivery_attempts SET active_session_id=NULL WHERE delivery_id=$1 AND claim_version=2 AND open_marker=TRUE`, row); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadPostgresAgentOneDeliverySessionStorageTx(context.Background(), tx, target); got != (PostgresAgentOneDeliverySessionStorage{}) || err != nil {
		t.Fatalf("physical NULL defaults changed: %+v err=%v", got, err)
	}
	if _, err := tx.Exec(`DELETE FROM event_delivery_attempts WHERE delivery_id=$1 AND claim_version=2 AND open_marker=TRUE`, row); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadPostgresAgentOneDeliverySessionStorageTx(context.Background(), tx, target); got != (PostgresAgentOneDeliverySessionStorage{}) || !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("closed/stale attempts substituted for an absent current open attempt: %+v err=%v", got, err)
	}
}
