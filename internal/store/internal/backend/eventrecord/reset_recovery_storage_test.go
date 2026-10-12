package eventrecord

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

// This isolated, unadmitted schema proves physical query behavior only. Native
// startup/reset execution is proved by the original conformance consumers.
func TestPostgresResetRecoveryPayloadsKeepPhysicalBytesNamesOrderAndNull(t *testing.T) {
	_, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE events (event_name TEXT, payload JSONB, created_at TIMESTAMPTZ)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events VALUES
		('platform.reset', '{"old":true}', '2026-10-12T00:00:00Z'),
		('platform.reset', '{"source":"admin_cli", "b":2,"a":1}', '2026-10-12T00:00:01Z'),
		('platform.recovery_failed', '{"old":true}', '2026-10-12T00:00:00Z'),
		('platform.recovery_failed', '{"failure":{"code":"claim"}}', '2026-10-12T00:00:02Z'),
		('platform.reset.extra', '{"wrong":true}', '2026-10-12T01:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, read := range []struct {
		name string
		read func(context.Context, *sql.Tx) ([]byte, error)
		want string
	}{
		{"platform.reset", ReadPostgresResetPayloadStorageTx, `{"a": 1, "b": 2, "source": "admin_cli"}`},
		{"platform.recovery_failed", ReadPostgresRecoveryFailedPayloadStorageTx, `{"failure": {"code": "claim"}}`},
	} {
		payload, err := read.read(context.Background(), tx)
		if err != nil || string(payload) != read.want {
			t.Fatalf("%s physical bytes/name/latest scope changed: %q err=%v", read.name, payload, err)
		}
		payload[0] = '!'
		again, err := read.read(context.Background(), tx)
		if err != nil || string(again) != read.want {
			t.Fatalf("%s detached payload mutation affected owner: %q err=%v", read.name, again, err)
		}
		if _, err := tx.Exec(`INSERT INTO events VALUES ($1, NULL, '2026-10-12T02:00:00Z')`, read.name); err != nil {
			t.Fatal(err)
		}
		if payload, err := read.read(context.Background(), tx); payload != nil || err != nil {
			t.Fatalf("%s physical NULL was inferred/coalesced: %q err=%v", read.name, payload, err)
		}
		if _, err := tx.Exec(`DELETE FROM events WHERE event_name=$1`, read.name); err != nil {
			t.Fatal(err)
		}
		if payload, err := read.read(context.Background(), tx); payload != nil || !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s missing row did not refuse with no evidence: %q err=%v", read.name, payload, err)
		}
	}
}
