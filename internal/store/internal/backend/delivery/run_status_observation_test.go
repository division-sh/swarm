package delivery

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// This unadmitted schema exercises only the physical reader, not selected-store
// construction, delivery authority, runtime execution or an executable fixture.
func TestPostgresRunStatusDiagnosticKeepsAllRowsNativeFailureTextAndFailClosedScan(t *testing.T) {
	_, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE event_deliveries (run_id UUID NOT NULL, status TEXT, failure JSONB)`); err != nil {
		t.Fatal(err)
	}
	run, other := uuid.NewString(), uuid.NewString()
	if _, err := db.Exec(`INSERT INTO event_deliveries VALUES
		($1, 'in_progress', NULL), ($1, 'failed', '{"code":"native"}'),
		($1, 'failed', '{"code":"native"}'), ($1, 'delivered', NULL), ($2, 'pending', NULL)`, run, other); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	want := map[PostgresRunStatusFailureDiagnosticStorage]int{
		{Status: "in_progress"}:                           1,
		{Status: "failed", Failure: `{"code": "native"}`}: 2,
		{Status: "delivered"}:                             1,
	}
	rows, err := ReadPostgresRunStatusFailureDiagnosticsStorageTx(context.Background(), tx, run)
	if err != nil {
		t.Fatal(err)
	}
	actual := map[PostgresRunStatusFailureDiagnosticStorage]int{}
	for _, row := range rows {
		actual[row]++
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("physical diagnostic lost duplicate/terminal rows, native text or exact-run scope: got=%v want=%v", actual, want)
	}
	if _, err := tx.Exec(`INSERT INTO event_deliveries VALUES ($1, NULL, NULL)`, run); err != nil {
		t.Fatal(err)
	}
	if rows, err := ReadPostgresRunStatusFailureDiagnosticsStorageTx(context.Background(), tx, run); rows != nil || err == nil {
		t.Fatalf("scan corruption supplied provisional rows or coalesced NULL status: rows=%+v err=%v", rows, err)
	}
}
