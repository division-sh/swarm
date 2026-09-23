package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func singleEventReaderFixture(t *testing.T) (*sqlitebackend.Backend, eventrecord.Record) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "reader.db"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	columns := []string{}
	for _, name := range strings.Fields("event_class event_id run_id event_name task_id entity_id flow_instance scope payload payload_schema_bundle_hash payload_schema_flow_id payload_schema_event_key payload_schema_digest payload_schema_class execution_mode produced_by produced_by_type source_event_id routing_source_kind routing_source_authority source_route target_route target_set route_settlement operator_reference_event_id inherited_fan_out_origin") {
		columns = append(columns, name+" TEXT")
	}
	columns = append(columns, "payload_bytes BLOB", "chain_depth INTEGER", "created_at TIMESTAMP", "handler_node TEXT", "idempotency_key TEXT", "UNIQUE(event_id)")
	for _, ddl := range []string{
		"CREATE TABLE events (" + strings.Join(columns, ",") + ")",
		"CREATE TABLE run_fork_revision_heads (run_id TEXT PRIMARY KEY, last_revision INTEGER NOT NULL DEFAULT 0, updated_at TIMESTAMP)",
		"CREATE TABLE run_fork_revisions (run_id TEXT, revision INTEGER, recorded_at TIMESTAMP)",
		"CREATE TABLE run_fork_fact_revisions (run_id TEXT, revision INTEGER, family TEXT, fact_key TEXT, fact TEXT, present BOOLEAN)",
		"CREATE TABLE run_fork_selected_contract_executions (fork_event_id TEXT, source_run_id TEXT, source_event_id TEXT, selection_authority TEXT)",
		"CREATE TABLE run_fork_delivery_event_replays (fork_event_id TEXT, source_run_id TEXT, source_event_id TEXT, selection_authority TEXT)",
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	if err := runlifecyclefixture.CreateSQLiteScenarioSchema(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "record.valid", "gateway", "task-1", []byte(`{"n":1}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
	binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: sourceartifactfixture.BundleHash, EventKey: "record.valid",
		SchemaDigest: "sha256:" + strings.Repeat("0", 64), SchemaClass: events.PayloadSchemaSchemaLess,
	})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := events.NewPayloadAdmission(event.Payload(), binding)
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, admission)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	if err := runlifecyclefixture.Materialize(context.Background(), db, runlifecyclefixture.DialectSQLite, runlifecyclefixture.Fixture{
		RunID: record.RunID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Artifact: sourceartifactfixture.Artifact(),
	}); err != nil {
		t.Fatal(err)
	}
	return b, record
}

func TestSingleEventReaderCanonicalFreshnessAndCorruption(t *testing.T) {
	b, record := singleEventReaderFixture(t)
	ctx := context.Background()
	var reader SingleEventReader
	compare := func(wantFound, wantError bool) {
		t.Helper()
		raw, rawSettlement, rawFound, rawErr := LoadAdmitted(ctx, b, record.EventID)
		got, gotSettlement, gotFound, gotErr := reader.LoadAdmitted(ctx, b, record.EventID)
		if rawFound != gotFound || !reflect.DeepEqual(raw, got) || !reflect.DeepEqual(rawSettlement, gotSettlement) || fmt.Sprint(rawErr) != fmt.Sprint(gotErr) || reflect.TypeOf(rawErr) != reflect.TypeOf(gotErr) {
			t.Fatalf("raw found=%v err=%v; prepared found=%v err=%v", rawFound, rawErr, gotFound, gotErr)
		}
		if gotFound != wantFound || (gotErr != nil) != wantError {
			t.Fatalf("found=%v err=%v want found=%v error=%v", gotFound, gotErr, wantFound, wantError)
		}
	}
	compare(false, false)
	result := mutationprotocol.RunSQLite(ctx, b, "event reader fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (bool, error) {
		return Insert(ctx, attempt, record)
	})
	if !result.Acknowledged() || result.Err() != nil {
		t.Fatal(result.Err())
	}
	if inserted, ok := result.Value(); !ok || !inserted {
		t.Fatalf("inserted=%v acknowledged=%v", inserted, ok)
	}
	var revision int64
	if err := b.QueryRowContext(ctx, "SELECT last_revision FROM run_fork_revision_heads WHERE run_id=?", record.RunID).Scan(&revision); err != nil || revision != 1 {
		t.Fatalf("event revision=%d err=%v", revision, err)
	}
	duplicate := mutationprotocol.RunSQLite(ctx, b, "duplicate event reader fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (bool, error) {
		return Insert(ctx, attempt, record)
	})
	if duplicate.Err() != nil || !duplicate.Acknowledged() {
		t.Fatalf("duplicate event: %v", duplicate.Err())
	}
	if inserted, ok := duplicate.Value(); !ok || inserted {
		t.Fatalf("duplicate inserted=%v acknowledged=%v", inserted, ok)
	}
	if err := b.QueryRowContext(ctx, "SELECT last_revision FROM run_fork_revision_heads WHERE run_id=?", record.RunID).Scan(&revision); err != nil || revision != 1 {
		t.Fatalf("duplicate event revision=%d err=%v", revision, err)
	}
	compare(true, false)
	// Caller-owned results cannot replace persisted bytes for the next read.
	admitted, _, _, err := reader.LoadAdmitted(ctx, b, record.EventID)
	if err != nil {
		t.Fatal(err)
	}
	payload := admitted.Event().Payload()
	payload[0] = '!'
	compare(true, false)
	for _, tc := range []struct {
		query        string
		args         []any
		found, fails bool
	}{
		{`UPDATE events SET payload_bytes='{"n":2}'`, nil, true, false},
		{`UPDATE events SET payload_bytes='{"n":null}'`, nil, false, true},
		{`UPDATE events SET payload_bytes='{"n":3}'`, nil, true, false},
		{`INSERT INTO run_fork_selected_contract_executions VALUES (?,?,?,?)`, []any{record.EventID, uuid.NewString(), uuid.NewString(), "fixture"}, false, true},
		{`INSERT INTO run_fork_delivery_event_replays SELECT * FROM run_fork_selected_contract_executions`, nil, false, true},
		{`DELETE FROM run_fork_selected_contract_executions`, nil, false, true},
		{`DELETE FROM run_fork_delivery_event_replays`, nil, true, false},
		{`ALTER TABLE events RENAME COLUMN payload_bytes TO missing_payload`, nil, false, true},
		{`ALTER TABLE events RENAME COLUMN missing_payload TO payload_bytes`, nil, true, false},
		{`DROP TABLE run_fork_delivery_event_replays`, nil, false, true},
		{`CREATE TABLE run_fork_delivery_event_replays (fork_event_id TEXT, source_run_id TEXT, source_event_id TEXT, selection_authority TEXT)`, nil, true, false},
		{`DELETE FROM events`, nil, false, false},
	} {
		if _, err := b.ExecContext(ctx, tc.query, tc.args...); err != nil {
			t.Fatal(err)
		}
		compare(tc.found, tc.fails)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, _, err := reader.LoadAdmitted(canceled, b, record.EventID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read: %v", err)
	}
	compare(false, false)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := reader.LoadAdmitted(ctx, b, record.EventID); err == nil {
		t.Fatal("reader reopened closed backend")
	}
}

func TestSingleEventReaderAbsentEventStillValidatesLineageSchema(t *testing.T) {
	for _, table := range []string{"run_fork_selected_contract_executions", "run_fork_delivery_event_replays"} {
		for _, corruption := range []string{"missing_table", "missing_column"} {
			t.Run(table+"/"+corruption, func(t *testing.T) {
				b, record := singleEventReaderFixture(t)
				ctx := context.Background()
				var warm SingleEventReader
				if _, _, found, err := warm.LoadAdmitted(ctx, b, record.EventID); err != nil || found {
					t.Fatalf("initial absence found=%v: %v", found, err)
				}
				query := "DROP TABLE " + table
				if corruption == "missing_column" {
					query = "ALTER TABLE " + table + " RENAME COLUMN source_event_id TO missing_source_event_id"
				}
				if _, err := b.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
				_, _, rawFound, rawErr := LoadAdmitted(ctx, b, record.EventID)
				if rawErr == nil || rawFound {
					t.Fatalf("raw absence bypassed broken schema: found=%v err=%v", rawFound, rawErr)
				}
				var cold SingleEventReader
				for _, reader := range []*SingleEventReader{&warm, &cold} {
					_, _, found, err := reader.LoadAdmitted(ctx, b, record.EventID)
					if found || err == nil || err.Error() != rawErr.Error() || reflect.TypeOf(err) != reflect.TypeOf(rawErr) {
						t.Fatalf("absent-event schema error changed: raw=%T %v prepared=%T %v found=%v", rawErr, rawErr, err, err, found)
					}
				}
			})
		}
	}
}
