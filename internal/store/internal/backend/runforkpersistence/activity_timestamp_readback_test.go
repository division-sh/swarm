package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// This reduced reader fixture preserves the canonical driver timestamp types.
// Nullable columns deliberately permit hostile rows refused by the full schema.
func activityTimestampDatabase(t *testing.T, backend string) *sql.DB {
	t.Helper()
	var db *sql.DB
	stamp := "TEXT"
	if backend == "postgres" {
		_, db, _ = testutil.StartEmptyPostgres(t)
		stamp = "TIMESTAMPTZ"
	} else {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "activity.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE activity_attempts (
		request_event_id TEXT PRIMARY KEY, status TEXT, execution_mode TEXT,
		result_event_type TEXT, result_payload TEXT, failure TEXT, input_hash TEXT,
		started_at %s, completed_at %s, updated_at %s,
		run_id TEXT NOT NULL DEFAULT '', source_event_id TEXT, parent_event_id TEXT,
		entity_id TEXT, flow_instance TEXT, node_id TEXT NOT NULL DEFAULT '',
		handler_event_key TEXT NOT NULL DEFAULT '', activity_id TEXT NOT NULL DEFAULT '',
		tool TEXT NOT NULL DEFAULT '', effect_class TEXT NOT NULL DEFAULT '',
		attempt INTEGER NOT NULL DEFAULT 1, success_event TEXT NOT NULL DEFAULT '',
		failure_event TEXT NOT NULL DEFAULT '', result_event_id TEXT, loop_generation TEXT,
		loop_stage TEXT)`, stamp, stamp, stamp))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func readActivityTimestampEvidence(t *testing.T, db *sql.DB, id string) (runForkActivityAttemptEvidence, error) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	return loadRunForkActivityAttemptEvidence(context.Background(), tx, id)
}

func TestActivityTimestampReaderTerminalPrecisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := activityTimestampDatabase(t, backend)
			at := time.Date(2026, 9, 8, 14, 2, 3, 123456000, time.UTC)
			for _, status := range []string{"succeeded", "failed", "uncertain"} {
				for _, mode := range []string{"live", "mock"} {
					t.Run(status+"/"+mode, func(t *testing.T) {
						id := uuid.NewString()
						want := [3]time.Time{at, at.Add(time.Second + 234567*time.Microsecond), at.Add(2*time.Second + 345678*time.Microsecond)}
						failure := "null"
						resultType, resultPayload := "write.succeeded", `{"result":true}`
						if status != "succeeded" {
							resultType, resultPayload = "write.failed", `{"failure":{"code":"timestamp_reader_fixture"}}`
							class := runtimefailures.ClassDependencyUnavailable
							if status == "uncertain" {
								class = runtimefailures.ClassOutcomeUncertain
							}
							envelope, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(class, "timestamp_reader_fixture", "activity-runtime", "execute", nil))
							if !ok {
								t.Fatal("canonical failure envelope is required")
							}
							raw, err := json.Marshal(envelope)
							if err != nil {
								t.Fatal(err)
							}
							failure = string(raw)
						}
						_, err := db.Exec(`INSERT INTO activity_attempts (request_event_id,status,execution_mode,result_event_type,result_payload,failure,input_hash,started_at,completed_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,'exact-hash',$7,$8,$9)`, id, status, mode, resultType, resultPayload, failure, want[0], want[1], want[2])
						if err != nil {
							t.Fatal(err)
						}
						got, err := readActivityTimestampEvidence(t, db, id)
						if err != nil {
							t.Fatal(err)
						}
						for i, actual := range [3]time.Time{got.StartedAt, got.CompletedAt, got.UpdatedAt} {
							if !actual.Equal(want[i]) || actual.Location() != time.UTC {
								t.Errorf("timestamp[%d]=%s (%v), want exact UTC %s", i, actual.Format(time.RFC3339Nano), actual.Location(), want[i].UTC().Format(time.RFC3339Nano))
							}
						}
						if got.Status != status || string(got.ExecutionMode) != mode || got.InputHash != "exact-hash" || got.ResultEventType != resultType || string(got.ResultPayload) != resultPayload || string(got.Failure) != failure {
							t.Fatalf("terminal evidence changed: %#v", got)
						}
					})
				}
			}
		})
	}
}

func TestActivityTimestampReaderSQLiteTextAndBytesPrecision(t *testing.T) {
	db := activityTimestampDatabase(t, "sqlite")
	at := time.Date(2026, 9, 8, 12, 3, 4, 123456789, time.FixedZone("offset", -7*3600))
	for _, kind := range []string{"text", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			id := uuid.NewString()
			var raw any = at.Format(time.RFC3339Nano)
			if kind == "bytes" {
				raw = []byte(raw.(string))
			}
			if _, err := db.Exec(`INSERT INTO activity_attempts (request_event_id,status,execution_mode,result_event_type,result_payload,failure,input_hash,started_at,completed_at,updated_at) VALUES ($1,'succeeded','live','write.result','{}','null','hash',$2,$3,$4)`, id, raw, raw, raw); err != nil {
				t.Fatal(err)
			}
			got, err := readActivityTimestampEvidence(t, db, id)
			if err != nil {
				t.Fatal(err)
			}
			for _, actual := range []time.Time{got.StartedAt, got.CompletedAt, got.UpdatedAt} {
				if !actual.Equal(at) || actual.Location() != time.UTC {
					t.Fatalf("lost nanoseconds/UTC: got %s want %s", actual.Format(time.RFC3339Nano), at.UTC().Format(time.RFC3339Nano))
				}
			}
		})
	}
}

func TestActivityTimestampReaderRejectsIncompleteEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := activityTimestampDatabase(t, backend)
			for _, column := range []string{"started_at", "completed_at", "updated_at"} {
				bad := map[string]any{"null": nil, "zero": time.Time{}}
				if backend == "sqlite" {
					bad["empty"], bad["malformed"] = "", "not-a-timestamp"
				}
				for name, value := range bad {
					t.Run(column+"/"+name, func(t *testing.T) {
						id := uuid.NewString()
						at := time.Date(2026, 9, 8, 0, 0, 0, 123456000, time.UTC)
						if _, err := db.Exec(`INSERT INTO activity_attempts (request_event_id,status,execution_mode,result_event_type,result_payload,failure,input_hash,started_at,completed_at,updated_at) VALUES ($1,'succeeded','live','write.result','{}','null','hash',$2,$3,$4)`, id, at, at, at); err != nil {
							t.Fatal(err)
						}
						if _, err := db.Exec(`UPDATE activity_attempts SET `+column+`=$1 WHERE request_event_id=$2`, value, id); err != nil {
							t.Fatal(err)
						}
						if _, err := readActivityTimestampEvidence(t, db, id); err == nil || !strings.Contains(err.Error(), column) {
							t.Fatalf("timestamp rejection=%v, want exact column %s", err, column)
						}
					})
				}
			}
			for _, cell := range []struct{ name, status, mode, result string }{
				{"started", "started", "live", "write.result"},
				{"missing_result_type", "succeeded", "live", ""},
				{"invalid_mode", "succeeded", "invalid", "write.result"},
			} {
				t.Run(cell.name, func(t *testing.T) {
					id, at := uuid.NewString(), time.Now().UTC()
					if _, err := db.Exec(`INSERT INTO activity_attempts (request_event_id,status,execution_mode,result_event_type,result_payload,failure,input_hash,started_at,completed_at,updated_at) VALUES ($1,$2,$3,$4,'{}','null','hash',$5,$6,$7)`, id, cell.status, cell.mode, cell.result, at, at, at); err != nil {
						t.Fatal(err)
					}
					if _, err := readActivityTimestampEvidence(t, db, id); err == nil {
						t.Fatal("incomplete activity evidence was admitted")
					}
				})
			}
			if _, err := readActivityTimestampEvidence(t, db, uuid.NewString()); err == nil {
				t.Fatal("missing activity evidence was admitted")
			}
		})
	}
}
