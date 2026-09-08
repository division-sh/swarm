package runtimepersistence

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestRunForkHistoricalIdentityGenericExecutionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newSnapshotOwnershipFixture(t, backend, false, false)
			plan := f.plan(t)
			if !plan.ExecutionReady {
				t.Fatalf("canonical state-only fixture is not execution ready: %#v", plan.UnsupportedBlockers)
			}
			// Ordinary committed engine mutation publishes a later valid metadata
			// fact. Only the older selected fact is corrupted, not the live row.
			f.advance(t)
			var revision, latestRevision int64
			var original, latest []byte
			if err := f.db.QueryRow(`SELECT revision,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND revision<=$3 ORDER BY revision DESC LIMIT 1`, f.runID, f.entityID, plan.ForkPoint.Revision).Scan(&revision, &original); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRow(`SELECT revision,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 ORDER BY revision DESC LIMIT 1`, f.runID, f.entityID).Scan(&latestRevision, &latest); err != nil || latestRevision <= plan.ForkPoint.Revision {
				t.Fatalf("real writer did not supersede metadata at R: latest=%d R=%d err=%v", latestRevision, plan.ForkPoint.Revision, err)
			}
			var body map[string]any
			if err := json.Unmarshal(original, &body); err != nil {
				t.Fatal(err)
			}
			body["entity_id"] = uuid.NewString()
			corrupt, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			write := func(raw []byte) {
				t.Helper()
				result, err := f.db.Exec(`UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family='entity_metadata' AND fact_key=$3 AND revision=$4`, string(raw), f.runID, f.entityID, revision)
				if err != nil {
					t.Fatal(err)
				}
				if n, err := result.RowsAffected(); err != nil || n != 1 {
					t.Fatalf("fault injection rows=%d err=%v", n, err)
				}
			}
			request := runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID}
			write(corrupt)
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
			_, err = f.store.MaterializeRunFork(f.ctx, request)
			requireForkHistoricalMetadataRefusal(t, err)
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
				t.Fatal("generic materialization refusal changed complete application rows")
			}
			write(original)
			staged, err := f.store.MaterializeRunFork(f.ctx, request)
			if err != nil || staged.ForkRunID == "" || staged.ForkRunStatus != runfork.RunForkMaterializedStatus {
				t.Fatalf("lawful generic staging: %#v, %v", staged, err)
			}
			write(corrupt)
			before = snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
			for attempt := 0; attempt < 2; attempt++ {
				activation, err := f.store.ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{ForkRunID: staged.ForkRunID, ConfirmSourceFreeze: true})
				requireForkHistoricalMetadataRefusal(t, err)
				if activation.Activated || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
					t.Fatalf("generic activation attempt %d changed lawful staged child/source rows", attempt)
				}
			}
			write(original)
			var unchangedLatest []byte
			if err := f.db.QueryRow(`SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND revision=$3`, f.runID, f.entityID, latestRevision).Scan(&unchangedLatest); err != nil || string(latest) != string(unchangedLatest) {
				t.Fatalf("latest canonical metadata changed: %v", err)
			}
			// Restoring R restores the separate, legitimate source-advanced
			// policy refusal. It must not be confused with historical corruption.
			_, err = f.store.ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{ForkRunID: staged.ForkRunID, ConfirmSourceFreeze: true})
			if _, fact, ok := runForkReplayResumeBlockerFromError(err); !ok || fact != runfork.RunForkReplayResumeFactSourceAdvanced {
				t.Fatalf("restored ordinary source-advanced policy: %v", err)
			}
		})
	}
}

func requireForkHistoricalMetadataRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "historical entity_metadata fact key") || !strings.Contains(err.Error(), "disagrees with body identity") {
		t.Fatalf("wanted exact historical metadata identity refusal, got %v", err)
	}
}

// Capture every column of every application table, including journal, revision,
// claim, binding and readiness families. Only SQLite implementation tables are
// outside the application schema.
func snapshotForkHistoricalExecutionTables(t *testing.T, db *sql.DB, postgres bool) map[string][]string {
	t.Helper()
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	if postgres {
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name`
	}
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	out := map[string][]string{}
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		out[table+"/columns"] = columns
		out[table] = []string{}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = string(raw)
				}
			}
			raw, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			out[table] = append(out[table], string(raw))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(out[table])
	}
	return out
}
