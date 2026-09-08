package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type historicalContextPlanner interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
}

type historicalContextFixture struct {
	db        *sql.DB
	planner   historicalContextPlanner
	postgres  bool
	source    runForkRevisionMatrixFixture
	latestID  string
	foreignID string
	anchorID  string
	initial   runfork.RunForkPlan
	facts     []historicalContextFact
}

type historicalContextFact struct {
	family string
	key    string
	raw    string
}

// H01/H02/H03/H07/H11: real canonical projections establish all thirteen
// families. Only retained historical evidence is corrupted, never live rows.
func TestRunForkHistoricalContextOlderRevisionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newHistoricalContextFixture(t, backend)
			for _, fact := range f.facts {
				t.Run(fact.family, func(t *testing.T) {
					for _, attack := range []string{"wrapper_key", "embedded_id", "missing_id", "malformed_id", "alias"} {
						if fact.family == "reply_contexts" && attack == "malformed_id" {
							continue // Reply identifiers are opaque TEXT, not UUIDs.
						}
						t.Run(attack, func(t *testing.T) {
							bad := fact
							fields := historicalContextFields(t, fact.raw)
							primary := historicalContextPrimary(fact.family)
							switch attack {
							case "wrapper_key", "alias":
								bad.key = uuid.NewString()
								if fact.family == "fan_out_obligations" {
									bad.key = `intent|` + uuid.NewString() + `|root|handler_rule|handlers["items.ready"].rules[0]`
								}
							case "embedded_id":
								fields[primary] = uuid.NewString()
							case "missing_id":
								delete(fields, primary)
							case "malformed_id":
								fields[primary] = "not-a-uuid"
							}
							bad.raw = historicalContextJSON(t, fields)
							f.reject(t, fact, bad, attack == "alias")
						})
					}
					if fact.family == "event_deliveries" || fact.family == "committed_replay_scopes" || fact.family == "timers" {
						for _, owner := range []string{"foreign", "absent", "malformed"} {
							t.Run("owning_run_"+owner, func(t *testing.T) {
								bad := fact
								fields := historicalContextFields(t, fact.raw)
								switch owner {
								case "foreign":
									fields["run_id"] = f.foreignID
								case "absent":
									delete(fields, "run_id")
								case "malformed":
									fields["run_id"] = "not-a-uuid"
								}
								bad.raw = historicalContextJSON(t, fields)
								f.reject(t, fact, bad, false)
							})
						}
					}
				})
			}
		})
	}
}

func TestRunForkHistoricalContextCanonicalWriterControlsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newHistoricalContextFixture(t, backend)
			f.requireLatestComplete(t)
			before := historicalContextDatabaseRows(t, f.db, f.postgres)
			for i := 0; i < 2; i++ {
				got, err := f.planner.PlanRunFork(testAuthorActivityContext(), runfork.RunForkPlanRequest{SourceRunID: f.source.runID, At: f.anchorID})
				if err != nil || !reflect.DeepEqual(got, f.initial) {
					t.Fatalf("repeat fixed-R planning changed: err=%v\ngot=%#v\nwant=%#v", err, got, f.initial)
				}
			}
			latest, err := f.planner.PlanRunFork(testAuthorActivityContext(), runfork.RunForkPlanRequest{SourceRunID: f.source.runID})
			if err != nil || latest.ForkPoint.EventID != f.latestID || latest.EventCountAtFork != f.initial.EventCountAtFork+1 {
				t.Fatalf("default latest cursor = %#v, err=%v", latest.ForkPoint, err)
			}
			historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, f.db, f.postgres))
		})
	}
}

func newHistoricalContextFixture(t *testing.T, backend eventRecordContractBackend) historicalContextFixture {
	t.Helper()
	return newHistoricalContextFixtureWithSeed(t, backend, nil)
}

func newHistoricalContextFixtureWithSeed(t *testing.T, backend eventRecordContractBackend, prepare func(*testing.T, *sql.Tx, historicalContextFixture)) historicalContextFixture {
	t.Helper()
	ctx := testAuthorActivityContext()
	opened := backend.open(t)
	f := historicalContextFixture{db: opened.db, planner: opened.store.(historicalContextPlanner), postgres: backend.name == "postgres", source: newRunForkRevisionMatrixFixture(), latestID: uuid.NewString(), foreignID: uuid.NewString(), anchorID: uuid.NewString()}
	s := f.source
	requireRunFixtureForTest(t, ctx, opened.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: s.runID, StartedAt: s.at})
	requireRunFixtureForTest(t, ctx, opened.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: f.foreignID, StartedAt: s.at})
	seedTestAgentRow(t, ctx, f.db, f.postgres, mustTestAgentIdentityForRun(s.runID, "revision-matrix-agent", ""), "active")
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	seedRunForkRevisionMatrixFacts(t, ctx, tx, s, true, f.postgres)
	// Select an independent canonical event in the same atomic revision. A
	// corruption of the subject event's key must not move the cursor to R2 and
	// thereby discard the very R1 fact whose admission is under test.
	seedRunForkRevisionMatrixEvent(t, ctx, tx, s.runID, f.anchorID, s.at.Add(time.Millisecond), f.postgres)
	if prepare != nil {
		prepare(t, tx, f)
	}
	f.finalize(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	f.initial, err = f.planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: s.runID, At: f.anchorID})
	if err != nil {
		t.Fatalf("healthy thirteen-family PlanRunFork prerequisite: %v", err)
	}
	if f.initial.ForkPoint.Revision != 1 || f.initial.EventCountAtFork != 2 {
		t.Fatalf("initial canonical cut = %#v events=%d", f.initial.ForkPoint, f.initial.EventCountAtFork)
	}
	rows, err := f.db.QueryContext(ctx, `SELECT family,fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=1 ORDER BY family,fact_key`, s.runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var fact historicalContextFact
		if err := rows.Scan(&fact.family, &fact.key, &fact.raw); err != nil {
			t.Fatal(err)
		}
		if fact.family == "events" && fact.key == f.anchorID {
			continue // The independent selector is not a corruption subject.
		}
		f.facts = append(f.facts, fact)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	families := make(map[string]bool)
	for _, fact := range f.facts {
		families[fact.family] = true
	}
	if len(families) != 13 {
		t.Fatalf("canonical family census=%d, want 13", len(families))
	}
	tx, err = f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	seedRunForkRevisionMatrixEvent(t, ctx, tx, s.runID, f.latestID, s.at.Add(time.Second), f.postgres)
	f.finalize(t, tx)
	// Immutable families cannot lawfully publish a changed payload. These exact
	// later copies are diagnostic retained-ledger controls, NOT writer updates.
	// Their bytes came from the real thirteen-family canonical finalizer above.
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) SELECT run_id,2,family,fact_key,fact,present FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=1`, s.runID)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	f.requireLatestComplete(t)
	return f
}

func (f historicalContextFixture) finalize(t *testing.T, tx *sql.Tx) {
	t.Helper()
	effects, err := runforkrevision.ForRun(f.source.runID, runforkrevision.AllFamilies()...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := finalizeRunForkRevisionMatrix(testAuthorActivityContext(), tx, f.postgres, effects); err != nil {
		t.Fatal(err)
	}
}

func (f historicalContextFixture) requireLatestComplete(t *testing.T) {
	t.Helper()
	tx, err := f.db.BeginTx(testAuthorActivityContext(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := validateRunForkRevisionMatrix(testAuthorActivityContext(), tx, f.postgres, f.source.runID); err != nil {
		t.Fatalf("latest/live completeness must remain valid after historical injection: %v", err)
	}
}

func (f historicalContextFixture) reject(t *testing.T, original, bad historicalContextFact, alias bool) {
	t.Helper()
	ctx := testAuthorActivityContext()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := f.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		exec(`DELETE FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=1 AND family=$2 AND fact_key=$3`, f.source.runID, bad.family, bad.key)
		if bad.family != original.family || bad.key != original.key {
			exec(`DELETE FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=2 AND family=$2 AND fact_key=$3`, f.source.runID, bad.family, bad.key)
		}
		exec(`INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,1,$2,$3,$4,TRUE) ON CONFLICT (run_id,revision,family,fact_key) DO UPDATE SET fact=excluded.fact,present=TRUE`, f.source.runID, original.family, original.key, original.raw)
	})
	if !alias {
		exec(`DELETE FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=1 AND family=$2 AND fact_key=$3`, f.source.runID, original.family, original.key)
	}
	exec(`INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,1,$2,$3,$4,TRUE)`, f.source.runID, bad.family, bad.key, bad.raw)
	if bad.family != original.family || bad.key != original.key {
		// Suppress only the injected alias at the later cut, preserving valid latest.
		exec(`INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,2,$2,$3,'{}',FALSE)`, f.source.runID, bad.family, bad.key)
	}
	f.requireLatestComplete(t)
	before := historicalContextDatabaseRows(t, f.db, f.postgres)
	plan, err := f.planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: f.source.runID, At: f.anchorID})
	historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, f.db, f.postgres))
	if err == nil {
		t.Fatalf("PlanRunFork accepted corrupted older-R %s/%s; latest remains canonical; plan_equal_healthy=%v", bad.family, bad.key, reflect.DeepEqual(plan, f.initial))
	}
	if !reflect.DeepEqual(plan, runfork.RunForkPlan{}) {
		t.Fatalf("corrupt history returned partial plan: %#v; error=%v", plan, err)
	}
	t.Logf("older-R rejection: %v; all persisted rows unchanged", err)
}

func historicalContextPrimary(family string) string {
	return map[string]string{
		"events": "event_id", "entity_mutations": "mutation_id", "entity_metadata": "entity_id",
		"event_deliveries": "delivery_id", "committed_replay_scopes": "event_id", "event_receipts": "receipt_id",
		"dead_letters": "dead_letter_id", "timers": "timer_id", "agent_sessions": "session_id",
		"agent_turns": "turn_id", "agent_conversation_audits": "session_id", "reply_contexts": "reply_context_id",
		"fan_out_obligations": "triggering_delivery_id",
	}[family]
}

func historicalContextFields(t *testing.T, raw string) map[string]any {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func historicalContextJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// Capture every application table, not merely row counts or a hand-picked list
// that could miss a newly introduced fork write. Baseline is AFTER injection.
func historicalContextDatabaseRows(t *testing.T, db *sql.DB, postgres bool) map[string][]string {
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
	result := make(map[string][]string, len(tables))
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		result[table] = []string{historicalContextJSON(t, columns)}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
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
			result[table] = append(result[table], historicalContextJSON(t, values))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(result[table][1:])
	}
	return result
}

func historicalContextRequireUnchanged(t *testing.T, before, after map[string][]string) {
	t.Helper()
	if reflect.DeepEqual(before, after) {
		return
	}
	for table, want := range before {
		if !reflect.DeepEqual(want, after[table]) {
			t.Errorf("persistent table %s changed:\nbefore=%s\nafter=%s", table, fmt.Sprint(want), fmt.Sprint(after[table]))
		}
	}
	t.Fatal("planning changed the full persisted database snapshot")
}
