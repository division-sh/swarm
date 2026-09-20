package pipelinepersistence

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

	obligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	_ "modernc.org/sqlite"
)

const summaryExperimentRun = "b055c052-9546-4e28-8ebf-e3ac930e6d25"

// Test-only interception keeps the production query, all arguments and the
// production Scan/Validate owner. The sole SQL difference is this CTE hint.
type summaryExperimentQueryer struct {
	pipelineQueryer
	original bool
	calls    int
	query    string
	args     []any
}

func (q *summaryExperimentQueryer) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.calls++
	q.query, q.args = query, append([]any(nil), args...)
	if q.original {
		if strings.Count(query, "WITH classified AS MATERIALIZED (") != 1 {
			panic("unexpected materialized summary query shape")
		}
		query = strings.Replace(query, "WITH classified AS MATERIALIZED (", "WITH classified AS (", 1)
	}
	return q.pipelineQueryer.QueryRowContext(ctx, query, args...)
}

func summaryExperimentDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "summary.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	// Query-focused fixture: retain relevant production unique/join/run indexes.
	// Nullable values intentionally exercise hostile SQL three-valued logic.
	for _, query := range []string{
		`PRAGMA journal_mode=WAL`,
		`CREATE TABLE events (insertion_sequence INTEGER PRIMARY KEY, event_id TEXT UNIQUE, run_id TEXT, event_name TEXT, created_at TEXT)`,
		`CREATE INDEX idx_events_run ON events(run_id,created_at) WHERE run_id IS NOT NULL`,
		`CREATE TABLE event_receipts (receipt_id INTEGER PRIMARY KEY, event_id TEXT, subscriber_type TEXT, subscriber_id TEXT, outcome TEXT, UNIQUE(event_id,subscriber_type,subscriber_id))`,
		`CREATE TABLE decision_card_route_obligations (insertion_sequence INTEGER PRIMARY KEY, event_id TEXT UNIQUE, status TEXT)`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func summaryExperimentPair(t testing.TB, ctx context.Context, q pipelineQueryer, run string) obligation.RunSummary {
	t.Helper()
	a := &summaryExperimentQueryer{pipelineQueryer: q, original: true}
	b := &summaryExperimentQueryer{pipelineQueryer: q}
	want, oldErr := summarizePipelineRun(ctx, a, run, false)
	got, newErr := summarizePipelineRun(ctx, b, run, false)
	if want != got || fmt.Sprint(oldErr) != fmt.Sprint(newErr) || a.calls != b.calls || a.query != b.query || !reflect.DeepEqual(a.args, b.args) {
		t.Fatalf("differential: original=%+v err=%v calls=%d materialized=%+v err=%v calls=%d", want, oldErr, a.calls, got, newErr, b.calls)
	}
	if tx, ok := q.(*sql.Tx); ok {
		actual, err := (&PipelineSQLiteOwner{}).SummarizeRunTx(ctx, tx, run)
		if actual != want || fmt.Sprint(err) != fmt.Sprint(oldErr) {
			t.Fatalf("named owner: got=%+v err=%v want=%+v err=%v", actual, err, want, oldErr)
		}
	}
	return got
}

func TestSQLiteSummaryMaterializationPostgresUnchanged(t *testing.T) {
	db := summaryExperimentDB(t)
	sqlite := &summaryExperimentQueryer{pipelineQueryer: db}
	if _, err := summarizePipelineRun(context.Background(), sqlite, summaryExperimentRun, false); err != nil {
		t.Fatal(err)
	}
	pg := &summaryExperimentQueryer{pipelineQueryer: db}
	// Capture PostgreSQL text; executing its casts on SQLite is not a PG proof.
	_, _ = summarizePipelineRun(context.Background(), pg, summaryExperimentRun, true)
	want := strings.Replace(sqlite.query, "WITH classified AS MATERIALIZED (", "WITH classified AS (", 1)
	want = strings.Replace(want, sqliteDiagnosticDirectReplayExclusionSQL("e"), postgresDiagnosticDirectReplayExclusionSQL("e", 1), 1)
	want = strings.Replace(want, "WHERE e.run_id = ?", fmt.Sprintf("WHERE e.run_id = $%d::uuid", len(diagnosticDirectReplayEventArgs())+1), 1)
	if pg.query != want || !reflect.DeepEqual(pg.args, sqlite.args) || strings.Contains(pg.query, "MATERIALIZED") {
		t.Fatalf("PostgreSQL query/arguments changed: %s args=%v", pg.query, pg.args)
	}
}

func TestSQLiteSummaryMaterializationTempStoreCleanup(t *testing.T) {
	for _, mode := range []string{"FILE", "MEMORY"} {
		t.Run(mode, func(t *testing.T) {
			db := summaryExperimentDB(t)
			if _, err := db.Exec("PRAGMA temp_store=" + mode); err != nil {
				t.Fatal(err)
			}
			// A small test-only cache plus a multi-megabyte joined projection
			// exercises both temporary-store policies, without asserting OS spill.
			if _, err := db.Exec("PRAGMA temp.cache_size=8"); err != nil {
				t.Fatal(err)
			}
			var actualMode int
			if err := db.QueryRow("PRAGMA temp_store").Scan(&actualMode); err != nil {
				t.Fatal(err)
			}
			if (mode == "FILE" && actualMode != 1) || (mode == "MEMORY" && actualMode != 2) {
				t.Fatalf("temp_store=%d", actualMode)
			}
			ctx := context.Background()
			const count = 32768
			for _, query := range []string{
				`WITH RECURSIVE sequence(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM sequence WHERE n<32768)
				 INSERT INTO events(event_id,run_id,event_name,created_at)
				 SELECT printf('%08x-0000-4000-8000-%012x',n,n), ?,
				 CASE WHEN n%5=0 THEN 'work.ready' ELSE 'platform.runtime_log' END,'2026-01-01' FROM sequence`,
				`INSERT INTO event_receipts(event_id,subscriber_type,subscriber_id,outcome)
				 SELECT event_id,'platform','pipeline','success' FROM events WHERE run_id=?`,
				`INSERT INTO decision_card_route_obligations(event_id,status)
				 SELECT event_id,'pending' FROM events WHERE run_id=?`,
			} {
				if _, err := db.Exec(query, summaryExperimentRun); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			want := obligation.RunSummary{RunID: summaryExperimentRun, Acknowledged: count / 5, Deferred: count / 5, ProcessedDeferred: count / 5, DiagnosticExcluded: count - count/5}
			original := &summaryExperimentQueryer{pipelineQueryer: tx, original: true}
			old, oldErr := summarizePipelineRun(ctx, original, summaryExperimentRun, false)
			owner := &PipelineSQLiteOwner{}
			if got, err := owner.SummarizeRunTx(ctx, tx, summaryExperimentRun); got != want || old != want || oldErr != nil || err != nil {
				t.Fatalf("large got=%+v err=%v original=%+v err=%v want=%+v", got, err, old, oldErr, want)
			}
			if _, err := tx.Exec(`UPDATE event_receipts SET outcome='reject'`); err != nil {
				t.Fatal(err)
			}
			changed := want
			changed.Acknowledged = 0
			changed.ProcessedDeferred = 0
			changed.TerminalNonSuccess = count / 5
			if got, err := owner.SummarizeRunTx(ctx, tx, summaryExperimentRun); got != changed || err != nil {
				t.Fatalf("fresh large got=%+v err=%v want=%+v", got, err, changed)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			fresh, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, readErr := owner.SummarizeRunTx(ctx, fresh, summaryExperimentRun)
			closeErr := fresh.Rollback()
			if got != want || readErr != nil || closeErr != nil {
				t.Fatalf("rollback large got=%+v read=%v close=%v want=%+v", got, readErr, closeErr, want)
			}
			for _, original := range []bool{false, true} {
				cancelCtx, cancel := context.WithCancel(ctx)
				q := &summaryExperimentQueryer{pipelineQueryer: &summaryTimedCancelQueryer{pipelineQueryer: db, cancel: cancel}, original: original}
				_, err := summarizePipelineRun(cancelCtx, q, summaryExperimentRun, false)
				cancel()
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("original=%t mid-query cancel=%v", original, err)
				}
			}
			// The same pool1 connection remains usable after cancellation and rollback.
			checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			var eventCount int
			if err := db.QueryRowContext(checkCtx, `SELECT count(*) FROM events`).Scan(&eventCount); err != nil || eventCount != count {
				t.Fatalf("post-cancel count=%d err=%v", eventCount, err)
			}
			if inUse := db.Stats().InUse; inUse != 0 {
				t.Fatalf("retained connections=%d", inUse)
			}
			var temporaryTables int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_temp_master WHERE type='table'`).Scan(&temporaryTables); err != nil {
				t.Fatal(err)
			}
			if temporaryTables != 0 {
				t.Fatalf("retained temp tables=%d", temporaryTables)
			}
			if _, err := db.Exec(`DELETE FROM event_receipts; DELETE FROM decision_card_route_obligations; DELETE FROM events`); err != nil {
				t.Fatal(err)
			}
			if got := summaryExperimentPair(t, ctx, db, summaryExperimentRun); got != (obligation.RunSummary{RunID: summaryExperimentRun}) {
				t.Fatalf("fresh empty=%+v", got)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if open := db.Stats().OpenConnections; open != 0 {
				t.Fatalf("open after close=%d", open)
			}
			t.Logf("temp_store=%s events=%d rollback/cancel/fresh/empty/pool-close passed", mode, count)
		})
	}
}

type summaryTimedCancelQueryer struct {
	pipelineQueryer
	cancel context.CancelFunc
}

func (q *summaryTimedCancelQueryer) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	timer := time.AfterFunc(2*time.Millisecond, q.cancel)
	defer timer.Stop()
	return q.pipelineQueryer.QueryRowContext(ctx, query, args...)
}

func TestSQLiteSummaryMaterializationDifferential(t *testing.T) {
	db := summaryExperimentDB(t)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	names := []any{"work.ready", "platform.runtime_log", "platform.agent_directive", "platform.inbound_recorded", nil, "", " platform.runtime_log", "PLATFORM.RUNTIME_LOG"}
	receipts := []any{"absent", "success", "reject", "waiting", nil, "hostile-outcome"}
	routes := []any{"absent", "pending", "completed", "quarantined", "superseded", nil, "hostile-status"}
	want := obligation.RunSummary{RunID: summaryExperimentRun}
	index := 0
	for _, name := range names {
		for _, receipt := range receipts {
			for _, route := range routes {
				id := fmt.Sprintf("event-%04d", index)
				index++
				if _, err := tx.Exec(`INSERT INTO events(event_id,run_id,event_name,created_at) VALUES(?,?,?,?)`, id, summaryExperimentRun, name, "2026-01-01"); err != nil {
					t.Fatal(err)
				}
				if receipt != "absent" {
					if _, err := tx.Exec(`INSERT INTO event_receipts(event_id,subscriber_type,subscriber_id,outcome) VALUES(?,'platform','pipeline',?)`, id, receipt); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := tx.Exec(`INSERT INTO event_receipts(event_id,subscriber_type,subscriber_id,outcome) VALUES(?,'platform','other','success')`, id); err != nil {
					t.Fatal(err)
				}
				if route != "absent" {
					if _, err := tx.Exec(`INSERT INTO decision_card_route_obligations(event_id,status) VALUES(?,?)`, id, route); err != nil {
						t.Fatal(err)
					}
				}
				diagnostic := name == "platform.runtime_log" || name == "platform.agent_directive" || name == "platform.inbound_recorded"
				if diagnostic {
					want.DiagnosticExcluded++
					continue
				}
				if name == nil {
					continue
				}
				if receipt == "absent" && (route == "absent" || route == "completed") {
					want.Replayable++
				}
				if receipt == "success" {
					want.Acknowledged++
				}
				if receipt != "absent" && receipt != nil && receipt != "success" {
					want.TerminalNonSuccess++
				}
				if route == "pending" {
					want.Deferred++
					if receipt == "success" {
						want.ProcessedDeferred++
					}
				}
			}
		}
	}
	if _, err := tx.Exec(`INSERT INTO events(event_id,run_id,event_name) VALUES('foreign','other-run','work.ready'),('unscoped',NULL,'work.ready')`); err != nil {
		t.Fatal(err)
	}
	got := summaryExperimentPair(t, ctx, tx, "  "+summaryExperimentRun+" ")
	if got != want {
		t.Fatalf("got=%+v independent truth-table=%+v", got, want)
	}
	t.Logf("cases=%d summary=%+v", index, got)
	// A fresh statement in the same transaction must see intervening writes.
	if _, err := tx.Exec(`INSERT INTO event_receipts(event_id,subscriber_type,subscriber_id,outcome) VALUES('event-0000','platform','pipeline','success')`); err != nil {
		t.Fatal(err)
	}
	want.Replayable--
	want.Acknowledged++
	if got := summaryExperimentPair(t, ctx, tx, summaryExperimentRun); got != want {
		t.Fatalf("fresh-write got=%+v want=%+v", got, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := summaryExperimentPair(t, ctx, db, summaryExperimentRun); got != (obligation.RunSummary{RunID: summaryExperimentRun}) {
		t.Fatalf("rollback=%+v", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	summaryExperimentPair(t, ctx, db, summaryExperimentRun)
}

func TestSQLiteSummaryMaterializationErrorPrecedence(t *testing.T) {
	for _, broken := range []string{"none", "events", "receipts", "routes", "column"} {
		t.Run(broken, func(t *testing.T) {
			db := summaryExperimentDB(t)
			query := map[string]string{"events": "DROP TABLE events", "receipts": "DROP TABLE event_receipts", "routes": "DROP TABLE decision_card_route_obligations", "column": "ALTER TABLE event_receipts RENAME COLUMN outcome TO missing_outcome"}[broken]
			if query != "" {
				if _, err := db.Exec(query); err != nil {
					t.Fatal(err)
				}
			}
			for _, mode := range []bool{false, true} {
				q := &summaryExperimentQueryer{pipelineQueryer: db, original: mode}
				_, err := summarizePipelineRun(context.Background(), q, "invalid-run", false)
				if err == nil || !strings.HasPrefix(err.Error(), "pipeline run summary:") || q.calls != 0 {
					t.Fatalf("invalid run precedence: calls=%d err=%v", q.calls, err)
				}
			}
			summaryExperimentPair(t, context.Background(), db, summaryExperimentRun)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for _, mode := range []bool{false, true} {
				q := &summaryExperimentQueryer{pipelineQueryer: db, original: mode}
				_, err := summarizePipelineRun(ctx, q, summaryExperimentRun, false)
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation precedence=%v", err)
				}
			}
		})
	}
}

func TestSQLiteSummaryMaterializationExplain(t *testing.T) {
	db := summaryExperimentDB(t)
	var version string
	if err := db.QueryRow(`SELECT sqlite_version()`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("native SQLite=%s", version)
	q := &summaryExperimentQueryer{pipelineQueryer: db}
	if _, err := summarizePipelineRun(context.Background(), q, summaryExperimentRun, false); err != nil {
		t.Fatal(err)
	}
	for _, materialized := range []bool{false, true} {
		query := q.query
		if !materialized {
			query = strings.Replace(query, "WITH classified AS MATERIALIZED (", "WITH classified AS (", 1)
		}
		rows, err := db.Query("EXPLAIN QUERY PLAN "+query, q.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, detail)
			t.Logf("materialized=%t plan %d %d %s", materialized, id, parent, detail)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(strings.Join(plan, "\n"), "MATERIALIZE classified") != materialized {
			t.Fatal("unexpected materialization plan")
		}
		rows, err = db.Query("EXPLAIN "+query, q.args...)
		if err != nil {
			t.Fatal(err)
		}
		counts := map[string]int{}
		for rows.Next() {
			var address, p1, p2, p3, p5 int
			var op string
			var p4, comment any
			if err := rows.Scan(&address, &op, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
				t.Fatal(err)
			}
			counts[op]++
			t.Logf("materialized=%t opcode %d %s %d %d %d %v %d %v", materialized, address, op, p1, p2, p3, p4, p5, comment)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		t.Logf("materialized=%t opcode_counts=%v", materialized, counts)
	}
}

func summaryExperimentSeed(b testing.TB, db *sql.DB, count, diagnosticPercent int) {
	b.Helper()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	e, err := tx.Prepare(`INSERT INTO events(event_id,run_id,event_name,created_at) VALUES(?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	r, err := tx.Prepare(`INSERT INTO event_receipts(event_id,subscriber_type,subscriber_id,outcome) VALUES(?,'platform','pipeline',?)`)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	o, err := tx.Prepare(`INSERT INTO decision_card_route_obligations(event_id,status) VALUES(?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	defer o.Close()
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i)
		name := "work.ready"
		if i%100 < diagnosticPercent {
			name = diagnosticDirectReplayEventNames()[i%3]
		}
		if _, err := e.Exec(id, summaryExperimentRun, name, "2026-01-01"); err != nil {
			b.Fatal(err)
		}
		if i%3 != 0 {
			outcome := "success"
			if i%7 == 0 {
				outcome = "reject"
			}
			if _, err := r.Exec(id, outcome); err != nil {
				b.Fatal(err)
			}
		}
		if i%5 == 0 {
			status := "pending"
			if i%2 == 0 {
				status = "completed"
			}
			if _, err := o.Exec(id, status); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkSQLiteSummaryMaterialization(b *testing.B) {
	for _, count := range []int{128, 512, 2048} {
		for _, diagnostic := range []int{0, 80} {
			b.Run(fmt.Sprintf("events%d/diagnostic%d", count, diagnostic), func(b *testing.B) {
				db := summaryExperimentDB(b)
				summaryExperimentSeed(b, db, count, diagnostic)
				want := summaryExperimentPair(b, context.Background(), db, summaryExperimentRun)
				for _, mode := range []string{"original", "materialized"} {
					b.Run(mode, func(b *testing.B) {
						ctx := context.Background()
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							tx, err := db.BeginTx(ctx, nil)
							if err != nil {
								b.Fatal(err)
							}
							q := &summaryExperimentQueryer{pipelineQueryer: tx, original: mode == "original"}
							got, readErr := summarizePipelineRun(ctx, q, summaryExperimentRun, false)
							closeErr := tx.Rollback()
							if readErr != nil || closeErr != nil || got != want {
								b.Fatalf("result=%+v read=%v rollback=%v", got, readErr, closeErr)
							}
						}
					})
				}
			})
		}
	}
}
