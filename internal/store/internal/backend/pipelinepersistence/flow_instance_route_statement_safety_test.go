package pipelinepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"modernc.org/sqlite"
)

type sqliteRouteStatementRecorder struct {
	mu           sync.Mutex
	prepared     map[string]int
	closed       map[string]int
	executed     map[string]int
	calls        []string
	failPrepare  string
	failure      error
	closeFailure error
	before       func(string, int)
}

func sqliteRouteStatementName(query string) string {
	switch query {
	case sqliteFlowInstanceRouteSourceSQL:
		return "source"
	case sqliteFlowInstanceRouteUpdateSQL:
		return "update"
	case sqliteFlowInstanceRouteInsertSQL:
		return "insert"
	case sqliteFlowInstanceRouteInactivateSQL:
		return "inactivate"
	}
	return ""
}

type sqliteRouteCountConnector struct {
	dsn string
	r   *sqliteRouteStatementRecorder
}

func (c sqliteRouteCountConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c sqliteRouteCountConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &sqliteRouteCountConn{Conn: conn, r: c.r}, nil
}

// Omitting the direct Exec/Query fast paths makes database/sql expose every
// physical preparation, including the non-reusing baseline, to this observer.
type sqliteRouteCountConn struct {
	driver.Conn
	r *sqliteRouteStatementRecorder
}

func (c *sqliteRouteCountConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *sqliteRouteCountConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	name := sqliteRouteStatementName(query)
	if name != "" && name == c.r.failPrepare {
		return nil, c.r.failure
	}
	stmt, err := c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return stmt, nil
	}
	c.r.mu.Lock()
	c.r.prepared[name]++
	c.r.mu.Unlock()
	return &sqliteRouteCountStmt{Stmt: stmt, name: name, r: c.r}, nil
}

type sqliteRouteCountStmt struct {
	driver.Stmt
	name string
	r    *sqliteRouteStatementRecorder
}

func (s *sqliteRouteCountStmt) record() {
	s.r.mu.Lock()
	s.r.executed[s.name]++
	n := s.r.executed[s.name]
	s.r.calls = append(s.r.calls, s.name)
	s.r.mu.Unlock()
	if s.r.before != nil {
		s.r.before(s.name, n)
	}
}
func (s *sqliteRouteCountStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.record()
	return s.Stmt.(driver.StmtExecContext).ExecContext(ctx, args)
}
func (s *sqliteRouteCountStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.record()
	return s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
}
func (s *sqliteRouteCountStmt) Close() error {
	s.r.mu.Lock()
	s.r.closed[s.name]++
	s.r.mu.Unlock()
	return errors.Join(s.Stmt.Close(), s.r.closeFailure)
}

func sqliteCountedRouteFixture(t *testing.T, owners int) (*sql.DB, []runtimebus.FlowInstanceRouteRecordSet, *sqliteRouteStatementRecorder) {
	t.Helper()
	db, sets := sqliteRouteStatementFixture(t, owners)
	var seq int
	var name, path string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	r := &sqliteRouteStatementRecorder{prepared: map[string]int{}, closed: map[string]int{}, executed: map[string]int{}}
	db = sql.OpenDB(sqliteRouteCountConnector{dsn: "file:" + path, r: r})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db, sets, r
}

func (r *sqliteRouteStatementRecorder) assertClosed(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !reflect.DeepEqual(r.prepared, r.closed) {
		t.Fatalf("handles remain open on return: prepared=%v closed=%v", r.prepared, r.closed)
	}
}

func TestSQLiteRouteTopologyStatementsReuseAndFreshSource(t *testing.T) {
	db, sets, r := sqliteCountedRouteFixture(t, 2)
	ctx := context.Background()
	// The second owner must observe a changed wildcard source even though its
	// source SELECT uses the same prepared native handle as the first owner.
	if _, err := db.Exec(`CREATE TRIGGER change_route_source AFTER INSERT ON routing_rules
	 WHEN NEW.is_materialized AND NEW.flow_instance='review/i000' AND NEW.event_pattern='work.ready'
	 BEGIN
	 UPDATE routing_rules SET status='inactive' WHERE rule_id=1;
	 INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,source_flow,is_wildcard,is_materialized,status,created_at)
	 VALUES(100,'work.ready','node','receiver','review',TRUE,FALSE,'active','2020-01-02');
	 END`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	got, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets)
	if err != nil || !reflect.DeepEqual(got, sets) {
		t.Fatalf("topology=%v err=%v", got, err)
	}
	r.assertClosed(t)
	if want := (map[string]int{"source": 1, "update": 1, "insert": 1, "inactivate": 1}); !reflect.DeepEqual(r.prepared, want) {
		t.Fatalf("prepared=%v want=%v", r.prepared, want)
	}
	if want := []string{"inactivate", "source", "update", "insert", "source", "update", "insert", "inactivate", "source", "update", "insert", "source", "update", "insert"}; !reflect.DeepEqual(r.calls, want) {
		t.Fatalf("execution order=%v want=%v", r.calls, want)
	}
	for i, want := range []int{1, 100} {
		var source int
		if err := tx.QueryRow(`SELECT materialized_from FROM routing_rules WHERE flow_instance=? AND event_pattern='work.ready'`, sets[i].Identity.Route.InstancePath).Scan(&source); err != nil || source != want {
			t.Fatalf("owner=%d source=%d want=%d err=%v", i, source, want, err)
		}
	}
	// The trigger changed the source after i000 was written. The next call
	// must refresh only i000; i001 is already exact.
	if _, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets); err != nil {
		t.Fatal(err)
	}
	r.assertClosed(t)
	if want := (map[string]int{"source": 2, "update": 2, "insert": 1, "inactivate": 2}); !reflect.DeepEqual(r.prepared, want) {
		t.Fatalf("second-call preparations=%v want=%v", r.prepared, want)
	}
	if r.executed["source"] != 6 || r.executed["update"] != 6 || r.executed["insert"] != 4 || r.executed["inactivate"] != 3 {
		t.Fatalf("unexpected exact-diff writes: %v", r.executed)
	}
	var repairedSource int
	if err := tx.QueryRow(`SELECT materialized_from FROM routing_rules WHERE flow_instance='review/i000' AND event_pattern='work.ready'`).Scan(&repairedSource); err != nil || repairedSource != 100 {
		t.Fatalf("triggered source was not repaired: source=%d err=%v", repairedSource, err)
	}
	beforeNoop := make(map[string]int, len(r.executed))
	for k, v := range r.executed {
		beforeNoop[k] = v
	}
	if _, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.executed, beforeNoop) {
		t.Fatalf("exact topology caused redundant writes: before=%v after=%v", beforeNoop, r.executed)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteRouteTopologyStatementsPrepareErrorsCloseAndRollback(t *testing.T) {
	for _, tc := range []struct{ name, context string }{
		{"inactivate", "inactivate sqlite flow-instance route owner"},
		{"source", "load sqlite flow instance route source"},
		{"update", "update sqlite flow instance route"},
		{"insert", "insert sqlite flow instance route"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, sets, r := sqliteCountedRouteFixture(t, 2)
			r.failPrepare, r.failure = tc.name, errors.New("route prepare refused")
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			got, err := replaceFlowInstanceRouteTopologyTx(context.Background(), tx, false, sets)
			if got != nil || !errors.Is(err, r.failure) || !strings.Contains(err.Error(), tc.context) {
				t.Fatalf("got=%v error=%v want wrapped %v and %q", got, err, r.failure, tc.context)
			}
			r.assertClosed(t)
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			assertSQLiteRouteMaterializedCount(t, db, 0)
		})
	}
}

func TestSQLiteRouteTopologyStatementsUpsertPhysicalCounts(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		name := "direct"
		if reuse {
			name = "reused"
		}
		t.Run(name, func(t *testing.T) {
			db, sets, r := sqliteCountedRouteFixture(t, 2)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			exec := &sqliteFlowInstanceRouteExecutor{tx: tx}
			if reuse {
				exec.statements = &[4]*sql.Stmt{}
			}
			defer exec.close()
			for pass := 0; pass < 2; pass++ {
				for _, set := range sets {
					for _, route := range set.Routes {
						if err := upsertSQLiteFlowInstanceRouteWithExecutor(context.Background(), exec, route); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			exec.close()
			r.assertClosed(t)
			want := map[string]int{"source": 8, "update": 8, "insert": 4}
			if !reflect.DeepEqual(r.executed, want) {
				t.Fatalf("executions=%v want=%v", r.executed, want)
			}
			if reuse {
				want = map[string]int{"source": 1, "update": 1, "insert": 1}
			}
			if !reflect.DeepEqual(r.prepared, want) {
				t.Fatalf("prepared=%v want=%v", r.prepared, want)
			}
			t.Logf("native prepares=%v; unchanged executions=%v", r.prepared, r.executed)
		})
	}
}

func TestSQLiteRouteTopologyStatementsCloseErrorPrecedence(t *testing.T) {
	for _, failPrepare := range []bool{false, true} {
		t.Run(fmt.Sprint(failPrepare), func(t *testing.T) {
			db, sets, r := sqliteCountedRouteFixture(t, 2)
			r.closeFailure = errors.New("route statement close refused")
			if failPrepare {
				r.failPrepare, r.failure = "update", errors.New("route update prepare refused")
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			got, err := replaceFlowInstanceRouteTopologyTx(context.Background(), tx, false, sets)
			want := r.closeFailure
			if failPrepare {
				want = r.failure
			}
			if got != nil || !errors.Is(err, want) {
				t.Fatalf("got=%v err=%v want=%v", got, err, want)
			}
			r.assertClosed(t)
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			assertSQLiteRouteMaterializedCount(t, db, 0)
		})
	}
}

func TestSQLiteRouteTopologyStatementsHostileFailureAndCancel(t *testing.T) {
	for _, failure := range []string{"insert", "update", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			db, sets, r := sqliteCountedRouteFixture(t, 2)
			if failure == "update" {
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := replaceFlowInstanceRouteTopologyTx(context.Background(), tx, false, sets); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				// A genuinely changed source must still take the UPDATE path.
				if _, err := db.Exec(`UPDATE routing_rules SET status='inactive' WHERE rule_id=1`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,source_flow,is_wildcard,is_materialized,status,created_at)
					VALUES(100,'work.ready','node','receiver','review',TRUE,FALSE,'active','2020-01-02')`); err != nil {
					t.Fatal(err)
				}
			}
			before := sqliteRouteRows(t, db)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "cancel" {
				r.before = func(name string, n int) {
					if name == "source" && n == 2 {
						cancel()
					}
				}
			} else {
				operation := "INSERT"
				if failure == "update" {
					operation = "UPDATE"
				}
				if _, err := db.Exec(`CREATE TRIGGER refuse_later_route BEFORE ` + operation + ` ON routing_rules
				 WHEN NEW.is_materialized AND NEW.status='active' AND NEW.event_pattern='work.done'
				 BEGIN SELECT RAISE(ABORT,'later route refused'); END`); err != nil {
					t.Fatal(err)
				}
			}
			// Keep transaction lifetime independent of the operation context, so
			// cancellation cannot hide missing cleanup behind automatic rollback.
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			got, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets)
			if got != nil || err == nil {
				t.Fatalf("partial success: got=%v err=%v", got, err)
			}
			if failure == "cancel" {
				if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "load sqlite flow instance route source") {
					t.Fatalf("cancellation lost: %v", err)
				}
			} else if !strings.Contains(err.Error(), failure+" sqlite flow instance route") || !strings.Contains(err.Error(), "later route refused") {
				t.Fatalf("mutation error lost: %v", err)
			}
			r.assertClosed(t)
			var prefix int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM routing_rules WHERE is_materialized AND status='active' AND flow_instance='review/i000' AND event_pattern='work.ready'`).Scan(&prefix); err != nil || prefix != 1 {
				t.Fatalf("failure did not follow earlier successful route mutation: count=%d err=%v", prefix, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if after := sqliteRouteRows(t, db); !reflect.DeepEqual(before, after) {
				t.Fatalf("rollback changed rows, timestamps or provenance: before=%v after=%v", before, after)
			}
		})
	}
}

func assertSQLiteRouteMaterializedCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM routing_rules WHERE is_materialized`).Scan(&count); err != nil || count != want {
		t.Fatalf("materialized count=%d want=%d err=%v", count, want, err)
	}
}

func sqliteRouteRows(t *testing.T, db *sql.DB) [][]any {
	t.Helper()
	rows, err := db.Query(`SELECT * FROM routing_rules ORDER BY rule_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		result = append(result, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
