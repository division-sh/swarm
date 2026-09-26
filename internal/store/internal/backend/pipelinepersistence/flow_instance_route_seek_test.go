package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

// Retain the old predicate as the differential oracle, including its treatment
// of NULL and empty paths. Production callers admit a nonempty instance path.
func sqliteRouteUpdateBeforeSeek() string {
	return strings.Replace(sqliteFlowInstanceRouteUpdateSQL, "AND flow_instance = ?", "AND COALESCE(flow_instance, '') = ?", 1)
}

func TestSQLiteRouteUpdateExactPathPreservesSelectedRows(t *testing.T) {
	db, sets := sqliteRouteStatementFixture(t, 1)
	route := sets[0].Routes[0]
	for _, path := range []any{nil, "", route.Identity.Route.InstancePath, "other/i000", "review/i001"} {
		for _, run := range []string{"run", "other-run"} {
			for _, materialized := range []bool{true, false} {
				if _, err := db.Exec(`INSERT INTO routing_rules(event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_materialized,status)
					VALUES('work.ready','node','receiver',?,?, 'old',?,'inactive')`, run, path, materialized); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	// Duplicate exact rows must all be updated, not silently collapsed.
	if _, err := db.Exec(`INSERT INTO routing_rules(event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_materialized,status)
		VALUES('work.ready','node','receiver','run',?,'old',TRUE,'inactive')`, route.Identity.Route.InstancePath); err != nil {
		t.Fatal(err)
	}
	var baseline []int64
	for _, query := range []string{sqliteRouteUpdateBeforeSeek(), sqliteFlowInstanceRouteUpdateSQL} {
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		result, err := tx.Exec(query, "new", 1, route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.RunID, route.Identity.Route.InstancePath)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		count, err := result.RowsAffected()
		if err != nil || count != 2 {
			_ = tx.Rollback()
			t.Fatalf("affected=%d err=%v", count, err)
		}
		rows, err := tx.Query(`SELECT rule_id FROM routing_rules WHERE source_flow='new' AND materialized_from=1 AND status='active' ORDER BY rule_id`)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		var selected []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			selected = append(selected, id)
		}
		readErr := rows.Err()
		closeErr := rows.Close()
		rollbackErr := tx.Rollback()
		if readErr != nil || closeErr != nil || rollbackErr != nil {
			t.Fatalf("read=%v close=%v rollback=%v", readErr, closeErr, rollbackErr)
		}
		if baseline == nil {
			baseline = selected
		} else if !reflect.DeepEqual(baseline, selected) {
			t.Fatalf("selected=%v want=%v", selected, baseline)
		}
	}
}

func sqliteRouteSeekFixture(t testing.TB) *sql.DB {
	t.Helper()
	db, _ := sqliteRouteStatementFixture(t, 0)
	// These are the existing authoritative routing indexes, not new DDL.
	for _, query := range []string{
		`CREATE INDEX idx_routing_event ON routing_rules(run_id,event_pattern,status)`,
		`CREATE INDEX idx_routing_subscriber ON routing_rules(run_id,subscriber_id)`,
		`CREATE INDEX idx_routing_flow ON routing_rules(run_id,flow_instance) WHERE flow_instance IS NOT NULL`,
		`WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<1023)
		 INSERT INTO routing_rules(event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_materialized,status)
		 SELECT 'work.ready','node','receiver','run',printf('review/i%04d',i),'review',TRUE,'active' FROM n`,
		`ANALYZE`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func sqliteRouteSourceBeforeSeek() string {
	return strings.Replace(sqliteFlowInstanceRouteSourceSQL, "run_id IS NULL", "1=1", 1)
}

func TestSQLiteWildcardSourceRunlessSeekPreservesSelection(t *testing.T) {
	if !strings.Contains(sqliteFlowInstanceRouteSourceSQL, "WHERE run_id IS NULL") {
		t.Fatal("wildcard source query no longer contains the runless predicate")
	}
	db := sqliteRouteSeekFixture(t)
	for _, query := range []string{
		`INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,run_id,source_flow,is_wildcard,is_materialized,status,created_at)
		 VALUES(3001,'work.ready','node','receiver',NULL,'other',TRUE,FALSE,'active','2019-01-01')`,
		`INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,run_id,source_flow,is_wildcard,is_materialized,status,created_at)
		 VALUES(3002,'work.ready','node','receiver',NULL,'review',TRUE,FALSE,'active','2021-01-01')`,
		`ANALYZE`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		pattern, subscriber, flow string
		want                      sql.NullInt64
	}{
		{"work.ready", "receiver", "review", sql.NullInt64{Int64: 1, Valid: true}},
		{"work.ready", "receiver", "other", sql.NullInt64{Int64: 3001, Valid: true}},
		{"work.done", "receiver", "review", sql.NullInt64{Int64: 2, Valid: true}},
		{"work.ready", "missing", "review", sql.NullInt64{}},
	} {
		var before, after sql.NullInt64
		args := []any{tc.pattern, "node", tc.subscriber, tc.flow}
		beforeErr := db.QueryRow(sqliteRouteSourceBeforeSeek(), args...).Scan(&before)
		afterErr := db.QueryRow(sqliteFlowInstanceRouteSourceSQL, args...).Scan(&after)
		if beforeErr != afterErr {
			t.Fatalf("lookup %v changed error: before=%v after=%v", args, beforeErr, afterErr)
		}
		if before != after || after != tc.want {
			t.Fatalf("lookup %v changed source: before=%v after=%v want=%v", args, before, after, tc.want)
		}
	}
	plan := func(query string) string {
		t.Helper()
		rows, err := db.Query("EXPLAIN QUERY PLAN "+query, "work.ready", "node", "receiver", "review")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var id, parent, auxiliary int
			var detail string
			if err := rows.Scan(&id, &parent, &auxiliary, &detail); err != nil {
				t.Fatal(err)
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return strings.Join(details, "\n")
	}
	beforePlan, afterPlan := plan(sqliteRouteSourceBeforeSeek()), plan(sqliteFlowInstanceRouteSourceSQL)
	if !strings.Contains(afterPlan, "idx_routing_event (run_id=? AND event_pattern=? AND status=?)") {
		t.Fatalf("wildcard source lost exact existing-index seek: before=%s after=%s", beforePlan, afterPlan)
	}
	t.Logf("wildcard source plans: before=%s after=%s", beforePlan, afterPlan)
}

func BenchmarkSQLiteWildcardSourceRunlessSeek(b *testing.B) {
	for _, exact := range []bool{false, true} {
		b.Run(fmt.Sprintf("runless=%t", exact), func(b *testing.B) {
			db := sqliteRouteSeekFixture(b)
			query := sqliteRouteSourceBeforeSeek()
			if exact {
				query = sqliteFlowInstanceRouteSourceSQL
			}
			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var ruleID int64
				if err := stmt.QueryRow("work.ready", "node", "receiver", "review").Scan(&ruleID); err != nil || ruleID != 1 {
					b.Fatalf("rule=%d err=%v", ruleID, err)
				}
			}
		})
	}
}

func TestPostgresWildcardSourceRunlessSeekPreservesSelection(t *testing.T) {
	if !strings.Contains(postgresFlowInstanceRouteUpsertSQL, "WHERE run_id IS NULL") {
		t.Fatal("wildcard source query no longer contains the runless predicate")
	}
	_, db, _ := testutil.StartEmptyPostgres(t)
	const runID = "00000000-0000-4000-8000-000000000001"
	const sourceID = "00000000-0000-4000-8000-000000000002"
	for _, query := range []string{
		`CREATE TABLE routing_rules (
		 rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),event_pattern TEXT,subscriber_type TEXT,
		 subscriber_id TEXT,run_id UUID,flow_instance TEXT,source_flow TEXT,is_wildcard BOOLEAN,
		 is_materialized BOOLEAN,materialized_from UUID,status TEXT,created_at TIMESTAMPTZ,
		 CHECK ((is_materialized AND run_id IS NOT NULL) OR (NOT is_materialized AND run_id IS NULL)))`,
		`CREATE INDEX idx_routing_event ON routing_rules(run_id,event_pattern,status)`,
		`INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,source_flow,is_wildcard,is_materialized,status,created_at)
		 VALUES ('` + sourceID + `','work.ready','node','receiver','review',TRUE,FALSE,'active','2020-01-01')`,
		`INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,source_flow,is_wildcard,is_materialized,status,created_at)
		 VALUES ('00000000-0000-4000-8000-000000000003','work.ready','node','receiver','review',TRUE,FALSE,'active','2021-01-01')`,
		`INSERT INTO routing_rules(event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_wildcard,is_materialized,status,created_at)
		 SELECT 'work.ready','node','receiver','` + runID + `','review/i'||i,'review',FALSE,TRUE,'active',NOW()
		 FROM generate_series(0,1023) i`,
		`ANALYZE routing_rules`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	for _, exact := range []bool{false, true} {
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		query := postgresFlowInstanceRouteUpsertSQL
		if !exact {
			query = strings.Replace(query, "WHERE run_id IS NULL", "WHERE TRUE", 1)
		}
		planRows, err := tx.QueryContext(context.Background(), "EXPLAIN (COSTS OFF) "+query, "work.ready", "node", "receiver", runID, "review/i0", "review")
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		var plan []string
		for planRows.Next() {
			var line string
			if err := planRows.Scan(&line); err != nil {
				_ = planRows.Close()
				_ = tx.Rollback()
				t.Fatal(err)
			}
			plan = append(plan, line)
		}
		if err := planRows.Err(); err != nil {
			_ = planRows.Close()
			_ = tx.Rollback()
			t.Fatal(err)
		}
		_ = planRows.Close()
		planText := strings.Join(plan, "\n")
		if exact && (!strings.Contains(planText, "Index Scan using idx_routing_event") || !strings.Contains(planText, "Index Cond: ((run_id IS NULL)")) {
			_ = tx.Rollback()
			t.Fatalf("wildcard source lost exact existing-index seek: %s", planText)
		}
		t.Logf("runless=%t postgres upsert plan:\n%s", exact, planText)
		if _, err := tx.ExecContext(context.Background(), query, "work.ready", "node", "receiver", runID, "review/i0", "review"); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		var source string
		if err := tx.QueryRow(`SELECT materialized_from::text FROM routing_rules WHERE run_id=$1 AND flow_instance='review/i0'`, runID).Scan(&source); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if source != sourceID {
			t.Fatalf("runless=%t selected source %s, want %s", exact, source, sourceID)
		}
	}
}

func TestSQLiteRouteUpdateUsesExactInstanceIndex(t *testing.T) {
	db := sqliteRouteSeekFixture(t)
	rows, err := db.Query("EXPLAIN QUERY PLAN "+sqliteFlowInstanceRouteUpdateSQL, "review", 1, "work.ready", "node", "receiver", "run", "review/i0000")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, auxiliary int
		var detail string
		if err := rows.Scan(&id, &parent, &auxiliary, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if plan := strings.Join(details, "\n"); !strings.Contains(plan, "idx_routing_flow (run_id=? AND flow_instance=?)") {
		t.Fatalf("route update lost exact existing-index seek: %s", plan)
	}
}

func BenchmarkSQLiteRouteUpdateExactInstance(b *testing.B) {
	for _, exact := range []bool{false, true} {
		b.Run(fmt.Sprintf("exact=%t", exact), func(b *testing.B) {
			db := sqliteRouteSeekFixture(b)
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			defer tx.Rollback()
			query := sqliteFlowInstanceRouteUpdateSQL
			if !exact {
				query = sqliteRouteUpdateBeforeSeek()
			}
			stmt, err := tx.Prepare(query)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := stmt.Exec("review", 1, "work.ready", "node", "receiver", "run", "review/i0000")
				if err != nil {
					b.Fatal(err)
				}
				if n, err := result.RowsAffected(); err != nil || n != 1 {
					b.Fatalf("rows=%d err=%v", n, err)
				}
			}
		})
	}
}
