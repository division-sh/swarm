package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
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
