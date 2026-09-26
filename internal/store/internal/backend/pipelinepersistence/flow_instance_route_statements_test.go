package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

func sqliteRouteStatementFixture(t testing.TB, owners int) (*sql.DB, []runtimebus.FlowInstanceRouteRecordSet) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "routes.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	for _, query := range []string{
		`CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT, bundle_hash TEXT,
		 origin_kind TEXT, trigger_event_id TEXT, trigger_event_type TEXT,
		 origin_service_id TEXT, origin_generation INTEGER, forked_from_run_id TEXT,
		 forked_from_event_id TEXT, started_at TIMESTAMP)`,
		`CREATE TABLE source_artifacts (bundle_hash TEXT PRIMARY KEY, source_blob BLOB,
		 member_count INTEGER, total_bytes INTEGER, created_at TIMESTAMP)`,
		`CREATE TABLE routing_rules (
		 rule_id INTEGER PRIMARY KEY, event_pattern TEXT, subscriber_type TEXT, subscriber_id TEXT,
		 run_id TEXT, flow_instance TEXT, source_flow TEXT, is_wildcard BOOLEAN,
		 is_materialized BOOLEAN, materialized_from INTEGER, status TEXT, created_at TIMESTAMP)`,
		`INSERT INTO routing_rules VALUES (1,'work.ready','node','receiver',NULL,NULL,'review',TRUE,FALSE,NULL,'active','2020-01-01')`,
		`INSERT INTO routing_rules VALUES (2,'work.done','node','receiver',NULL,NULL,'review',TRUE,FALSE,NULL,'active','2020-01-01')`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	runlifecyclefixture.RequireSQLite(t, context.Background(), db, runlifecyclefixture.Fixture{
		RunID: "run", Origin: runlifecyclefixture.ScenarioSetupOrigin(),
	})
	sets := make([]runtimebus.FlowInstanceRouteRecordSet, owners)
	for i := range sets {
		id := flowidentity.RunScopedFlowInstance{RunID: "run", Route: flowidentity.DeriveRoute("review", fmt.Sprintf("i%03d", i))}
		sets[i].Identity = id
		for _, pattern := range []string{"work.ready", "work.done"} {
			sets[i].Routes = append(sets[i].Routes, runtimebus.FlowInstanceRouteRecord{
				Identity: id, EventPattern: pattern, SubscriberType: "node", SubscriberID: "receiver", SourceFlow: "review",
			})
		}
	}
	return db, sets
}

func BenchmarkSQLiteRouteTopologyStatements(b *testing.B) {
	db, sets := sqliteRouteStatementFixture(b, 64)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets); err != nil {
		_ = tx.Rollback()
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		_, replaceErr := replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets)
		rollbackErr := tx.Rollback()
		if replaceErr != nil || rollbackErr != nil {
			b.Fatalf("replace=%v rollback=%v", replaceErr, rollbackErr)
		}
	}
}

func BenchmarkSQLiteRouteTopologyOneNew(b *testing.B) {
	for _, newIndex := range []int{0, 63} {
		b.Run(fmt.Sprintf("new_%d", newIndex), func(b *testing.B) {
			db, sets := sqliteRouteStatementFixture(b, 64)
			ctx := context.Background()
			seed := make([]runtimebus.FlowInstanceRouteRecordSet, 0, len(sets)-1)
			seed = append(seed, sets[:newIndex]...)
			seed = append(seed, sets[newIndex+1:]...)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, false, seed); err != nil {
				_ = tx.Rollback()
				b.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					b.Fatal(err)
				}
				_, replaceErr := replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets)
				rollbackErr := tx.Rollback()
				if replaceErr != nil || rollbackErr != nil {
					b.Fatalf("replace=%v rollback=%v", replaceErr, rollbackErr)
				}
			}
		})
	}
}
