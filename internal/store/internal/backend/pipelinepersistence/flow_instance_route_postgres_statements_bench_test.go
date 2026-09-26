package pipelinepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/testpostgres"
)

func BenchmarkPostgresRouteTopologyStatements(b *testing.B) {
	benchmarkPostgresRouteTopologyStatements(b, 64)
}

func BenchmarkPostgresRouteTopologyStatementsSingleOwner(b *testing.B) {
	benchmarkPostgresRouteTopologyStatements(b, 1)
}

func benchmarkPostgresRouteTopologyStatements(b *testing.B, owners int) {
	ctx := context.Background()
	setupCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	manager, err := testpostgres.ManagerFromEnvironment(setupCtx)
	if err != nil {
		b.Fatal(err)
	}
	for _, state := range []string{"insert", "existing", "one_new_first", "one_new_last"} {
		b.Run(state, func(b *testing.B) {
			for _, mode := range []string{"direct", "reuse"} {
				b.Run(mode, func(b *testing.B) {
					setupCtx, cancel := context.WithTimeout(ctx, time.Minute)
					defer cancel()
					sandbox, err := manager.Acquire(setupCtx, false)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
						defer cancel()
						if err := sandbox.Release(ctx); err != nil {
							b.Error(err)
						}
					})
					db := sandbox.DB
					sets := populatePostgresRouteStatementFixture(b, db, owners, false, false)
					// No audit/provenance-changing triggers in the timed workload;
					// use the real random-key default rather than differential keys.
					for _, ddl := range []string{
						`DROP TRIGGER assign_route_id ON routing_rules`,
						`ALTER TABLE routing_rules ALTER COLUMN rule_id SET DEFAULT gen_random_uuid()`,
						`CREATE INDEX idx_routing_subscriber ON routing_rules(run_id,subscriber_id)`,
					} {
						if _, err := db.Exec(ddl); err != nil {
							b.Fatal(err)
						}
					}
					if state != "insert" {
						tx, err := db.BeginTx(ctx, nil)
						if err != nil {
							b.Fatal(err)
						}
						seed := sets
						switch state {
						case "one_new_first":
							seed = sets[1:]
						case "one_new_last":
							seed = sets[:len(sets)-1]
						}
						if _, err := postgresRouteTopologyDirect(ctx, tx, seed); err != nil {
							_ = tx.Rollback()
							b.Fatal(err)
						}
						if err := tx.Commit(); err != nil {
							b.Fatal(err)
						}
					}
					if _, err := db.Exec(`ANALYZE routing_rules`); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						tx, err := db.BeginTx(ctx, nil)
						if err != nil {
							b.Fatal(err)
						}
						var mutationErr error
						if mode == "direct" {
							_, mutationErr = postgresRouteTopologyDirect(ctx, tx, sets)
						} else {
							_, mutationErr = replaceFlowInstanceRouteTopologyTx(ctx, tx, true, sets)
						}
						rollbackErr := tx.Rollback()
						if mutationErr != nil || rollbackErr != nil {
							b.Fatalf("replace=%v rollback=%v", mutationErr, rollbackErr)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(owners), "owners/op")
					b.ReportMetric(float64(2*owners), "routes/op")
				})
			}
		})
	}
}

func BenchmarkRouteTopologyOneNewAt1024(b *testing.B) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		b.Run(name, func(b *testing.B) {
			var sets []runtimebus.FlowInstanceRouteRecordSet
			var replaceErr error
			ctx := context.Background()
			var db *sql.DB
			if postgres {
				manager, err := testpostgres.ManagerFromEnvironment(ctx)
				if err != nil {
					b.Fatal(err)
				}
				sandbox, err := manager.Acquire(ctx, false)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					if err := sandbox.Release(cleanupCtx); err != nil {
						b.Error(err)
					}
				})
				db = sandbox.DB
				sets = populatePostgresRouteStatementFixture(b, db, 1024, false, false)
			} else {
				db, sets = sqliteRouteStatementFixture(b, 1024)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}
			if postgres {
				_, replaceErr = postgresRouteTopologyDirect(ctx, tx, sets[:len(sets)-1])
			} else {
				_, replaceErr = replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets[:len(sets)-1])
			}
			if replaceErr != nil {
				_ = tx.Rollback()
				b.Fatal(replaceErr)
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
				_, replaceErr := replaceFlowInstanceRouteTopologyTx(ctx, tx, postgres, sets)
				rollbackErr := tx.Rollback()
				if replaceErr != nil || rollbackErr != nil {
					b.Fatalf("replace=%v rollback=%v", replaceErr, rollbackErr)
				}
			}
			b.StopTimer()
			b.ReportMetric(1024, "owners/op")
			b.ReportMetric(2048, "routes/op")
		})
	}
}
