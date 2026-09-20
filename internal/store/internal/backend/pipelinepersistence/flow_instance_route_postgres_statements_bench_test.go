package pipelinepersistence

import (
	"context"
	"testing"
	"time"

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
	for _, state := range []string{"insert", "existing"} {
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
					if state == "existing" {
						tx, err := db.BeginTx(ctx, nil)
						if err != nil {
							b.Fatal(err)
						}
						if _, err := postgresRouteTopologyDirect(ctx, tx, sets); err != nil {
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
