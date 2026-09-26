package pipelinepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestRouteTopologyExactDiffUsesSelectedRowsBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			var db *sql.DB
			var sets []runtimebus.FlowInstanceRouteRecordSet
			if postgres {
				db, sets = postgresRouteStatementFixture(t, 2, false)
			} else {
				db, sets = sqliteRouteStatementFixture(t, 2)
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, postgres, sets); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			before := selectedRouteRows(t, db, postgres)
			installRejectMaterializedRouteWrite(t, db, postgres)
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, postgres, sets); err != nil || !reflect.DeepEqual(got, sets) {
				_ = tx.Rollback()
				t.Fatalf("exact selected-store comparison: result=%v err=%v", got, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if after := selectedRouteRows(t, db, postgres); !reflect.DeepEqual(after, before) {
				t.Fatalf("exact comparison changed selected rows: before=%v after=%v", before, after)
			}

			removeRejectMaterializedRouteWrite(t, db, postgres)
			query := `UPDATE routing_rules SET source_flow='wrong' WHERE flow_instance='review/i001' AND event_pattern='work.ready' AND is_materialized=TRUE`
			if _, err := db.ExecContext(ctx, query); err != nil {
				t.Fatal(err)
			}
			corrupt := selectedRouteRows(t, db, postgres)
			installRejectMaterializedRouteWrite(t, db, postgres)
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, postgres, sets)
			if got != nil || err == nil || !strings.Contains(err.Error(), "route write refused") {
				_ = tx.Rollback()
				t.Fatalf("corrupt selected row bypassed hostile write: result=%v err=%v", got, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if after := selectedRouteRows(t, db, postgres); !reflect.DeepEqual(after, corrupt) {
				t.Fatalf("failed replacement changed selected rows: before=%v after=%v", corrupt, after)
			}

			removeRejectMaterializedRouteWrite(t, db, postgres)
			if _, err := db.ExecContext(ctx, `UPDATE routing_rules SET source_flow='review' WHERE flow_instance='review/i001' AND event_pattern='work.ready' AND is_materialized=TRUE`); err != nil {
				t.Fatal(err)
			}
			runID := "run"
			if postgres {
				runID = postgresStatementRunID
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO routing_rules(event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_wildcard,is_materialized,materialized_from,status,created_at)
				VALUES('unexpected','node','receiver',$1,'review/i001','review',FALSE,TRUE,NULL,'active','2020-01-03')`, runID); err != nil {
				t.Fatal(err)
			}
			extra := selectedRouteRows(t, db, postgres)
			installRejectMaterializedRouteWrite(t, db, postgres)
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err = replaceFlowInstanceRouteTopologyTx(ctx, tx, postgres, sets)
			if got != nil || err == nil || !strings.Contains(err.Error(), "route write refused") {
				_ = tx.Rollback()
				t.Fatalf("extra active selected row bypassed hostile write: result=%v err=%v", got, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if after := selectedRouteRows(t, db, postgres); !reflect.DeepEqual(after, extra) {
				t.Fatalf("failed extra-route replacement changed selected rows: before=%v after=%v", extra, after)
			}
		})
	}
}

func TestRouteTopologySelectedEvidenceReadFailureRollsBackBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, column := range []string{"flow_instance", "created_at"} {
			name := "sqlite/" + column
			if postgres {
				name = "postgres/" + column
			}
			t.Run(name, func(t *testing.T) {
				var db *sql.DB
				var sets []runtimebus.FlowInstanceRouteRecordSet
				if postgres {
					db, sets = postgresRouteStatementFixture(t, 1, false)
				} else {
					db, sets = sqliteRouteStatementFixture(t, 1)
				}
				before := selectedRouteRows(t, db, postgres)
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.ExecContext(context.Background(), `ALTER TABLE routing_rules RENAME COLUMN `+column+` TO hidden_`+column); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				got, err := replaceFlowInstanceRouteTopologyTx(context.Background(), tx, postgres, sets)
				if got != nil || err == nil || !strings.Contains(err.Error(), "read selected") && !strings.Contains(err.Error(), "read current wildcard") {
					_ = tx.Rollback()
					t.Fatalf("missing selected evidence did not fail closed: result=%v err=%v", got, err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if after := selectedRouteRows(t, db, postgres); !reflect.DeepEqual(after, before) {
					t.Fatalf("failed evidence read changed rows: before=%v after=%v", before, after)
				}
			})
		}
	}
}

func TestRouteTopologyWildcardAndInactiveDifferentialBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			wantWrite bool
		}{
			{"missing_winner_stale_provenance", true},
			{"missing_winner_exact_null", false},
			{"ambiguous_winner", true},
			{"inactive_matching_route", true},
			{"inactive_unrelated_route", false},
		} {
			name := "sqlite/" + tc.name
			if postgres {
				name = "postgres/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				var db *sql.DB
				var sets []runtimebus.FlowInstanceRouteRecordSet
				if postgres {
					_, db, _ = testutil.StartEmptyPostgres(t)
					sets = populatePostgresRouteStatementFixture(t, db, 1, false, false)
				} else {
					db, sets = sqliteRouteStatementFixture(t, 1)
				}
				ctx := context.Background()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, postgres, sets); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				var mutation string
				switch tc.name {
				case "missing_winner_stale_provenance", "missing_winner_exact_null":
					mutation = `UPDATE routing_rules SET status='inactive' WHERE run_id IS NULL AND event_pattern='work.ready' AND is_wildcard=TRUE`
				case "ambiguous_winner":
					if postgres {
						mutation = `INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,source_flow,is_wildcard,is_materialized,status,created_at)
							VALUES('00000000-0000-4000-8000-000000000014','work.ready','node','receiver','review',TRUE,FALSE,'active','2020-01-01')`
					} else {
						mutation = `INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,source_flow,is_wildcard,is_materialized,status,created_at)
							VALUES(100,'work.ready','node','receiver','review',TRUE,FALSE,'active','2020-01-01')`
					}
				case "inactive_matching_route":
					mutation = `UPDATE routing_rules SET status='inactive' WHERE flow_instance='review/i000' AND event_pattern='work.ready' AND is_materialized=TRUE`
				case "inactive_unrelated_route":
					if postgres {
						mutation = `INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_wildcard,is_materialized,status,created_at)
							VALUES('00000000-0000-4000-8000-000000000101','unexpected','node','receiver','` + postgresStatementRunID + `','review/i000','review',FALSE,TRUE,'inactive','2020-01-03')`
					} else {
						mutation = `INSERT INTO routing_rules(rule_id,event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_wildcard,is_materialized,status,created_at)
							VALUES(101,'unexpected','node','receiver','run','review/i000','review',FALSE,TRUE,'inactive','2020-01-03')`
					}
				}
				if _, err := db.ExecContext(ctx, mutation); err != nil {
					t.Fatal(err)
				}
				if tc.name == "missing_winner_exact_null" {
					if _, err := db.ExecContext(ctx, `UPDATE routing_rules SET materialized_from=NULL WHERE flow_instance='review/i000' AND event_pattern='work.ready' AND is_materialized=TRUE`); err != nil {
						t.Fatal(err)
					}
				}
				before := selectedRouteRows(t, db, postgres)
				installRejectMaterializedRouteWrite(t, db, postgres)
				tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				got, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, postgres, sets)
				if tc.wantWrite {
					if got != nil || err == nil || !strings.Contains(err.Error(), "route write refused") {
						_ = tx.Rollback()
						t.Fatalf("changed selected state did not require write: result=%v err=%v", got, err)
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				} else {
					if err != nil || !reflect.DeepEqual(got, sets) {
						_ = tx.Rollback()
						t.Fatalf("semantically exact selected state caused write: result=%v err=%v", got, err)
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
				if after := selectedRouteRows(t, db, postgres); !reflect.DeepEqual(after, before) {
					t.Fatalf("comparison changed selected rows: before=%v after=%v", before, after)
				}
			})
		}
	}
}

func selectedRouteRows(t *testing.T, db *sql.DB, postgres bool) any {
	t.Helper()
	if postgres {
		return postgresRouteStrings(t, db, postgresAllRouteRows)
	}
	return sqliteRouteRows(t, db)
}

func installRejectMaterializedRouteWrite(t *testing.T, db *sql.DB, postgres bool) {
	t.Helper()
	queries := []string{
		`CREATE TRIGGER reject_materialized_route_update BEFORE UPDATE ON routing_rules
		 WHEN NEW.is_materialized BEGIN SELECT RAISE(ABORT,'route write refused'); END`,
		`CREATE TRIGGER reject_materialized_route_insert BEFORE INSERT ON routing_rules
		 WHEN NEW.is_materialized BEGIN SELECT RAISE(ABORT,'route write refused'); END`,
	}
	if postgres {
		queries = []string{
			`CREATE FUNCTION reject_materialized_route_write() RETURNS trigger LANGUAGE plpgsql AS $$
			 BEGIN IF NEW.is_materialized THEN RAISE EXCEPTION 'route write refused'; END IF;
			 RETURN NEW; END $$`,
			`CREATE TRIGGER reject_materialized_route_write BEFORE INSERT OR UPDATE ON routing_rules
			 FOR EACH ROW EXECUTE FUNCTION reject_materialized_route_write()`,
		}
	}
	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
}

func removeRejectMaterializedRouteWrite(t *testing.T, db *sql.DB, postgres bool) {
	t.Helper()
	queries := []string{
		`DROP TRIGGER reject_materialized_route_insert`,
		`DROP TRIGGER reject_materialized_route_update`,
	}
	if postgres {
		queries = []string{
			`DROP TRIGGER reject_materialized_route_write ON routing_rules`,
			`DROP FUNCTION reject_materialized_route_write()`,
		}
	}
	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
}
