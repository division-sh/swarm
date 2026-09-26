package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/lib/pq"
)

func postgresRouteStatementFixture(t *testing.T, owners int, existing bool) (*sql.DB, []runtimebus.FlowInstanceRouteRecordSet) {
	t.Helper()
	_, db, _ := testutil.StartEmptyPostgres(t)
	return db, populatePostgresRouteStatementFixture(t, db, owners, existing, true)
}

func populatePostgresRouteStatementFixture(t testing.TB, db *sql.DB, owners int, existing, audit bool) []runtimebus.FlowInstanceRouteRecordSet {
	t.Helper()
	// Cancellation observes the server barrier through an independent session.
	db.SetMaxOpenConns(2)
	for _, query := range []string{
		`CREATE TABLE runs (run_id UUID PRIMARY KEY, status TEXT, bundle_hash TEXT,
		 origin_kind TEXT, trigger_event_id UUID, trigger_event_type TEXT,
		 origin_service_id UUID, origin_generation BIGINT, forked_from_run_id UUID,
		 forked_from_event_id UUID, started_at TIMESTAMPTZ)`,
		`CREATE TABLE source_artifacts (bundle_hash TEXT PRIMARY KEY, source_blob BYTEA,
		 member_count INTEGER, total_bytes BIGINT, created_at TIMESTAMPTZ)`,
		`CREATE TABLE routing_rules (rule_id UUID PRIMARY KEY,
		 event_pattern TEXT, subscriber_type TEXT, subscriber_id TEXT, run_id UUID,
		 flow_instance TEXT, source_flow TEXT, is_wildcard BOOLEAN, is_materialized BOOLEAN,
		 materialized_from UUID, status TEXT, created_at TIMESTAMPTZ)`,
		`CREATE INDEX idx_routing_flow ON routing_rules(run_id,flow_instance) WHERE flow_instance IS NOT NULL`,
		// Reproducible generated keys permit full-row comparison after savepoint
		// rollback; both arms also share the exact same transaction-time NOW().
		`CREATE FUNCTION assign_route_id() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN
		 IF NEW.rule_id IS NULL THEN
		 NEW.rule_id := md5(NEW.run_id::text || NEW.flow_instance || NEW.event_pattern)::uuid;
		 END IF; RETURN NEW; END $$`,
		`CREATE TRIGGER assign_route_id BEFORE INSERT ON routing_rules FOR EACH ROW EXECUTE FUNCTION assign_route_id()`,
		`INSERT INTO routing_rules VALUES
		 ('00000000-0000-4000-8000-000000000011','work.done','node','receiver',NULL,NULL,'review',TRUE,FALSE,NULL,'active','2020-01-01'),
		 ('00000000-0000-4000-8000-000000000012','work.ready','node','receiver',NULL,NULL,'review',TRUE,FALSE,NULL,'active','2020-01-01'),
		 ('00000000-0000-4000-8000-000000000013','work.ready','node','receiver',NULL,NULL,'review',TRUE,FALSE,NULL,'inactive','2020-01-02')`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	runlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{
		RunID: postgresStatementRunID, Origin: runlifecyclefixture.ScenarioSetupOrigin(),
	})
	sets := postgresStatementRouteSets(owners)
	if existing {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		for i, set := range sets {
			if i%2 != 0 {
				continue
			}
			for _, route := range set.Routes {
				if err := upsertPostgresFlowInstanceRoute(context.Background(), tx, route); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if !audit {
		return sets
	}
	triggerOwner := "review/i000"
	if existing {
		triggerOwner = "review/i001" // i000 is already exact and must not be rewritten.
	}
	for _, query := range []string{
		`CREATE TABLE route_audit (seq BIGSERIAL, mutation JSONB, handles INTEGER, generic BIGINT, custom BIGINT)`,
		`CREATE FUNCTION audit_route() RETURNS trigger LANGUAGE plpgsql AS $$
		 DECLARE handles INTEGER; generic BIGINT; custom BIGINT;
		 BEGIN
		 SELECT COUNT(*),COALESCE(SUM(generic_plans),0),COALESCE(SUM(custom_plans),0)
		 INTO handles,generic,custom FROM pg_prepared_statements
		 WHERE statement LIKE '%WITH source AS MATERIALIZED%' OR statement LIKE '%SET status = ''inactive''%';
		 INSERT INTO route_audit(mutation,handles,generic,custom)
		 VALUES(jsonb_build_object('op',TG_OP,'old',to_jsonb(OLD),'new',to_jsonb(NEW)),handles,generic,custom);
		 RETURN NEW; END $$`,
		`CREATE TRIGGER a_audit_route AFTER INSERT OR UPDATE ON routing_rules FOR EACH ROW EXECUTE FUNCTION audit_route()`,
		// A later upsert must reread the changed wildcard even with a prepared
		// handle. Auditing includes both source changes and materialized writes.
		fmt.Sprintf(`CREATE FUNCTION change_route_source() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN
		 IF NEW.is_materialized AND NEW.status='active' AND NEW.flow_instance='%s' AND NEW.event_pattern='work.done' THEN
		 UPDATE routing_rules SET status='inactive' WHERE rule_id='00000000-0000-4000-8000-000000000012';
		 UPDATE routing_rules SET status='active' WHERE rule_id='00000000-0000-4000-8000-000000000013';
		 END IF; RETURN NEW; END $$`, triggerOwner),
		`CREATE TRIGGER b_change_route_source AFTER INSERT OR UPDATE ON routing_rules FOR EACH ROW EXECUTE FUNCTION change_route_source()`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return sets
}

func postgresRouteStrings(t *testing.T, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string) []string {
	t.Helper()
	rows, err := q.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

const postgresAllRouteRows = `SELECT to_jsonb(r)::text FROM routing_rules r ORDER BY rule_id`

func TestPostgresRouteTopologyStatementsNativeDifferential(t *testing.T) {
	for _, mode := range []string{"auto", "force_generic_plan"} {
		for _, existing := range []bool{false, true} {
			name := mode + "/insert"
			if existing {
				name = mode + "/mixed_update_insert"
			}
			t.Run(name, func(t *testing.T) {
				db, sets := postgresRouteStatementFixture(t, 64, existing)
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.Exec("SET LOCAL plan_cache_mode = " + mode); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec("SAVEPOINT direct_loop"); err != nil {
					t.Fatal(err)
				}
				want, err := postgresRouteTopologyDirect(context.Background(), tx, sets)
				if err != nil {
					t.Fatal(err)
				}
				wantRows := postgresRouteStrings(t, tx, postgresAllRouteRows)
				wantAudit := postgresRouteStrings(t, tx, `SELECT mutation::text FROM route_audit ORDER BY seq`)
				if _, err := tx.Exec("ROLLBACK TO direct_loop"); err != nil {
					t.Fatal(err)
				}
				got, err := replaceFlowInstanceRouteTopologyTx(context.Background(), tx, true, sets)
				if err != nil || !reflect.DeepEqual(want, got) {
					t.Fatalf("normalized topology differs: err=%v", err)
				}
				if rows := postgresRouteStrings(t, tx, postgresAllRouteRows); !reflect.DeepEqual(rows, wantRows) {
					t.Fatalf("full rows/keys/timestamps/provenance differ: got=%v want=%v", rows, wantRows)
				}
				if audit := postgresRouteStrings(t, tx, `SELECT mutation::text FROM route_audit ORDER BY seq`); !existing && !reflect.DeepEqual(audit, wantAudit) {
					t.Fatalf("ordered mutations differ: got=%v want=%v", audit, wantAudit)
				}
				if existing {
					var redundantWrites int
					if err := tx.QueryRow(`SELECT COUNT(*) FROM route_audit WHERE mutation->>'op'='UPDATE' AND mutation->'new'->>'flow_instance'='review/i000'`).Scan(&redundantWrites); err != nil || redundantWrites != 0 {
						t.Fatalf("exact owner was rewritten: count=%d err=%v", redundantWrites, err)
					}
				}
				var source string
				observedOwner := "review/i001"
				if existing {
					observedOwner = "review/i002"
				}
				if err := tx.QueryRow(`SELECT materialized_from::text FROM routing_rules WHERE flow_instance=$1 AND event_pattern='work.ready'`, observedOwner).Scan(&source); err != nil || source != "00000000-0000-4000-8000-000000000013" {
					t.Fatalf("stale source: source=%s err=%v", source, err)
				}
				var handles, generic, custom int64
				if err := tx.QueryRow(`SELECT MAX(handles),MAX(generic),MAX(custom) FROM route_audit`).Scan(&handles, &generic, &custom); err != nil || handles != 2 || generic+custom <= 5 || (mode == "force_generic_plan" && generic <= 5) {
					t.Fatalf("native plan reuse: handles=%d generic=%d custom=%d err=%v", handles, generic, custom, err)
				}
				var remaining int
				if err := tx.QueryRow(`SELECT COUNT(*) FROM pg_prepared_statements`).Scan(&remaining); err != nil || remaining != 0 {
					t.Fatalf("handles survived invocation: remaining=%d err=%v", remaining, err)
				}
				t.Logf("64 owners/128 routes: exact rows and fresh triggered provenance; peak handles=%d generic=%d custom=%d; zero handles on return", handles, generic, custom)
			})
		}
	}
}

func TestPostgresRouteTopologyStatementsNativeRollbackAndCancel(t *testing.T) {
	for _, failure := range []string{"insert", "update", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			db, sets := postgresRouteStatementFixture(t, 2, failure == "update")
			if failure == "update" {
				// The old source is no longer authoritative, so an exact-diff
				// replacement must execute the hostile UPDATE.
				if _, err := db.Exec(`UPDATE routing_rules SET status='inactive' WHERE rule_id='00000000-0000-4000-8000-000000000012'`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`UPDATE routing_rules SET status='active' WHERE rule_id='00000000-0000-4000-8000-000000000013'`); err != nil {
					t.Fatal(err)
				}
			}
			before := postgresRouteStrings(t, db, postgresAllRouteRows)
			for _, ddl := range []string{
				`CREATE SEQUENCE route_fault_entered`,
				`CREATE FUNCTION refuse_route() RETURNS trigger LANGUAGE plpgsql AS $$
				 BEGIN
				 IF NEW.is_materialized AND NEW.status='active' AND NEW.flow_instance='review/i000' AND NEW.event_pattern='work.ready' THEN
				 PERFORM nextval('route_fault_entered');
				 IF current_setting('swarm.route_cancel',TRUE) = 'yes' THEN
				 PERFORM pg_sleep(30);
				 ELSE RAISE EXCEPTION 'later route refused'; END IF;
				 END IF; RETURN NEW; END $$`,
				`CREATE TRIGGER refuse_route BEFORE INSERT OR UPDATE ON routing_rules FOR EACH ROW EXECUTE FUNCTION refuse_route()`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "cancel" {
				if _, err := tx.Exec(`SET LOCAL swarm.route_cancel = 'yes'`); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				_, err := replaceFlowInstanceRouteTopologyTx(ctx, tx, true, sets)
				done <- err
			}()
			if failure == "cancel" {
				// The nontransactional sequence proves we reached the second
				// mutation after earlier writes, rather than canceling preparation.
				waitCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				for {
					var entered bool
					if err := db.QueryRowContext(waitCtx, `SELECT is_called FROM route_fault_entered`).Scan(&entered); err != nil {
						cancel()
						<-done
						t.Fatal(err)
					}
					if entered {
						cancel()
						break
					}
					time.Sleep(time.Millisecond)
				}
			}
			err = <-done
			if err == nil || !strings.Contains(err.Error(), "upsert flow instance route") {
				t.Fatalf("lost mutation error: %v", err)
			}
			if failure == "cancel" {
				var pgErr *pq.Error
				if !errors.Is(err, context.Canceled) && !(errors.As(err, &pgErr) && pgErr.Code == "57014") {
					t.Fatalf("lost cancellation: %v", err)
				}
			} else if !strings.Contains(err.Error(), "later route refused") {
				t.Fatalf("lost hostile mutation error: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if rows := postgresRouteStrings(t, db, postgresAllRouteRows); !reflect.DeepEqual(rows, before) {
				t.Fatalf("rollback changed rows/keys/timestamps/provenance: got=%v want=%v", rows, before)
			}
		})
	}
}
