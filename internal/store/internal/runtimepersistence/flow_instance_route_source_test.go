package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestFlowInstanceRouteSourceProjectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var db *sql.DB
			var selected interface {
				UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error
			}
			if backend == "postgres" {
				_, postgres, _ := testutil.StartPostgres(t)
				db, selected = postgres, admitTestPostgresStore(t, postgres)
			} else {
				sqlite := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, selected = sqlite.backend.ConstructionHandle(), sqlite
			}
			ctx = seedFlowRouteTestRun(t, ctx, db, backend == "postgres")
			route := runtimebus.FlowInstanceRouteRecord{
				Identity:     flowRouteTestIdentity(runtimeflowidentity.DeriveRoute("review", "source-proof")),
				EventPattern: "review/source-proof/input", SubscriberType: "node", SubscriberID: "receiver", SourceFlow: "review",
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO flow_instances
				(run_id,instance_path,flow_template,mode,config,status,created_at)
				VALUES ($1,$2,'review','template','{}','active',$3)`, flowRouteTestRunID, route.Identity.Route.InstancePath, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			insertSource := func(flow, status string, wildcard bool, age time.Duration) string {
				t.Helper()
				var id string
				if err := db.QueryRowContext(ctx, `INSERT INTO routing_rules
					(event_pattern,subscriber_type,subscriber_id,source_flow,is_wildcard,is_materialized,status,created_at)
					VALUES ($1,$2,$3,$4,$5,FALSE,$6,$7) RETURNING CAST(rule_id AS TEXT)`,
					route.EventPattern, route.SubscriberType, route.SubscriberID, flow, wildcard, status, time.Now().UTC().Add(age)).Scan(&id); err != nil {
					t.Fatal(err)
				}
				return id
			}
			check := func(want string) string {
				t.Helper()
				if err := selected.UpsertFlowInstanceRoute(ctx, route); err != nil {
					t.Fatal(err)
				}
				rows, err := db.QueryContext(ctx, `SELECT CAST(rule_id AS TEXT), CAST(materialized_from AS TEXT),status
					FROM routing_rules WHERE run_id=$1 AND flow_instance=$2 AND is_materialized=TRUE`, flowRouteTestRunID, route.Identity.Route.InstancePath)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				if !rows.Next() {
					t.Fatalf("missing materialized route: %v", rows.Err())
				}
				var id, status string
				var source sql.NullString
				if err := rows.Scan(&id, &source, &status); err != nil {
					t.Fatal(err)
				}
				if source.Valid != (want != "") || source.String != want || status != "active" || rows.Next() || rows.Err() != nil {
					t.Fatalf("route=%s source=%+v status=%s want source=%q rows error=%v", id, source, status, want, rows.Err())
				}
				return id
			}
			insertSource("other", "active", true, -4*time.Hour)
			insertSource("review", "inactive", true, -3*time.Hour)
			insertSource("review", "active", false, -2*time.Hour)
			original := check("")
			first := insertSource("review", "active", true, -time.Hour)
			second := insertSource("review", "active", true, -time.Minute)
			if got := check(first); got != original {
				t.Fatal("source update inserted a second materialized identity")
			}
			if _, err := db.ExecContext(ctx, `UPDATE routing_rules SET status='inactive' WHERE CAST(rule_id AS TEXT)=$1`, first); err != nil {
				t.Fatal(err)
			}
			if got := check(second); got != original {
				t.Fatal("source replacement changed materialized identity")
			}
			if _, err := db.ExecContext(ctx, `UPDATE routing_rules SET status='inactive' WHERE CAST(rule_id AS TEXT)=$1`, second); err != nil {
				t.Fatal(err)
			}
			if got := check(""); got != original {
				t.Fatal("source absence changed materialized identity")
			}
		})
	}
}
