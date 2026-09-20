package pipelinepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/testutil"
)

type postgresRouteSeekProbe struct {
	*sql.Tx
	before bool
	query  string
	args   []any
}

func (p *postgresRouteSeekProbe) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if p.before {
		query = strings.Replace(query, "AND flow_instance = $5", "AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')", 1)
	}
	p.query, p.args = query, args
	return p.Tx.ExecContext(ctx, query, args...)
}

func TestPostgresRouteUpdateExactInstanceSeekPreservesRows(t *testing.T) {
	_, db, _ := testutil.StartEmptyPostgres(t)
	const runID = "00000000-0000-4000-8000-000000000001"
	for _, query := range []string{
		`CREATE TABLE routing_rules(rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		 event_pattern TEXT,subscriber_type TEXT,subscriber_id TEXT,run_id UUID,flow_instance TEXT,
		 source_flow TEXT,is_wildcard BOOLEAN DEFAULT FALSE,is_materialized BOOLEAN,
		 materialized_from UUID,status TEXT,created_at TIMESTAMPTZ DEFAULT NOW())`,
		`CREATE INDEX idx_routing_subscriber ON routing_rules(run_id,subscriber_id)`,
		`CREATE INDEX idx_routing_flow ON routing_rules(run_id,flow_instance) WHERE flow_instance IS NOT NULL`,
		`INSERT INTO routing_rules(event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_materialized,status)
		 SELECT 'work.ready','node','receiver','` + runID + `', 'review/i'||i,'old',TRUE,'inactive' FROM generate_series(0,1023) i`,
		`INSERT INTO routing_rules(event_pattern,subscriber_type,subscriber_id,run_id,flow_instance,source_flow,is_materialized,status)
		 SELECT 'work.ready','node','receiver',r::uuid,p,'old',m,'inactive'
		 FROM unnest(ARRAY['` + runID + `','00000000-0000-4000-8000-000000000002']) r,
		 unnest(ARRAY[NULL::text,'','review/i0']) p,unnest(ARRAY[TRUE,FALSE]) m`,
		`ANALYZE routing_rules`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	route := runtimebus.FlowInstanceRouteRecord{
		Identity:     flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.DeriveRoute("review", "i0")},
		EventPattern: "work.ready", SubscriberType: "node", SubscriberID: "receiver", SourceFlow: "review",
	}
	var baseline []string
	for _, before := range []bool{true, false} {
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		p := &postgresRouteSeekProbe{Tx: tx, before: before}
		if err := upsertPostgresFlowInstanceRoute(context.Background(), p, route); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		rows, err := tx.Query(`SELECT rule_id::text FROM routing_rules WHERE source_flow='review' AND status='active' ORDER BY rule_id`)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		var selected []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			selected = append(selected, id)
		}
		readErr := rows.Err()
		closeErr := rows.Close()
		if readErr != nil || closeErr != nil || len(selected) != 2 {
			_ = tx.Rollback()
			t.Fatalf("selected=%v read=%v close=%v", selected, readErr, closeErr)
		}
		if before {
			baseline = selected
		} else {
			if !reflect.DeepEqual(selected, baseline) {
				t.Fatalf("selected=%v want=%v", selected, baseline)
			}
			plans, err := tx.Query("EXPLAIN "+p.query, p.args...)
			if err != nil {
				t.Fatal(err)
			}
			var details []string
			for plans.Next() {
				var detail string
				if err := plans.Scan(&detail); err != nil {
					t.Fatal(err)
				}
				details = append(details, detail)
			}
			readErr, closeErr = plans.Err(), plans.Close()
			if plan := strings.Join(details, "\n"); readErr != nil || closeErr != nil || !strings.Contains(plan, "idx_routing_flow") {
				t.Fatalf("exact instance index absent: read=%v close=%v plan=%s", readErr, closeErr, plan)
			}
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}
