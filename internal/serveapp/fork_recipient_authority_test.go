package serveapp

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Run lifecycle/freeze and fork lineage are intentionally outside this source
// domain snapshot: lawful fork admission owns those writes.
func readServedForkRecipientSourceDomain(t *testing.T, rt servedControlProofRuntime, runID string) map[string][]string {
	t.Helper()
	queries := map[string]string{
		"events":                                 `SELECT * FROM events WHERE run_id=$1`,
		"entity_state":                           `SELECT * FROM entity_state WHERE run_id=$1`,
		"entity_mutations":                       `SELECT * FROM entity_mutations WHERE run_id=$1`,
		"event_deliveries":                       `SELECT * FROM event_deliveries WHERE run_id=$1`,
		"event_receipts":                         `SELECT * FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1)`,
		"event_delivery_attempts":                `SELECT * FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`,
		"event_delivery_outcomes":                `SELECT * FROM event_delivery_outcomes WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`,
		"event_delivery_handler_rule_selections": `SELECT * FROM event_delivery_handler_rule_selections WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`,
	}
	out := make(map[string][]string, len(queries))
	for table, query := range queries {
		rows, err := rt.DB.Query(query, runID)
		if err != nil {
			t.Fatalf("read source %s: %v", table, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		out[table] = []string{}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = string(raw)
				}
			}
			raw, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			out[table] = append(out[table], string(raw))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(out[table])
	}
	return out
}

func requireServedForkConsumerAuthority(t *testing.T, rt servedControlProofRuntime, runID, eventID string) {
	t.Helper()
	deliveries := readServedForkDeliveryEvidence(t, rt, runID)
	matched := 0
	for id, delivery := range deliveries {
		if delivery.eventID != eventID {
			continue
		}
		matched++
		logServedForkWorkflowRows(t, rt, runID)
		routeJSON, err := json.Marshal(delivery.route)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("consumer target evidence run=%s event=%s delivery=%s route=%s", runID, eventID, id, routeJSON)
		node, isNode := delivery.route.Recipient.Node()
		handler, localEvent, hasHandler := delivery.route.ConnectClaim.NodeHandlerOwner()
		if !isNode || node.FlowPath() != "consumer" || node.NodeID() != "consumer-node" || !hasHandler || !handler.Equal(node) || localEvent != "work.ready" || delivery.route.Target.Route().FlowInstance != "consumer" {
			t.Fatalf("delivery %s lost exact consumer/handler/local-event authority: %#v", id, delivery.route)
		}
		var status, outcome, failure string
		var claimVersion, outcomeVersion int64
		if err := rt.DB.QueryRow(`SELECT d.status,d.claim_version,o.claim_version,o.outcome,COALESCE(CAST(o.failure AS TEXT),'') FROM event_deliveries d JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id WHERE d.delivery_id=$1 AND d.run_id=$2`, id, runID).Scan(&status, &claimVersion, &outcomeVersion, &outcome, &failure); err != nil {
			t.Fatalf("read exact consumer settlement: %v", err)
		}
		var outcomes int
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=$1`, id).Scan(&outcomes); err != nil {
			t.Fatal(err)
		}
		if status != "delivered" || outcome != "delivered" || claimVersion != 1 || outcomeVersion != claimVersion || outcomes != 1 {
			t.Logf("failed consumer entity-state rows run=%s: %v", runID, readServedForkRecipientSourceDomain(t, rt, runID)["entity_state"])
			t.Logf("failed consumer run=%s event=%s delivery=%s\n%s", runID, eventID, id, servedEventPublishDebugQuery(t, rt.DB, rt.Backend, "runtime_logs", runID))
			logServedForkHandlerDiagnostics(t, rt)
			t.Fatalf("consumer did not settle exactly once: status=%s outcome=%s claim=%d outcome_claim=%d outcomes=%d failure=%s", status, outcome, claimVersion, outcomeVersion, outcomes, failure)
		}
	}
	if matched != 1 {
		t.Fatalf("event %s has %d consumer deliveries, want exactly one", eventID, matched)
	}
}

func logServedForkWorkflowRows(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT instance_path,flow_template,mode,CAST(config AS TEXT),status FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`, runID)
	if err != nil {
		t.Logf("workflow diagnostic query failed: %v", err)
		return
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var path, template, mode, config, status string
		if err := rows.Scan(&path, &template, &mode, &config, &status); err != nil {
			t.Logf("workflow diagnostic scan failed: %v", err)
			return
		}
		count++
		t.Logf("workflow companion run=%s instance_path=%s flow_template=%s mode=%s status=%s config=%s", runID, path, template, mode, status, config)
	}
	if err := rows.Err(); err != nil {
		t.Logf("workflow diagnostic iteration failed: %v", err)
	}
	t.Logf("workflow companion census run=%s rows=%d", runID, count)
}

func logServedForkHandlerDiagnostics(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT event_id,COALESCE(CAST(run_id AS TEXT),''),CAST(payload AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at,event_id`)
	if err != nil {
		t.Logf("runtime diagnostic query failed: %v", err)
		return
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var eventID, runID, payload string
		if err := rows.Scan(&eventID, &runID, &payload); err != nil {
			t.Logf("runtime diagnostic scan failed: %v", err)
			return
		}
		count++
		t.Logf("runtime diagnostic event=%s run=%s payload=%s", eventID, runID, payload)
	}
	if err := rows.Err(); err != nil {
		t.Logf("runtime diagnostic iteration failed: %v", err)
	}
	t.Logf("runtime diagnostic census: %d rows, all runs including runless, no row limit", count)
}

func TestServedForkConnectedRecipientAuthorityOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkDeliveryRouteEvidence(t))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "parent.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"work_id": "fork-recipient-authority"}, "idempotency_key": "recipient-seed",
			})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			if selected == nil {
				t.Fatal("served runtime did not expose its selected persistence owner")
			}
			planner, ok := selected.RunFork()
			if !ok {
				t.Fatal("served runtime has no selected fork owner")
			}
			plan, err := planner.Plan(servedControlProofAuthorActivityContext(t, rt), runfork.RunForkPlanRequest{SourceRunID: seed.RunID})
			if err != nil {
				t.Fatal(err)
			}
			var frontierName string
			if err := rt.DB.QueryRow(`SELECT event_name FROM events WHERE run_id=$1 AND event_id=$2`, seed.RunID, plan.ForkPoint.EventID).Scan(&frontierName); err != nil {
				t.Fatal(err)
			}
			if frontierName != "producer/work.ready" {
				t.Fatalf("fixture frontier must exercise the final connected consumer, got %s", frontierName)
			}
			requireServedForkConsumerAuthority(t, rt, seed.RunID, plan.ForkPoint.EventID)
			t.Logf("source consumer settled before fork: source_run=%s source_event=%s", seed.RunID, plan.ForkPoint.EventID)
			before := readServedForkRecipientSourceDomain(t, rt, seed.RunID)
			t.Logf("source entity-state rows run=%s: %v", seed.RunID, before["entity_state"])
			checkSource := func() {
				t.Helper()
				for table, rows := range readServedForkRecipientSourceDomain(t, rt, seed.RunID) {
					if !reflect.DeepEqual(rows, before[table]) {
						t.Errorf("fork changed source %s: before=%v after=%v", table, before[table], rows)
					}
				}
			}
			defer checkSource()
			params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": plan.ForkPoint.EventID, "confirm_source_freeze": true, "idempotency_key": "recipient-fork"}
			var fork, replay apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ForkEventID != plan.ForkPoint.EventID || fork.ExecutedEventCount != 1 {
				t.Fatalf("fork lost exact source/child work identity: %+v", fork)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
			childEvent := activityidentity.ForkLineageEventID(fork.ForkRunID, plan.ForkPoint.EventID)
			t.Logf("checking fork child: source_run=%s fork_run=%s child_event=%s", seed.RunID, fork.ForkRunID, childEvent)
			requireServedForkConsumerAuthority(t, rt, fork.ForkRunID, childEvent)
			childBeforeRetry := readServedForkRecipientSourceDomain(t, rt, fork.ForkRunID)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
			if !reflect.DeepEqual(fork, replay) {
				t.Fatalf("same-key fork response changed: first=%+v replay=%+v", fork, replay)
			}
			if got := readServedForkRecipientSourceDomain(t, rt, fork.ForkRunID); !reflect.DeepEqual(got, childBeforeRetry) {
				t.Fatalf("same-key replay changed child domain: before=%v after=%v", childBeforeRetry, got)
			}
		})
	}
}
