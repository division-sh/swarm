package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Run lifecycle/freeze and fork lineage are intentionally outside this source
// domain snapshot: lawful fork admission owns those writes.
func readServedForkRecipientSourceDomain(t *testing.T, owner runtimebus.EventStore, runID string) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkSourceDomain(context.Background(), owner, runID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func requireServedForkConsumerAuthority(t *testing.T, rt servedControlProofRuntime, runID, eventID string) {
	t.Helper()
	deliveries := readServedForkDeliveryEvidence(t, rt.ReceiverStateReader, runID)
	matched := 0
	for id, delivery := range deliveries {
		if delivery.eventID != eventID {
			continue
		}
		matched++
		logServedForkWorkflowRows(t, rt.ReceiverStateReader, runID)
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
		if err := rt.DB.QueryRow(`SELECT d.status,d.claim_version,o.claim_version,o.outcome,COALESCE(CAST(o.failure AS TEXT),'') FROM event_deliveries d JOIN (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o ON o.delivery_id=d.delivery_id WHERE d.delivery_id=$1 AND d.run_id=$2`, id, runID).Scan(&status, &claimVersion, &outcomeVersion, &outcome, &failure); err != nil {
			t.Fatalf("read exact consumer settlement: %v", err)
		}
		var outcomes int
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') WHERE delivery_id=$1`, id).Scan(&outcomes); err != nil {
			t.Fatal(err)
		}
		if status != "delivered" || outcome != "delivered" || claimVersion != 1 || outcomeVersion != claimVersion || outcomes != 1 {
			t.Logf("failed consumer entity-state rows run=%s: %v", runID, readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), runID)["entity_state"])
			t.Logf("failed consumer run=%s event=%s delivery=%s\n%s", runID, eventID, id, servedInspectionRuntimeLogDebugSummary(rt.ReceiverStateReader, runID))
			logServedForkHandlerDiagnostics(t, rt.ReceiverStateReader)
			t.Fatalf("consumer did not settle exactly once: status=%s outcome=%s claim=%d outcome_claim=%d outcomes=%d failure=%s", status, outcome, claimVersion, outcomeVersion, outcomes, failure)
		}
	}
	if matched != 1 {
		t.Fatalf("event %s has %d consumer deliveries, want exactly one", eventID, matched)
	}
}

func logServedForkWorkflowRows(t *testing.T, selectedProofReceiverStateReader receiverProofStateReader, runID string) {
	t.Helper()
	rows, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (storetest.ServedForkWorkflowDiagnosticsStorage, error) {
		return storetest.ReadServedForkWorkflowDiagnosticsStorage(ctx, selectedProofReceiverStateReader, runID)
	})
	if err != nil {
		t.Logf("workflow diagnostic query failed: %v", err)
		return
	}
	count := 0
	for _, selectedReadRow := range rows.Rows {
		var path, template, mode, config, status string
		path = selectedReadRow.Path
		template = selectedReadRow.Template
		mode = selectedReadRow.Mode
		config = selectedReadRow.Config
		status = selectedReadRow.Status
		count++
		t.Logf("workflow companion run=%s instance_path=%s flow_template=%s mode=%s status=%s config=%s", runID, path, template, mode, status, config)
	}
	if err := rows.ScanError; err != nil {
		t.Logf("workflow diagnostic scan failed: %v", err)
		return
	}
	if err := rows.IterationError; err != nil {
		t.Logf("workflow diagnostic iteration failed: %v", err)
	}
	t.Logf("workflow companion census run=%s rows=%d", runID, count)
}

func logServedForkHandlerDiagnostics(t *testing.T, selectedProofReceiverStateReader receiverProofStateReader) {
	t.Helper()
	rows, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (storetest.ServedForkRuntimeDiagnosticsStorage, error) {
		return storetest.ReadServedForkRuntimeDiagnosticsStorage(ctx, selectedProofReceiverStateReader)
	})
	if err != nil {
		t.Logf("runtime diagnostic query failed: %v", err)
		return
	}
	count := 0
	for _, selectedReadRow := range rows.Rows {
		var eventID, runID, payload string
		eventID = selectedReadRow.EventID
		runID = selectedReadRow.RunID
		payload = selectedReadRow.Payload
		count++
		t.Logf("runtime diagnostic event=%s run=%s payload=%s", eventID, runID, payload)
	}
	if err := rows.ScanError; err != nil {
		t.Logf("runtime diagnostic scan failed: %v", err)
		return
	}
	if err := rows.IterationError; err != nil {
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
			publication := newExactOriginPublicationProbe(t)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkDeliveryRouteEvidence(t))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "parent.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"work_id": "fork-recipient-authority"}, "idempotency_key": "recipient-seed",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader, seed.RunID)
			if selected == nil {
				t.Fatal("served runtime did not expose its selected persistence owner")
			}
			planner, ok := selected.RunFork()
			if !ok {
				t.Fatal("served runtime has no selected fork owner")
			}
			plan, err := planner.Plan(servedControlProofAuthorActivityContext(t, rt.BundleHash, rt.Runtime), runfork.RunForkPlanRequest{SourceRunID: seed.RunID})
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
			if err := waitExactOriginPublications(t.Context(), publication, seed.EventID); err != nil {
				t.Fatal(err)
			}
			before := readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), seed.RunID)
			t.Logf("source entity-state rows run=%s: %v", seed.RunID, before["entity_state"])
			checkSource := func() {
				t.Helper()
				for table, rows := range readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), seed.RunID) {
					if !reflect.DeepEqual(rows, before[table]) {
						t.Errorf("fork changed source %s: before=%v after=%v", table, before[table], rows)
					}
				}
			}
			defer checkSource()
			params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": plan.ForkPoint.EventID, "allow_source_freeze": true, "idempotency_key": "recipient-fork"}
			var fork, replay apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ForkEventID != plan.ForkPoint.EventID || fork.ExecutedEventCount != 1 {
				t.Fatalf("fork lost exact source/child work identity: %+v", fork)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, fork.ForkRunID)
			childEvent := activityidentity.ForkLineageEventID(fork.ForkRunID, plan.ForkPoint.EventID)
			t.Logf("checking fork child: source_run=%s fork_run=%s child_event=%s", seed.RunID, fork.ForkRunID, childEvent)
			requireServedForkConsumerAuthority(t, rt, fork.ForkRunID, childEvent)
			childBeforeRetry := readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), fork.ForkRunID)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
			if !reflect.DeepEqual(fork, replay) {
				t.Fatalf("same-key fork response changed: first=%+v replay=%+v", fork, replay)
			}
			if got := readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), fork.ForkRunID); !reflect.DeepEqual(got, childBeforeRetry) {
				t.Fatalf("same-key replay changed child domain: before=%v after=%v", childBeforeRetry, got)
			}
		})
	}
}
