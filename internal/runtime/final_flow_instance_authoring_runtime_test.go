package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance(t *testing.T) {
	bundle := finalflowinstanceauthoring.LoadBundle(t, finalflowinstanceauthoring.Options{})
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("final flow-instance authoring hard invalidities = %#v, want none", got)
	}

	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	var manager *runtimemanager.AgentManager
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle: source,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return errors.New("agent manager not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	manager = runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkflowInstances: workflowStore,
	})
	module := newRuntimeTestWorkflowModule(t, source)
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
	})

	evt := eventtest.RootIngress(
		"99999999-9999-4999-8999-999999999955",
		events.EventType(finalflowinstanceauthoring.ProducerFlowID+"/"+finalflowinstanceauthoring.ProducerOutput),
		finalflowinstanceauthoring.ProducerFlowID,
		"",
		json.RawMessage(`{"account_id":"acct-42","score":"91","decision":"approved"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID: finalflowinstanceauthoring.ProducerFlowID, FlowInstance: finalflowinstanceauthoring.ProducerFlowID, EntityID: "88888888-8888-4888-8888-888888888888",
		}),
		time.Now().UTC(),
	)
	preflight, err := bus.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 || !preflight.UsesCanonicalRouteAuthority() {
		t.Fatalf("preflight failure/routes/canonical = %q/%#v/%v, want one canonical template route", preflight.TargetFailure, preflight.DeliveryRoutes, preflight.UsesCanonicalRouteAuthority())
	}
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM entity_state
		WHERE flow_instance LIKE 'account/%'
	`, 0)

	if err := bus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatalf("PublishAcknowledged final account.ready: %v", err)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		dumpFinalFlowInstanceAuthoringRuntimeProofState(t, ctx, db, evt.ID())
	})
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND outcome = 'no_op'
	`, 1, evt.ID(), finalflowinstanceauthoring.TemplateNodeID)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM entity_state
		WHERE flow_instance LIKE 'account/%'
		  AND current_state = 'reviewed'
		  AND fields @> $1::jsonb
	`, 1, `{"account_id":"acct-42","score":"91","decision":"approved"}`)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM entity_state
		WHERE flow_instance LIKE 'account/%'
	`, 1)

	flowInstance, entityID := loadFinalFlowInstanceAuthoringTemplateIdentity(t, ctx, db)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND status = 'delivered'
		  AND delivery_target_route @> $3::jsonb
	`, 1, evt.ID(), finalflowinstanceauthoring.TemplateNodeID, finalFlowInstanceAuthoringDeliveryTargetRouteJSON(t, events.RouteIdentity{
		FlowID:       finalflowinstanceauthoring.TemplateFlowID,
		FlowInstance: flowInstance,
		EntityID:     entityID,
	}))

	loaded, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.StorageRef != flowInstance || loaded.WorkflowName != finalflowinstanceauthoring.TemplateFlowID || loaded.CurrentState != "reviewed" {
		t.Fatalf("loaded account_case instance = storage:%q workflow:%q state:%q, want %s/%s/reviewed", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState, flowInstance, finalflowinstanceauthoring.TemplateFlowID)
	}
	if loaded.Metadata[finalflowinstanceauthoring.TemplateInstanceBy] != "acct-42" ||
		loaded.Metadata["score"] != "91" ||
		loaded.Metadata["decision"] != "approved" {
		t.Fatalf("loaded account_case metadata = %#v, want account_id/score/decision from selected routed payload", loaded.Metadata)
	}
}

func loadFinalFlowInstanceAuthoringTemplateIdentity(t *testing.T, ctx context.Context, db *sql.DB) (string, string) {
	t.Helper()
	var flowInstance string
	var entityID string
	if err := db.QueryRowContext(ctx, `
		SELECT flow_instance, entity_id::text
		FROM entity_state
		WHERE flow_instance LIKE 'account/%'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&flowInstance, &entityID); err != nil {
		t.Fatalf("load account_case instance identity: %v", err)
	}
	return flowInstance, entityID
}

func finalFlowInstanceAuthoringDeliveryTargetRouteJSON(t *testing.T, target events.RouteIdentity) string {
	t.Helper()
	encoded, err := json.Marshal(target.Normalized())
	if err != nil {
		t.Fatalf("marshal final flow-instance authoring delivery target: %v", err)
	}
	return strings.TrimSpace(string(encoded))
}

func dumpFinalFlowInstanceAuthoringRuntimeProofState(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT subscriber_type, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(failure::text, ''),
		       COALESCE(delivery_target_route::text, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		ORDER BY subscriber_type, subscriber_id
	`, eventID)
	if err != nil {
		t.Logf("event_deliveries diagnostic query failed: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var subscriberType, subscriberID, status, reason, failure, target string
		if err := rows.Scan(&subscriberType, &subscriberID, &status, &reason, &failure, &target); err != nil {
			t.Logf("event_deliveries diagnostic scan failed: %v", err)
			return
		}
		t.Logf("event_delivery: type=%s id=%s status=%s reason=%s failure=%s target=%s", subscriberType, subscriberID, status, reason, failure, target)
	}
	if err := rows.Err(); err != nil {
		t.Logf("event_deliveries diagnostic rows failed: %v", err)
	}

	receiptRows, err := db.QueryContext(ctx, `
		SELECT subscriber_type, subscriber_id, outcome, COALESCE(idempotency_key, ''), COALESCE(side_effects::text, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		ORDER BY subscriber_type, subscriber_id, idempotency_key
	`, eventID)
	if err != nil {
		t.Logf("event_receipts diagnostic query failed: %v", err)
		return
	}
	defer receiptRows.Close()
	for receiptRows.Next() {
		var subscriberType, subscriberID, outcome, idempotencyKey, sideEffects string
		if err := receiptRows.Scan(&subscriberType, &subscriberID, &outcome, &idempotencyKey, &sideEffects); err != nil {
			t.Logf("event_receipts diagnostic scan failed: %v", err)
			return
		}
		t.Logf("event_receipt: type=%s id=%s outcome=%s key=%s side_effects=%s", subscriberType, subscriberID, outcome, idempotencyKey, sideEffects)
	}
	if err := receiptRows.Err(); err != nil {
		t.Logf("event_receipts diagnostic rows failed: %v", err)
	}

	stateRows, err := db.QueryContext(ctx, `
		SELECT entity_id::text, flow_instance, current_state, fields::text
		FROM entity_state
		WHERE flow_instance LIKE 'account/%'
		ORDER BY created_at
	`)
	if err != nil {
		t.Logf("entity_state diagnostic query failed: %v", err)
		return
	}
	defer stateRows.Close()
	for stateRows.Next() {
		var entityID, flowInstance, currentState, fields string
		if err := stateRows.Scan(&entityID, &flowInstance, &currentState, &fields); err != nil {
			t.Logf("entity_state diagnostic scan failed: %v", err)
			return
		}
		t.Logf("entity_state: entity=%s flow_instance=%s state=%s fields=%s", entityID, flowInstance, currentState, fields)
	}
	if err := stateRows.Err(); err != nil {
		t.Logf("entity_state diagnostic rows failed: %v", err)
	}
}
