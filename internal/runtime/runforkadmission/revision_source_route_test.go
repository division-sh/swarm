package runforkadmission

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	pendingEventID := uuid.NewString()
	completedEventID := uuid.NewString()
	sourceEntityID := uuid.NewString()
	targetEntityID := uuid.NewString()
	at := time.Unix(1700001000, 0).UTC()
	sourceRoute := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: sourceEntityID}
	targetRoute := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/inst-9", EntityID: targetEntityID}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			execution_mode, run_id, event_id, event_name, entity_id, flow_instance,
			source_route, target_route, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			('live', $1::uuid, $2::uuid, 'producer/inst-1/scan.requested', $4::uuid, 'consumer/inst-9', $5::jsonb, $6::jsonb, 'flow', '{}'::jsonb, 'producer-node', 'node', $7),
			('live', $1::uuid, $3::uuid, 'producer/inst-1/scan.requested', $4::uuid, 'consumer/inst-9', $5::jsonb, $6::jsonb, 'flow', '{}'::jsonb, 'producer-node', 'node', $8)
	`, runID, pendingEventID, completedEventID, targetEntityID, mustJSON(t, sourceRoute), mustJSON(t, targetRoute), at, at.Add(time.Second)); err != nil {
		t.Fatalf("seed routed events: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id,
			status, retry_count, reason_code, delivered_at, created_at
		)
		VALUES
			($1::uuid, $2::uuid, $3::uuid, 'node', 'pending-source-node', 'pending', 0, 'matched_node_subscription', NULL, $5),
			($4::uuid, $2::uuid, $6::uuid, 'node', 'completed-source-node', 'delivered', 0, 'ok', $7, $7)
	`, uuid.NewString(), runID, pendingEventID, uuid.NewString(), at, completedEventID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		)
		VALUES ($1::uuid, 'node', 'completed-source-node', 'success', 'ok', '{}'::jsonb, $2)
	`, completedEventID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed completed receipt: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin revision capture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := runforkrevision.Capture(ctx, tx, runID, runforkrevision.AllFamilies()...); err != nil {
		t.Fatalf("capture revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit revision: %v", err)
	}

	plan, err := (&store.PostgresStore{DB: db}).PlanRunFork(ctx, store.RunForkPlanRequest{
		SourceRunID: runID,
		At:          completedEventID,
	})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if len(plan.PendingWork) != 2 {
		t.Fatalf("pending work = %#v, want pending and completed revision rows", plan.PendingWork)
	}
	for _, item := range plan.PendingWork {
		if item.SourceRoute != sourceRoute || item.FlowInstance != targetRoute.FlowInstance {
			t.Fatalf("revision projection = source:%#v receiver:%q, want source %#v and receiver %q", item.SourceRoute, item.FlowInstance, sourceRoute, targetRoute.FlowInstance)
		}
	}

	source := testContractFrontierTemplateConnectSource()
	selection := SelectedContractSelection(source, "/tmp/contracts-a")
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: selection})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	if len(frontier.FrontierEvents) != 1 || len(frontier.FrontierEvents[0].DerivedRecipients) != 1 || frontier.FrontierEvents[0].DerivedRecipients[0].SubscriberID != "consumer-node" {
		t.Fatalf("frontier = %#v, want producer/inst-1 source routed independently of consumer/inst-9 receiver context", frontier.FrontierEvents)
	}

	history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{
		Plan: plan, Source: source, ContractSelection: selection, FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	if len(history.SelectedRouteEvents) != 1 || len(history.SelectedRouteEvents[0].DerivedRecipients) != 1 || history.SelectedRouteEvents[0].DerivedRecipients[0].SubscriberID != "consumer-node" {
		t.Fatalf("history = %#v, want revisioned producer source routed independently of receiver context", history.SelectedRouteEvents)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return string(raw)
}
