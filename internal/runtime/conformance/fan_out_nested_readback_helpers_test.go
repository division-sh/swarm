package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

type nestedOperatorReader interface {
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
}

func nestedPublicReader(t *testing.T, selected notifyAllChildrenStore) nestedOperatorReader {
	t.Helper()
	reader, ok := selected.(nestedOperatorReader)
	if !ok {
		t.Fatalf("nested real runtime store %T lacks public event readback", selected)
	}
	return reader
}

// SQL discovers exact durable identities; all payload and delivery assertions
// below use the operator surface, not private fan-out planner representations.
func nestedEventIDs(t *testing.T, ctx context.Context, db *sql.DB, runID, eventName, parentID string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2 AND source_event_id=$3 ORDER BY event_id`, runID, eventName, parentID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func assertNestedDeliveredEvent(t *testing.T, ctx context.Context, reader nestedOperatorReader, id string, routes int) operatorread.OperatorEventFull {
	t.Helper()
	view, err := reader.LoadOperatorEvent(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if routes < 1 || len(view.Deliveries) != routes || view.NoDelivery != nil {
		t.Fatalf("nested event %s actual recipients = %+v no-delivery=%+v, want %d routes", id, view.Deliveries, view.NoDelivery, routes)
	}
	for _, delivery := range view.Deliveries {
		if !delivery.Terminal || delivery.Status != string(deliverylifecycle.StatusDelivered) {
			t.Fatalf("nested event %s recipient did not settle successfully: %+v", id, delivery)
		}
	}
	return view
}

// The public surface exposes delivery completion, not platform-pipeline
// receipts. Check both facts at this held boundary, without inventing a group
// receipt or requiring the retired one-revision-per-member history layout.
func assertNestedPredecessorsSettledWhileChildHeld(t *testing.T, ctx context.Context, db *sql.DB, reader nestedOperatorReader, parents []notifyAllChildrenItemEvent, heldEventID string) {
	t.Helper()
	for _, parent := range parents {
		assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 1)
		var outcome string
		if err := db.QueryRowContext(ctx, `SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, parent.ID).Scan(&outcome); err != nil || outcome != "success" {
			t.Fatalf("nested predecessor %s handoff not durably settled before child execution: outcome=%q err=%v", parent.ID, outcome, err)
		}
	}
	var prematureReceipts int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, heldEventID).Scan(&prematureReceipts); err != nil {
		t.Fatal(err)
	}
	if prematureReceipts != 0 {
		t.Fatalf("still-executing nested member %s acknowledged itself to unblock handoff: receipts=%d", heldEventID, prematureReceipts)
	}
}

// Each account sibling uses the SAME task identity values. Only event lineage
// and exact sibling account ownership may separate its recipient effects.
func assertNestedSiblingTaskEffects(t *testing.T, ctx context.Context, selected notifyAllChildrenStore, db *sql.DB, runID, taskEvent, effectEvent string, parents []notifyAllChildrenItemEvent) {
	t.Helper()
	reader := nestedPublicReader(t, selected)
	seenEvents := make(map[string]bool, 4*len(parents))
	for _, parent := range parents {
		parentView := assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 1)
		taskName := eventidentity.ExternalizeForFlow(parentView.Deliveries[0].Target.FlowInstance, []string{taskEvent}, taskEvent)
		ids := nestedEventIDs(t, ctx, db, runID, taskName, parent.ID)
		if len(ids) != 2 {
			t.Fatalf("nested sibling %s direct task count=%d, want 2", parent.AccountID, len(ids))
		}
		seenTasks := map[string]bool{}
		for _, id := range ids {
			if seenEvents[id] {
				t.Fatalf("sibling task identity collapsed into shared event %s", id)
			}
			seenEvents[id] = true
			view := assertNestedDeliveredEvent(t, ctx, reader, id, 1)
			task, _ := view.Payload["task"].(string)
			if (task != "prepare" && task != "publish") || seenTasks[task] || view.Payload["account_id"] != parent.AccountID || view.Payload["task_key"] != parent.AccountID+":"+task {
				t.Fatalf("nested sibling %s task payload=%+v repeated=%v", parent.AccountID, view.Payload, seenTasks[task])
			}
			seenTasks[task] = true
			effectName := eventidentity.ExternalizeForFlow(view.Deliveries[0].Target.FlowInstance, []string{effectEvent}, effectEvent)
			effectIDs := nestedEventIDs(t, ctx, db, runID, effectName, id)
			if len(effectIDs) != 1 || seenEvents[effectIDs[0]] {
				t.Fatalf("nested task %s exact recipient effect IDs=%v", id, effectIDs)
			}
			seenEvents[effectIDs[0]] = true
			effect, err := reader.LoadOperatorEvent(ctx, effectIDs[0])
			if err != nil {
				t.Fatal(err)
			}
			if effect.Payload["account_id"] != parent.AccountID || effect.Payload["task"] != task || effect.NoDelivery == nil || len(effect.Deliveries) != 0 {
				t.Fatalf("nested handler business effect payload=%+v deliveries=%+v no-delivery=%+v", effect.Payload, effect.Deliveries, effect.NoDelivery)
			}
		}
	}
}

func assertNestedExactBarrier(t *testing.T, ctx context.Context, db *sql.DB, key fanoutobligation.IntentKey, status string, want fanoutbarrier.Summary) {
	t.Helper()
	var gotStatus string
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT status,summary FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`,
		key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&gotStatus, &raw); err != nil {
		t.Fatal(err)
	}
	var got fanoutbarrier.Summary
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if gotStatus != status || got != want {
		t.Fatalf("nested exact-intent barrier %+v = %s/%+v, want %s/%+v", key, gotStatus, got, status, want)
	}
}

func assertNestedPublicCompletion(t *testing.T, ctx context.Context, selected notifyAllChildrenStore, db *sql.DB, runID, eventName, parentID, accountID string, want fanoutbarrier.Summary) {
	t.Helper()
	ids := nestedEventIDs(t, ctx, db, runID, eventName, parentID)
	if len(ids) != 1 {
		t.Fatalf("nested completion %s for parent %s count=%d, want exactly 1", eventName, parentID, len(ids))
	}
	view, err := nestedPublicReader(t, selected).LoadOperatorEvent(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if accountID != "" && view.Payload["account_id"] != accountID {
		t.Fatalf("nested completion sibling identity=%+v, want account_id=%s", view.Payload, accountID)
	}
	for field, value := range map[string]int{
		"total": want.Total, "succeeded": want.Succeeded, "dead_lettered": want.DeadLettered,
		"no_route": want.NoRoute, "semantic_rejected": want.SemanticRejected, "canceled": want.Canceled,
	} {
		if got, ok := view.Payload[field].(float64); !ok || got != float64(value) {
			t.Fatalf("nested public completion %s=%#v, want %d; payload=%+v", field, view.Payload[field], value, view.Payload)
		}
	}
	if view.NoDelivery == nil || len(view.Deliveries) != 0 {
		t.Fatalf("nested public completion disposition=%+v/%+v", view.NoDelivery, view.Deliveries)
	}
}
