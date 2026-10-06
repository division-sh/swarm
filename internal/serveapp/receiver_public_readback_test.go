package serveapp

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/gorilla/websocket"
)

func requireReceiverConstructedInstance(t *testing.T, rt servedControlProofRuntime, runID, instance, template, entityID, entityType, state, parent, wantPhase string, wantRevision int, fields map[string]any) {
	t.Helper()
	physical := requireReceiverConstructionStorage(t, rt.ReceiverStateReader, runID, instance, entityID, entityType)
	if physical.EntityID != entityID || physical.Template != template || physical.State != state || physical.EntityType != entityType || physical.EntityTypePresent != (entityType != "") || physical.Revision != wantRevision || physical.CreatedAt == "" || physical.UpdatedAt == "" || !physical.OrderedClocks {
		t.Fatalf("constructed receiver mismatch: run=%s instance=%s physical=%+v", runID, instance, physical)
	}
	// Historical source-only fork headers preserve their recorded clocks.
	if wantRevision == 1 && wantPhase != "" && physical.UpdatedAt != physical.CreatedAt {
		t.Fatal("delivery-only settlement changed the construction header clock")
	}
	phase, planHash := physical.Phase, physical.PlanHash
	if wantPhase == "" {
		// A historical, fieldless source-only root is inventoried, not attached.
		if physical.ReadinessPresent || template != "." || entityType != "" || parent != "" {
			t.Fatalf("historical source-only projection acquired attachment: phase=%s", phase)
		}
	} else {
		if !physical.ReadinessPresent || phase != wantPhase || planHash == "" {
			t.Fatalf("receiver lacks exact canonical attachment: %s/%s phase=%s hash=%s", runID, instance, phase, planHash)
		}
		plan, err := pipeline.DecodeFlowReadinessPlan(physical.Plan, planHash)
		if err != nil {
			t.Fatal(err)
		}
		wantParent := flowidentity.ParentRoute{}
		if parent != "" {
			parentTemplate := parent
			if parent == runID {
				parentTemplate = "."
			}
			wantParent = flowidentity.ParentRoute{FlowID: parentTemplate, FlowInstance: parent, EntityID: flowidentity.EntityID(parent)}
		}
		if plan.RunID != runID || plan.BundleHash != rt.BundleHash || plan.Identity.TemplateID != template || plan.Identity.InstancePath != instance || plan.Identity.EntityID != entityID || plan.Identity.ParentRoute != wantParent || plan.Identity.ParentEntityID != wantParent.EntityID {
			t.Fatalf("receiver attachment lost exact construction/source identity: %+v", plan)
		}
	}
	count := physical.FieldRows
	if entityType == "" {
		if count != 0 {
			t.Fatalf("fieldless receiver acquired a field companion: %d", count)
		}
		return
	}
	if count != 1 {
		t.Fatalf("declared receiver lacks its unique field companion: %d", count)
	}
	raw := physical.Fields
	var gotFields map[string]any
	if err := json.Unmarshal(raw, &gotFields); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotFields, fields) {
		t.Fatalf("receiver business fields changed: got=%s want=%v", raw, fields)
	}
}

type receiverPublicDeliveryEvidence struct {
	eventID, status string
	owner           events.DeliveryTargetOwnership
}

func requireReceiverPublicReadback(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	want := map[string]receiverPublicDeliveryEvidence{}
	rows, err := rt.DB.Query(`SELECT delivery_id,event_id,status,CAST(delivery_target_route AS TEXT) FROM event_deliveries WHERE run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, raw string
		var record receiverPublicDeliveryEvidence
		if err := rows.Scan(&id, &record.eventID, &record.status, &raw); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(raw), &record.owner); err != nil {
			t.Fatal(err)
		}
		want[id] = record
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	requireReceiverPublicReadbackEvidence(t, rt.Endpoint, runID, want)
}

func requireReceiverPublicReadbackEvidence(t *testing.T, endpoint, runID string, want map[string]receiverPublicDeliveryEvidence) {
	t.Helper()
	if len(want) == 0 {
		t.Fatal("readback oracle has no production deliveries")
	}
	check := func(event operatorread.OperatorEventFull) {
		t.Helper()
		for _, delivery := range event.Deliveries {
			record, ok := want[delivery.DeliveryID]
			if !ok {
				t.Fatalf("public delivery absent from selected store: %#v", delivery)
			}
			route := record.owner.Route()
			target := operatorread.OperatorDeliveryTarget{Kind: record.owner.Code(), FlowID: route.FlowID, FlowInstance: route.FlowInstance, EntityID: route.EntityID}
			if event.RunID != runID || record.eventID != event.EventID || delivery.Target != target || delivery.Status != record.status {
				t.Fatalf("public delivery changed admitted ownership: %#v want target=%#v status=%s", delivery, target, record.status)
			}
		}
	}
	seen := map[string]bool{}
	publicEvents := map[string]operatorread.OperatorEventFull{}
	cursors := map[string]bool{}
	cursor := ""
	for {
		var page operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 1, "cursor": cursor}, &page)
		for _, event := range page.Events {
			if _, exists := publicEvents[event.EventID]; exists {
				t.Fatal("pagination repeated an event")
			}
			publicEvents[event.EventID] = event
			check(event)
			var got operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": event.EventID}, &got)
			check(got)
			if len(event.Deliveries) != len(got.Deliveries) {
				t.Fatal("get/list delivery cardinality disagrees")
			}
			for _, delivery := range got.Deliveries {
				if seen[delivery.DeliveryID] {
					t.Fatal("pagination repeated a delivery")
				}
				seen[delivery.DeliveryID] = true
			}
		}
		if page.NextCursor == "" {
			break
		}
		if cursors[page.NextCursor] {
			t.Fatal("pagination cursor repeated")
		}
		cursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
	if len(seen) != len(want) {
		t.Fatalf("public delivery omissions: read %d, stored %d", len(seen), len(want))
	}
	wsURL := "ws" + strings.TrimPrefix(strings.TrimSuffix(endpoint, "/v1/rpc"), "http") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + apiv1.DefaultLoopbackAPIToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": "receiver-subscribe", "method": "event.subscribe", "params": map[string]any{"filter": map[string]any{"run_id": runID}, "replay_since": time.Unix(0, 0).UTC().Format(time.RFC3339Nano)}}); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var response struct {
		Result struct {
			SubscriptionID string `json:"subscription_id"`
		} `json:"result"`
		Error *servedJSONRPCError `json:"error"`
	}
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || response.Result.SubscriptionID == "" {
		t.Fatalf("event.subscribe rejected: %#v", response)
	}
	for len(publicEvents) > 0 {
		var notification struct {
			Method string `json:"method"`
			Params struct {
				Subscription string                         `json:"subscription"`
				Result       operatorread.OperatorEventFull `json:"result"`
			} `json:"params"`
		}
		if err := conn.ReadJSON(&notification); err != nil {
			t.Fatalf("event.subscribe omitted %d listed events: %v", len(publicEvents), err)
		}
		event := notification.Params.Result
		listed, exists := publicEvents[event.EventID]
		if notification.Method != "rpc.subscription" || notification.Params.Subscription != response.Result.SubscriptionID || !exists {
			t.Fatalf("unexpected or duplicate event notification: %#v", notification)
		}
		check(event)
		if len(event.Deliveries) != len(listed.Deliveries) || event.EntityID != listed.EntityID || event.EventName != listed.EventName {
			t.Fatalf("subscription changed event projection: %#v want %#v", event, listed)
		}
		delete(publicEvents, event.EventID)
	}
}
