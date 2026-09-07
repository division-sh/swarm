package serveapp

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/gorilla/websocket"
)

func requireReceiverPublicReadback(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	type admitted struct {
		eventID, status string
		owner           events.DeliveryTargetOwnership
	}
	want := map[string]admitted{}
	rows, err := rt.DB.Query(`SELECT delivery_id,event_id,status,CAST(delivery_target_route AS TEXT) FROM event_deliveries WHERE run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, raw string
		var record admitted
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
		requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 1, "cursor": cursor}, &page)
		for _, event := range page.Events {
			if _, exists := publicEvents[event.EventID]; exists {
				t.Fatal("pagination repeated an event")
			}
			publicEvents[event.EventID] = event
			check(event)
			var got operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": event.EventID}, &got)
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
	wsURL := "ws" + strings.TrimPrefix(strings.TrimSuffix(rt.Endpoint, "/v1/rpc"), "http") + "/v1/ws"
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
