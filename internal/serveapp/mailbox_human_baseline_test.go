package serveapp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestHumanTaskRealProducerCompletionBaselineBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			f := newMailboxCompletionFixture(t, backend)
			card := mailboxCompletionAnchorCard(t, f, decisioncard.AnchorKindHumanTask)
			anchor, err := card.Anchor.HumanTask()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(anchor.OperationID.String(), "human-task-operation:v1:") {
				t.Fatalf("real dispatcher operation = %q", anchor.OperationID.String())
			}
			if anchor.Source.Route().EntityID != "" {
				t.Fatalf("regression requires the original entityless requester, got %#v", anchor.Source.Route())
			}
			control, err := card.Anchor.ControlRoutingSource()
			if err != nil || control.Kind() != events.RoutingSourceFlowOwnedControl || control.Route() != anchor.Source.Route() {
				t.Fatalf("control projection = %#v, %v; source %#v", control, err, anchor.Source.Route())
			}
			var result map[string]any
			requireServedJSONRPCResult(t, f.rt.Endpoint, "mailbox.defer", map[string]any{"card_id": card.CardID, "until": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano), "idempotency_key": "baseline-human-defer"}, &result)
			waitServedRunDeliveryQuiescence(t, f.rt.DB, f.rt.Backend, card.RunID)
			rows, err := f.rt.DB.Query(`SELECT d.subscriber_id,d.status,CAST(d.delivery_target_route AS TEXT),d.agent_flow_scope_key,d.agent_flow_instance_path FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.event_name='human_task.deferred'`, card.RunID)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				var recipient, status, raw, flow, instance string
				if err := rows.Scan(&recipient, &status, &raw, &flow, &instance); err != nil {
					t.Fatal(err)
				}
				var target events.DeliveryTargetOwnership
				if err := json.Unmarshal([]byte(raw), &target); err != nil {
					t.Fatal(err)
				}
				if recipient != anchor.RequesterAgentID || status != "delivered" || !target.EntitylessReceiver() || target.Route() != anchor.Source.Route() || flow != anchor.Source.Route().FlowID || instance != anchor.Source.Route().FlowInstance {
					t.Fatalf("deferred outcome recipient=%s status=%s target=%s identity=%s/%s, want exact entityless requester %#v", recipient, status, raw, flow, instance, anchor)
				}
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("deferred outcome deliveries = %d, want exactly one requester", count)
			}
		})
	}
}
