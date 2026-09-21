package serveapp

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/servedparity"
)

// The selected-system-node notice composition is retired by #2307 Gate A.
// This is independent supported notify_human coverage, not equivalent fork proof.
func TestSupportedHumanNoticeAcknowledgmentAfterRetirementBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			f := newMailboxCompletionFixture(t, backend)
			rt, runID := f.rt, f.base.RunID
			noticeID := mailboxCompletionNotice(t, f)
			var childEvent string
			if err := rt.DB.QueryRow(`SELECT m.source_event_id FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE m.item_id=$1 AND e.run_id=$2`, noticeID, runID).Scan(&childEvent); err != nil {
				t.Fatal(err)
			}
			type noticeProjection struct {
				Kind   string               `json:"kind"`
				Notice mailbox.V1ItemDetail `json:"notice"`
			}
			var detail noticeProjection
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": noticeID}, &detail)
			if detail.Kind != "notice" || detail.Notice.Item.MailboxID != noticeID || detail.Notice.Item.SourceEventID != childEvent || detail.Notice.Item.SourceFlow != "observers" || detail.Notice.Item.Status != "pending" {
				t.Fatalf("supported notice projection: %+v", detail)
			}
			var list struct {
				Items []struct {
					Kind   string         `json:"kind"`
					Notice mailbox.V1Item `json:"notice"`
				} `json:"items"`
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"run_id": runID}, &list)
			notices := 0
			for _, item := range list.Items {
				if item.Kind == "notice" {
					notices++
					if !reflect.DeepEqual(item.Notice, detail.Notice.Item) {
						t.Fatalf("notice list borrowed another identity: %+v", list)
					}
				}
			}
			if notices != 1 {
				t.Fatalf("supported notice list count=%d: %+v", notices, list)
			}
			// Retire execution through the public owner before comparing all domain
			// rows. Notice acknowledgment remains available without a live context.
			var stopped map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "run.stop", map[string]any{"run_id": runID}, &stopped)
			before := snapshotForkReceiverApplication(t, rt)
			var notified bool
			if err := rt.DB.QueryRow(`SELECT notified FROM mailbox WHERE item_id=$1`, noticeID).Scan(&notified); err != nil || notified {
				t.Fatalf("fresh supported notice notification: notified=%v err=%v", notified, err)
			}
			var ack struct {
				Kind     string `json:"kind"`
				OK       bool   `json:"ok"`
				Replayed bool   `json:"idempotency_replayed"`
			}
			ackParams := map[string]any{"mailbox_id": noticeID, "idempotency_key": "supported-notice-ack"}
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.acknowledge", ackParams, &ack)
			if !ack.OK || ack.Kind != "notice" || ack.Replayed {
				t.Fatalf("supported durable acknowledgment refused: %+v", ack)
			}
			var acknowledged noticeProjection
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": noticeID}, &acknowledged)
			if !reflect.DeepEqual(acknowledged, detail) {
				t.Fatalf("supported acknowledgment lost notice identity/payload: %+v", acknowledged)
			}
			if err := rt.DB.QueryRow(`SELECT notified FROM mailbox WHERE item_id=$1`, noticeID).Scan(&notified); err != nil || !notified {
				t.Fatalf("supported notice acknowledgment not persisted: notified=%v err=%v", notified, err)
			}
			after := snapshotForkReceiverApplication(t, rt)
			for _, table := range []string{"runs", "events", "event_deliveries", "event_delivery_attempts", "entity_state", "agents", "agent_turns", "runtime_external_effect_attempts", "run_fork_selected_contract_runtime_executions"} {
				if !reflect.DeepEqual(before[table], after[table]) {
					t.Fatalf("notice acknowledgment acquired execution or changed domain %s: before=%v after=%v", table, before[table], after[table])
				}
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.acknowledge", ackParams, &ack)
			if !ack.OK || ack.Kind != "notice" || !ack.Replayed {
				t.Fatalf("supported acknowledgment did not replay exact result: %+v", ack)
			}
			if !reflect.DeepEqual(after, snapshotForkReceiverApplication(t, rt)) {
				t.Fatal("repeated acknowledgment mutated state")
			}
		})
	}
}
