package serveapp

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestSelectedForkNoticeAcknowledgmentBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyForkReceiverNoticeOwnership(t, []canonicalrouting.ForkReceiver{{Path: "consumer", Policy: canonicalrouting.ForkReceiverRequiredExisting}}, false, false, false)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "selected-notice-seed",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "selected-notice-request",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, seed.RunID).Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			sourceNotices := readForkReceiverNoticeDomain(t, rt, seed.RunID)
			var fork apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{
				"source_run_id": seed.RunID, "fork_event_id": frontier, "allow_source_freeze": true, "idempotency_key": "selected-notice-fork",
			}, &fork)
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 {
				t.Fatalf("public fork lost exact source/execution: %+v", fork)
			}
			var noticeID, childEvent string
			if err := rt.DB.QueryRow(`SELECT m.item_id,m.source_event_id FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1 AND m.item_type='receiver_processed'`, fork.ForkRunID).Scan(&noticeID, &childEvent); err != nil {
				t.Fatal(err)
			}
			type noticeProjection struct {
				Kind   string               `json:"kind"`
				Notice mailbox.V1ItemDetail `json:"notice"`
			}
			var detail noticeProjection
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": noticeID}, &detail)
			if detail.Kind != "notice" || detail.Notice.Item.MailboxID != noticeID || detail.Notice.Item.SourceEventID != childEvent || detail.Notice.Item.Status != "pending" {
				t.Fatalf("selected notice projection: %+v", detail)
			}
			var list struct {
				Items []struct {
					Kind   string         `json:"kind"`
					Notice mailbox.V1Item `json:"notice"`
				} `json:"items"`
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"run_id": fork.ForkRunID}, &list)
			if len(list.Items) != 1 || list.Items[0].Kind != "notice" || !reflect.DeepEqual(list.Items[0].Notice, detail.Notice.Item) {
				t.Fatalf("selected notice list borrowed another run: %+v", list)
			}
			// Retire execution through the public owner before comparing all domain
			// rows. Notice acknowledgment remains available without a live context.
			var stopped map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "run.stop", map[string]any{"run_id": fork.ForkRunID}, &stopped)
			before := snapshotForkReceiverApplication(t, rt)
			var notified bool
			if err := rt.DB.QueryRow(`SELECT notified FROM mailbox WHERE item_id=$1`, noticeID).Scan(&notified); err != nil || notified {
				t.Fatalf("fresh selected notice notification: notified=%v err=%v", notified, err)
			}
			var ack struct {
				Kind     string `json:"kind"`
				OK       bool   `json:"ok"`
				Replayed bool   `json:"idempotency_replayed"`
			}
			ackParams := map[string]any{"mailbox_id": noticeID, "idempotency_key": "selected-notice-ack"}
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.acknowledge", ackParams, &ack)
			if !ack.OK || ack.Kind != "notice" || ack.Replayed {
				t.Fatalf("selected durable acknowledgment refused: %+v", ack)
			}
			var acknowledged noticeProjection
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": noticeID}, &acknowledged)
			if !reflect.DeepEqual(acknowledged, detail) {
				t.Fatalf("selected acknowledgment lost notice identity/payload: %+v", acknowledged)
			}
			if err := rt.DB.QueryRow(`SELECT notified FROM mailbox WHERE item_id=$1`, noticeID).Scan(&notified); err != nil || !notified {
				t.Fatalf("selected notice acknowledgment not persisted: notified=%v err=%v", notified, err)
			}
			after := snapshotForkReceiverApplication(t, rt)
			for _, table := range []string{"runs", "events", "event_deliveries", "event_delivery_attempts", "entity_state", "agents", "agent_turns", "runtime_external_effect_attempts", "run_fork_selected_contract_runtime_executions"} {
				if !reflect.DeepEqual(before[table], after[table]) {
					t.Fatalf("notice acknowledgment acquired execution or changed domain %s: before=%v after=%v", table, before[table], after[table])
				}
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.acknowledge", ackParams, &ack)
			if !ack.OK || ack.Kind != "notice" || !ack.Replayed {
				t.Fatalf("selected acknowledgment did not replay exact result: %+v", ack)
			}
			if !reflect.DeepEqual(after, snapshotForkReceiverApplication(t, rt)) || !reflect.DeepEqual(sourceNotices, readForkReceiverNoticeDomain(t, rt, seed.RunID)) {
				t.Fatal("repeated acknowledgment mutated state or source notice")
			}
		})
	}
}
