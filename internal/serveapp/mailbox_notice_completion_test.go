package serveapp

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestMailboxNoticeAtomicCompletionBothStores(t *testing.T) {
	requireMailboxCompletionFaultFunction(t)
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			f := newCursorMailboxFixture(t, backend)
			n := f.notice(t, uuid.NewString(), time.Now().UTC())
			id := n.Notice.MailboxID
			params := map[string]any{"mailbox_id": id, "idempotency_key": "notice-atomic"}
			beforeEffects := mailboxCompletionRunEffects(t, f.rt, f.base.RunID)
			assertCut, remove := installMailboxCompletionFaultWitness(t, f.rt, "notice-atomic")
			failed := requestServedJSONRPC(t, f.rt.Endpoint, "mailbox.acknowledge", params)
			if failed.Error == nil {
				t.Fatal("completion INSERT fault was not surfaced")
			}
			assertCut()
			if after := mailboxCompletionRunEffects(t, f.rt, f.base.RunID); !reflect.DeepEqual(beforeEffects, after) {
				t.Fatalf("notice failure mutated run: before=%v after=%v", beforeEffects, after)
			}
			var notified bool
			var rows int
			if err := f.rt.DB.QueryRow(`SELECT notified FROM mailbox WHERE item_id=$1`, id).Scan(&notified); err != nil {
				t.Fatal(err)
			}
			if err := f.rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE resource_id=$1`, id).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if notified || rows != 0 {
				t.Fatalf("partial notice commit: notified=%t completion=%d", notified, rows)
			}
			remove()
			var original, replay map[string]any
			requireServedJSONRPCResult(t, f.rt.Endpoint, "mailbox.acknowledge", params, &original)
			requireServedJSONRPCResult(t, f.rt.Endpoint, "mailbox.acknowledge", params, &replay)
			if original["idempotency_replayed"] != false || replay["idempotency_replayed"] != true {
				t.Fatalf("replay markers: %v %v", original, replay)
			}
			delete(original, "idempotency_replayed")
			delete(replay, "idempotency_replayed")
			var kind, actor, principal, raw string
			if err := f.rt.DB.QueryRow(`SELECT actor_kind,actor_id,CAST(response AS TEXT) FROM api_idempotency WHERE method='mailbox.acknowledge' AND resource_id=$1`, id).Scan(&kind, &actor, &raw); err != nil {
				t.Fatal(err)
			}
			if err := f.rt.DB.QueryRow(`SELECT principal_id FROM operator_principals`).Scan(&principal); err != nil {
				t.Fatal(err)
			}
			var stored map[string]any
			if err := json.Unmarshal([]byte(raw), &stored); err != nil {
				t.Fatal(err)
			}
			if kind != "operator_principal" || actor != principal || !reflect.DeepEqual(stored, original) || !reflect.DeepEqual(original, replay) {
				t.Fatalf("notice completion mismatch: kind=%s actor=%s result=%v replay=%v stored=%v", kind, actor, original, replay, stored)
			}
			if err := f.rt.DB.QueryRow(`SELECT notified FROM mailbox WHERE item_id=$1`, id).Scan(&notified); err != nil || !notified {
				t.Fatalf("notice not acknowledged: %t %v", notified, err)
			}
			for _, rejected := range []string{uuid.NewString(), f.base.CardID} {
				bad := requestServedJSONRPC(t, f.rt.Endpoint, "mailbox.acknowledge", map[string]any{"mailbox_id": rejected, "idempotency_key": "invalid-" + rejected})
				if bad.Error == nil {
					t.Fatalf("acknowledged invalid notice %s", rejected)
				}
				if err := f.rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE idempotency_key=$1`, "invalid-"+rejected).Scan(&rows); err != nil || rows != 0 {
					t.Fatalf("invalid completion: %d %v", rows, err)
				}
			}
			if strings.Contains(raw, "decision_event_id") {
				t.Fatal("notice returned a card decision")
			}
		})
	}
}
