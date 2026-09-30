package serveapp

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelDeliveryCompiledRenderFailureDoesNotStarveReceiptUpdates(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			workerLog := &lockedBuffer{}
			previousLog := log.Writer()
			log.SetOutput(io.MultiWriter(previousLog, workerLog))
			defer log.SetOutput(previousLog)
			h, db, bundleHash := startChannelAnchorJourney(t, backend, "scan-token", false)
			var cards [2]string
			var messages [2]int
			for index := range cards {
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": bundleHash,
					"payload": map[string]any{"seed": true}, "idempotency_key": fmt.Sprintf("scan-isolation-%d", index),
				})
				cards[index] = waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
				messages[index] = waitChannelAnchorReceipt(t, db, cards[index])
			}
			// Negative fault injection only: neither source cards nor authority are fabricated.
			injectionCtx, cancelInjection := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelInjection()
			conn, err := db.Conn(injectionCtx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if backend == servedparity.BackendDefaultSQLite {
				// The served writer may hold SQLite's lock; configure only this fault-injection connection.
				if _, err := conn.ExecContext(injectionCtx, `PRAGMA busy_timeout=5000`); err != nil {
					t.Fatal(err)
				}
			}
			result, err := conn.ExecContext(injectionCtx, `UPDATE channel_delivery_renders SET render_input='{}'
				WHERE render_id IN (SELECT current_render_id FROM channel_delivery_plans WHERE source_id=$1)`, cards[0])
			if err != nil {
				t.Fatal(err)
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				t.Fatalf("fault did not address one actual frozen render: rows=%d err=%v", rows, err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			var deferred map[string]any
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.defer", map[string]any{
				"card_id": cards[1], "until": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
				"idempotency_key": "scan-isolation-defer",
			}, &deferred)
			deadline := time.Now().Add(20 * time.Second)
			for {
				updated := false
				for _, edit := range h.provider.Edits() {
					updated = updated || (fmt.Sprint(edit["message_id"]) == fmt.Sprint(messages[1]) && strings.Contains(fmt.Sprint(edit["text"]), "Deferred until: "))
				}
				if updated && strings.Contains(workerLog.String(), "freeze channel delivery") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("compiled scan did not isolate corrupt render and update second receipt: edits=%v\n%s", h.provider.Edits(), h.process.outputString())
				}
				time.Sleep(20 * time.Millisecond)
			}
			var state, status string
			if err := db.QueryRow(`SELECT p.state,c.status FROM channel_delivery_plans p
				JOIN decision_cards c ON c.card_id=p.source_id WHERE p.source_id=$1`, cards[0]).Scan(&state, &status); err != nil || state != "sent" || status != "pending" {
				t.Fatalf("corrupt render gained new authority: state=%s status=%s err=%v", state, status, err)
			}
		})
	}
}
