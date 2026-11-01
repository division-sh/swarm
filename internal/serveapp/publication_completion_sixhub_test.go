package serveapp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestPublicPublicationSixHubOriginalDeadlineBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := issue2564H2Source(t)
			keys := issue2564H2DeclarationKeys(t, root)
			start, _, _ := issue2564H2Harness(t, backend, root)
			process, rt := start(false)
			t.Cleanup(func() {
				if err := process.stop(); err != nil {
					t.Error(err)
				}
			})
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "hub.start", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "q6-hub-1",
			})
			starts := map[string]bool{seed.EventID: true}
			for hub := 2; hub <= 6; hub++ {
				params := map[string]any{"event_name": "hub.start", "run_id": seed.RunID, "source_event_id": seed.EventID,
					"payload": map[string]any{"hub_id": fmt.Sprintf("h%02d", hub)}, "idempotency_key": fmt.Sprintf("q6-hub-%d", hub)}
				first := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				replay := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if first.EventID != replay.EventID || first.RunID != seed.RunID || replay.RunID != seed.RunID {
					t.Fatalf("hub %d lost exact-key/run identity: first=%+v replay=%+v", hub, first, replay)
				}
				if starts[first.EventID] {
					t.Fatal("distinct start requests shared an event ID")
				}
				starts[first.EventID] = true
			}
			rt.waitDeliveries(t, seed.RunID)
			ctx, cancel := context.WithTimeout(t.Context(), servedProofPollDeadline)
			defer cancel()
			var accountingErr error
			for ctx.Err() == nil {
				snapshot, err := issue2564H2Read(ctx, rt, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
				accountingErr = issue2564H2Accounting(seed.RunID, snapshot, false, keys, 6)
				if accountingErr == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if accountingErr != nil {
				t.Fatalf("six exact constructed hubs/timers/receipts: %v\n%s", accountingErr, rt.debug(t, seed.RunID))
			}
			for id := range starts {
				evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), rt.selected, seed.RunID, id)
				if !evidence.RecordFound || evidence.Record.EventName != "hub.start" || evidence.PipelineReceiptCount != 1 || evidence.PipelineReceiptOutcome != "success" || evidence.PipelineReceiptFailure != nil {
					t.Fatalf("accepted exact start did not settle: %+v", evidence)
				}
			}
			if len(starts) != 6 {
				t.Fatalf("six original requests/replays created %d starts", len(starts))
			}
		})
	}
}
