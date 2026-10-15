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
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestChannelDeliveryCompiledRenderFailureDoesNotStarveReceiptUpdates(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			workerLog := &lockedBuffer{}
			previousLog := log.Writer()
			log.SetOutput(io.MultiWriter(previousLog, workerLog))
			defer log.SetOutput(previousLog)
			var selected any
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p.deps.EventStore })
			h, _, bundleHash := startChannelAnchorJourney(t, backend, "scan-token", false)
			var cards [2]string
			var messages [2]int
			for index := range cards {
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "bundle_hash": bundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": fmt.Sprintf("scan-isolation-%d", index)})
				cards[index] = waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				messages[index] = waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), cards[index])
			}
			injectionCtx, cancelInjection := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelInjection()
			fault, err := storetest.CaptureChannelCurrentRenderFaultForTest(injectionCtx, selected, cards[0])
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := fault.RestoreForTest(ctx); err != nil {
					t.Errorf("restore original current render before joined serve teardown: %v", err)
				}
			})
			if err := fault.InstallForTest(injectionCtx); err != nil {
				t.Fatal(err)
			}
			var deferred map[string]any
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.defer", map[string]any{"card_id": cards[1], "until": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano), "idempotency_key": "scan-isolation-defer"}, &deferred)
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
			observed, err := channelProcessStorageObservation(t, h).ReadChannelPlanCardStateStorage(context.Background(), cards[0])
			if err != nil || observed.State != "sent" || observed.Status != "pending" {
				t.Fatalf("corrupt render gained new authority: state=%s status=%s err=%v", observed.State, observed.Status, err)
			}
		})
	}
}
