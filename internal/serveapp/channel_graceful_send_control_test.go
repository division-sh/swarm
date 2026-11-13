package serveapp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestGracefulStopKeepsAlreadyLaunchedChannelSendUntilAcknowledgement(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h, hash := startChannelAnchorPublicJourney(t, backend, "synthetic-drain-token", false, 0, nil)
			reader := openChannelAnchorObservation(t, h)
			runID := seedServedPublicationRun(t, h.runtime)
			arrived, release := h.provider.PauseDeliveryResponseMatching(func(message map[string]any) bool {
				text := fmt.Sprint(message["text"])
				return strings.Contains(text, "Run: "+runID[:8]) && strings.Contains(text, "Flow instance: chat/reviews") && strings.Contains(text, "Decision: pending")
			})
			defer release()
			seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
				"event_name": "work.requested", "bundle_hash": hash, "run_id": runID,
				"payload": map[string]any{"seed": true}, "idempotency_key": "held-channel-stop",
			})
			cardID := waitChannelPublicCard(t, h, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
			var heldMessage int
			select {
			case heldMessage = <-arrived:
			case <-time.After(15 * time.Second):
				t.Fatal("accepted channel send did not reach the held provider response")
			}
			process := h.process
			process.cancel()
			release()
			code := process.stop()
			h.process = nil
			receipt, err := reader.HistoricalReceipt(context.Background(), cardID)
			if code != 0 || err != nil || receipt == "" {
				t.Fatalf("graceful stop lost the already-launched channel acknowledgement: run=%s card=%s held_message=%d payload=%+v exit=%d receipt=%q error=%v\n%s", runID, cardID, heldMessage, h.provider.Delivery(heldMessage-1), code, receipt, err, process.outputString())
			}
		})
	}
}
