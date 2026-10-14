package serveapp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelLearnedObjectHumanTemporalReceiptPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, transition := range []string{"defer_restart", "expire_restart", "stop_supersedes"} {
			t.Run(string(backend)+"/"+transition, func(t *testing.T) {
				h, db, p, _, hash := startObjectChannelJourney(t, backend, canonicalrouting.CopyChannelLearnedObjectAnchorJourney(t), channelAnchorLLMRuntime{}, "object-provider-secret")
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "object-temporal-seed"})
				gate := waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				gateReceipt := waitObjectCardReceipt(t, channelSettlementStorageObservation(t, h), gate)
				deadline := time.Now().UTC().Add(time.Hour)
				if transition == "expire_restart" {
					deadline = time.Now().UTC().Add(10 * time.Second)
				}
				requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "observer.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true, "deadline_at": deadline.Format(time.RFC3339Nano)}, "idempotency_key": "object-temporal-human"})
				card := waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindHumanTask, "observers")
				receipt := waitObjectCardReceipt(t, channelSettlementStorageObservation(t, h), card)
				approve := objectAnchorChoice(t, p, receipt, "Approve", 0)
				var snapshot, render, input, frozenHash string
				if err := func() error {
					observed, err := channelSettlementStorageObservation(t, h).ReadChannelCardRenderStorage(context.Background(), card)
					snapshot = observed.Snapshot
					render = observed.RenderID
					input = observed.Input
					frozenHash = observed.Hash
					return err
				}(); err != nil {
					t.Fatal(err)
				}
				fragment := "Deferred until: "
				var result map[string]any
				switch transition {
				case "defer_restart":
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.defer", map[string]any{"card_id": card, "until": time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339Nano), "idempotency_key": "object-temporal-defer"}, &result)
					waitObjectMessageText(t, p, receipt, fragment)
					closeObjectIntentObservation(t, p)
					h.stop(t)
					h.start(t)
					p.intentReader = openChannelAnchorObservation(t, h)
					waitObjectMessageText(t, p, receipt, fragment)
				case "expire_restart":
					closeObjectIntentObservation(t, p)
					h.stop(t)
					if remaining := time.Until(deadline); remaining > 0 {
						time.Sleep(remaining)
					}
					h.start(t)
					p.intentReader = openChannelAnchorObservation(t, h)
					fragment = "The decision expired. No action was taken."
				case "stop_supersedes":
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.stop", map[string]any{"run_id": seed.RunID, "idempotency_key": "object-temporal-stop"}, &result)
					fragment = "The flow moved on. No action was taken."
					waitObjectTerminalReceipt(t, db, p, gate, gateReceipt, fragment)
				}
				if transition != "defer_restart" {
					waitObjectTerminalReceipt(t, db, p, card, receipt, fragment)
				}
				var afterSnapshot, afterInput, afterHash string
				if err := func() error {
					observed, err := channelSettlementStorageObservation(t, h).ReadCardSnapshotStorage(context.Background(), card)
					afterSnapshot = observed
					return err
				}(); err != nil {
					t.Fatal(err)
				}
				if err := func() error {
					observed, err := channelSettlementStorageObservation(t, h).ReadChannelRenderStorage(context.Background(), render)
					afterInput = observed.Input
					afterHash = observed.Hash
					return err
				}(); err != nil {
					t.Fatal(err)
				}
				if afterSnapshot != snapshot || afterInput != input || afterHash != frozenHash {
					t.Fatal("temporal transition/restart changed the frozen historical meaning")
				}
				proveObjectRejectedControl(t, db, p, receipt, approve, "temporal-stale")
				var decisions int
				{
					var err error
					decisions, err = channelAnchorStorageObservation(t, h).CountRunNamedEventStorage(context.Background(), seed.RunID, "mailbox.card_decided")
					if err != nil || decisions != 0 {
						t.Fatalf("historical control regained authority: decisions=%d %v", decisions, err)
					}
				}
				p.mu.Lock()
				defer p.mu.Unlock()
				for _, operation := range []string{"/v2/deliver", "/v2/edit"} {
					for _, sent := range p.calls[operation] {
						if !objectChannelPresentationValid(sent, false) {
							t.Fatalf("%s temporal receipt violates the compiled bounds: %s", operation, fmt.Sprint(sent))
						}
					}
				}
			})
		}
	}
}
