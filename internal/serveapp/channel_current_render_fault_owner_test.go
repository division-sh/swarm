package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestChannelCurrentRenderFaultPreservesScopeCancellationAndRestorationBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			var selected any
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p.deps.EventStore })
			h, _, hash := startChannelAnchorJourney(t, backend, "render-fault-control", false)
			seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
				"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "render-fault-control",
			})
			card := waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
			waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), card)
			reader := channelSettlementStorageObservation(t, h)
			before, err := reader.ReadChannelCardRenderStorage(context.Background(), card)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := storetest.CaptureChannelCurrentRenderFaultForTest(context.Background(), nil, card); err == nil {
				t.Fatal("foreign owner acquired a render mutation")
			}
			if _, err := storetest.CaptureChannelCurrentRenderFaultForTest(context.Background(), selected, uuid.NewString()); err == nil {
				t.Fatal("absent source acquired a render mutation")
			}
			fault := captureChannelRenderFaultWithCleanup(t, selected, card)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := fault.InstallForTest(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled mutation did not refuse: %v", err)
			}
			assertChannelRenderStorageUnchanged(t, reader, card, before)
			if err := fault.InstallForTest(context.Background()); err != nil {
				t.Fatal(err)
			}
			corrupt, err := reader.ReadChannelRenderStorage(context.Background(), before.RenderID)
			if err != nil || corrupt.Input != "{}" || corrupt.Hash != before.Hash {
				t.Fatalf("fault changed more than exact render input: %+v %v", corrupt, err)
			}
			if err := fault.InstallForTest(context.Background()); err == nil {
				t.Fatal("duplicate installation passed its expected-row fence")
			}
			if err := fault.RestoreForTest(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled restoration mutated the row: %v", err)
			}
			if err := fault.RestoreForTest(context.Background()); err != nil {
				t.Fatal(err)
			}
			assertChannelRenderStorageUnchanged(t, reader, card, before)
			proveChannelRenderFaultRefusesSuccessor(t, h, selected, reader, card, before)
		})
	}
}

func captureChannelRenderFaultWithCleanup(t *testing.T, selected any, card string) *storetest.ChannelCurrentRenderFault {
	t.Helper()
	fault, err := storetest.CaptureChannelCurrentRenderFaultForTest(context.Background(), selected, card)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := fault.RestoreForTest(ctx); err != nil {
			t.Error(err)
		}
	})
	return fault
}

func assertChannelRenderStorageUnchanged(t *testing.T, reader storetest.ChannelSettlementObservation, card string, before storetest.ChannelCardRenderStorage) {
	t.Helper()
	after, err := reader.ReadChannelCardRenderStorage(context.Background(), card)
	if err != nil || after != before {
		t.Fatalf("render refusal/restoration changed exact source snapshot: before=%+v after=%+v err=%v", before, after, err)
	}
}

func proveChannelRenderFaultRefusesSuccessor(t *testing.T, h *channelOnboardingE2EHarness, selected any, reader storetest.ChannelSettlementObservation, card string, before storetest.ChannelCardRenderStorage) {
	t.Helper()
	stale := captureChannelRenderFaultWithCleanup(t, selected, card)
	var deferred map[string]any
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.defer", map[string]any{
		"card_id": card, "until": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano), "idempotency_key": "render-fault-successor",
	}, &deferred)
	deadline := time.Now().Add(20 * time.Second)
	var successor storetest.ChannelCardRenderStorage
	for {
		var err error
		successor, err = reader.ReadChannelCardRenderStorage(context.Background(), card)
		if err != nil {
			t.Fatal(err)
		}
		if successor.RenderID != before.RenderID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native defer did not advance its current render")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := stale.InstallForTest(context.Background()); err == nil {
		t.Fatal("stale capture overwrote a successor render")
	}
	assertChannelRenderStorageUnchanged(t, reader, card, successor)
	retained, err := reader.ReadChannelRenderStorage(context.Background(), before.RenderID)
	if err != nil || retained.Input != before.Input || retained.Hash != before.Hash {
		t.Fatalf("stale refusal changed retained history: %+v %v", retained, err)
	}
}
