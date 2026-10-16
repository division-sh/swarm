package serveapp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type channelHistoryInterleavedObservation struct {
	storetest.ChannelProcessObservation
	afterFirst func()
}

func (r channelHistoryInterleavedObservation) ReadChannelPreservedPrincipalsStorage(ctx context.Context) ([][]any, error) {
	rows, err := r.ChannelProcessObservation.ReadChannelPreservedPrincipalsStorage(ctx)
	if err == nil {
		r.afterFirst()
	}
	return rows, err
}

func TestChannelProcessHistoryKeepsNativeFactsAndIndependentTemporalCutsBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h, _, hash := startChannelAnchorJourney(t, backend, "history-observer-control", false)
			reader := channelProcessStorageObservation(t, h)
			waitSelectedChannelDeliverySendsSettled(t, channelSettlementStorageObservation(t, h))
			before := readChannelPreservedHistory(t, reader)
			var card string
			interleaved := channelHistoryInterleavedObservation{reader, func() {
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "history-interleaved-cut",
				})
				card = waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), card)
			}}
			after := readChannelPreservedHistory(t, interleaved)
			if strings.Contains(before, card) || !strings.Contains(after, card) {
				t.Fatal("later physical observations lost the independently committed source between history cuts")
			}
			var tables [][][]any
			if err := json.Unmarshal([]byte(after), &tables); err != nil || len(tables) != 15 {
				t.Fatalf("physical history lost a table or its row representation: %d %v", len(tables), err)
			}
			for _, index := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14} {
				if len(tables[index]) == 0 {
					t.Fatalf("native history control did not populate table %d", index)
				}
			}
			native := readChannelNativeHistory(t, reader)
			if native[0] == "" || native[2] == "" || native[3] != "installed" || native[6] == "" || native[7] != "current" {
				t.Fatalf("native history lost exact installation/consumer identity: %v", native)
			}
			if frozen := readChannelSourceHistory(t, reader, card); !strings.Contains(frozen, "\n") {
				t.Fatal("source history lost its exact render/first-settled receipt pair")
			}
		})
	}
}
