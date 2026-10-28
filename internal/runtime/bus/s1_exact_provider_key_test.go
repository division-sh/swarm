package bus

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

func TestS1InboundBatchPreflightRequiresRawPortableProviderKey(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", strings.Repeat("a", 100), "", "_team", "con", "lpt9", "Telegram", "telegram.bot", " telegram", "telegram ", strings.Repeat("a", 101)} {
		batch := InboundDeliveryBatch{Provider: key, Events: []InboundDeliveryEvent{{Kind: provideroutput.KindRaw}}}
		got, err := preflightInboundDeliveryBatch(nil, batch)
		valid := key == "telegram-sales" || key == "telegram_sales" || len(key) == 100
		if valid {
			if err != nil || got.Provider != key {
				t.Errorf("raw preflight changed key %q: got=%q err=%v", key, got.Provider, err)
			}
		} else if err == nil {
			t.Errorf("raw preflight repaired nonportable key %q into %q", key, got.Provider)
		}
	}
}
