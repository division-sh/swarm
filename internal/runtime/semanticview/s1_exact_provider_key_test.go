package semanticview

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestS1TriggerProvenanceReadbackRefusesProviderKeyRepair(t *testing.T) {
	generation := triggergeneration.FromCanonicalBytes([]byte("exact-provider-key"))
	base := Capabilities{providerTrigger: &providerTriggerCapabilities{generation: generation}}
	for _, key := range []string{"telegram-sales", "telegram_sales", "_team", "con", "Telegram", "telegram.bot", " telegram_sales", "telegram_sales ", strings.Repeat("a", 101)} {
		provenance := ProviderTriggerEventProvenance{Provider: key, Event: "inbound.telegram_sales.message", Kind: "normalized", PackID: "test.trigger", PackVersion: "1.0.0",
			ManifestHash: "sha256:" + strings.Repeat("a", 64), SourceProvenance: "test", Generation: generation, FlowScopes: []string{"."}}
		got := base.WithProviderTriggerEventProvenance([]ProviderTriggerEventProvenance{provenance})
		readback := got.ProviderTriggerEventProvenance()
		valid := key == "telegram-sales" || key == "telegram_sales"
		if !valid {
			if provenance.Valid() || len(readback) != 0 || got.providerTrigger != nil {
				t.Errorf("nonportable provenance provider %q survived: %+v", key, readback)
			}
			continue
		}
		if !provenance.Valid() || len(readback) != 1 || readback[0].Provider != key {
			t.Fatalf("readback changed exact key %q: %+v", key, readback)
		}
		readback[0].Provider = "changed"
		if got.ProviderTriggerEventProvenance()[0].Provider != key || len(base.ProviderTriggerEventProvenance()) != 0 {
			t.Fatal("provenance capture/readback mutated another value")
		}
	}
}
