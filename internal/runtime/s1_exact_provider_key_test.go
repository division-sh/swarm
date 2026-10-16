package runtime

import (
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestS1StandingAndSchemaOwnersKeepExactProviderKeys(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", " Telegram", "telegram "} {
		target := (StandingTarget{Provider: key}).normalized()
		if target.Provider != key {
			t.Errorf("standing target changed %q to %q", key, target.Provider)
		}
		owner := newProviderTriggerSchemaOwner(key, "inbound."+key+".message", providertriggers.OutputKindNormalized, providertriggers.PackIdentity{}, triggergeneration.FromCanonicalBytes([]byte("exact-key")))
		if owner.provider != key {
			t.Errorf("schema owner changed %q to %q", key, owner.provider)
		}
	}
}

func TestS1IngressLookupDoesNotRepairProviderKeys(t *testing.T) {
	manager := &RuntimeContextManager{
		order: []string{"source"},
		contexts: map[string]*runtimeContextEntry{"source": {
			state: RuntimeContextStateLoaded,
			context: &BundleContext{StandingTargets: []StandingTarget{
				{Alias: "shop", Provider: "telegram-sales", RunID: "hyphen"},
				{Alias: "shop", Provider: "telegram_sales", RunID: "underscore"},
			}},
		}},
	}
	for key, run := range map[string]string{"telegram-sales": "hyphen", "telegram_sales": "underscore"} {
		got := manager.LookupIngress("shop", key)
		if !got.Loaded() || got.Target.RunID != run || got.Target.Provider != key {
			t.Errorf("exact key %q selected %+v", key, got)
		}
	}
	for _, key := range []string{" telegram-sales", "telegram-sales ", " telegram_sales", "telegram_sales ", "Telegram_sales", "telegram.sales"} {
		if got := manager.LookupIngress("shop", key); got.Found || !got.AliasFound {
			t.Errorf("lookup repaired %q into a target: %+v", key, got)
		}
		use, got, err := manager.AcquireIngress(t.Context(), "shop", key)
		if use != nil {
			if releaseErr := use.Done(); releaseErr != nil {
				t.Error(releaseErr)
			}
		}
		if err != nil || use != nil || got.Found || !got.AliasFound {
			t.Errorf("acquisition repaired %q into a target: found=%v use=%v err=%v", key, got.Found, use != nil, err)
		}
	}
	for _, key := range []string{"telegram-sales", "telegram_sales", " telegram_sales", "telegram_sales "} {
		_, got, _ := parseWebhookPath("/webhooks/shop/" + key)
		if got != key {
			t.Errorf("gateway path changed %q to %q", key, got)
		}
	}
}
