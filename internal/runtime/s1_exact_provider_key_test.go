package runtime

import (
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestS1StandingAndSchemaOwnersKeepExactProviderKeys(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", " Telegram", "telegram "} {
		target := (StandingTarget{Provider: key}).normalized()
		if target.Provider != key {
			t.Errorf("standing target changed %q to %q", key, target.Provider)
		}
		owner := newProviderTriggerSchemaOwner(key, "inbound."+key+".message", providertriggers.OutputKindNormalized, providertriggers.PackIdentity{}, triggergeneration.FromCanonicalBytes([]byte("exact-key")), "fixture/trigger.yaml")
		if owner.provider != key {
			t.Errorf("schema owner changed %q to %q", key, owner.provider)
		}
	}
	// Only the consumed child BODY declares ingress, not the visible sibling.
	members := map[string][]byte{
		"manifest.yaml":         []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":           []byte("stages: []\n"),
		"child/manifest.yaml":   []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"child/schema.yaml":     []byte("stages: []\npins: {inputs: [inbound.acme.raw], outputs: [inbound.acme.raw]}\n"),
		"sibling/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"sibling/schema.yaml":   []byte("stages: []\n"),
	}
	for _, holder := range []string{"child", "sibling"} {
		members[holder+"/acme/manifest.yaml"] = []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw]}, cannot: [execute code]}\n")
		members[holder+"/acme/trigger.yaml"] = []byte("transport: webhook\nevent_name: {literal: raw}\ndelivery_id: {literal: receipt, required: true}\nevent_type: {literal: message, required: true}\n")
	}
	bundle, _, _ := loadS1EffectiveProviderScope(t, members)
	declarations, err := ResolveStandingTargetDeclarations(semanticview.Wrap(bundle), nil)
	if err != nil || len(declarations) != 1 || declarations[0].FlowPath != "child" || len(declarations[0].Ingress) != 1 || declarations[0].Ingress[0].Provider != "acme" {
		t.Fatalf("captured child declaration lost its exact provider: %+v %v", declarations, err)
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
