package providertriggers

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

func TestS1TriggerNormalizedOutputKeepsDirectoryKeyAndWireConversionSeparate(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales"} {
		t.Run(key, func(t *testing.T) {
			fixture := normalizedEventTestManifest()
			fixture.Provider = key
			fixture.EventType.Literal = "Message.Sent-V2"
			fixture.EventName.Literal = "inbound." + key + ".raw"
			fixture.NormalizedEvents[0].Event = "inbound." + key + ".text_message"
			manifest, err := fixture.admit()
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: manifest,
				Identity: PackIdentity{ID: "test.trigger", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("a", 64), Provenance: "external"},
				Source:   "test", SourcePath: "/trigger",
			})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "shop", Provider: key,
				Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement}})
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := plan.Accept(Request{Provider: key, Payload: map[string]any{"message": map[string]any{
				"chat": map[string]any{"id": json.Number("42")}, "message_id": json.Number("7"), "text": "hello",
			}}})
			if err != nil || len(delivery.Events) != 2 {
				t.Fatalf("normalization: %+v err=%v", delivery, err)
			}
			if got := delivery.ProviderEventType; got != "message_sent_v2" {
				t.Errorf("wire event-type conversion changed: %q", got)
			}
			raw, normalized := delivery.Events[0], delivery.Events[1]
			if raw.Name != events.EventType("inbound."+key+".raw") || raw.Payload["provider"] != key || !raw.Authorization.Empty() {
				t.Errorf("raw key or authority changed: %+v", raw)
			}
			if normalized.Name != events.EventType("inbound."+key+".text_message") || normalized.Authorization.Provider() != key {
				t.Errorf("normalized key changed: %+v", normalized)
			}
			if err := catalog.VerifyProviderOutputAuthorization(normalized.Authorization); err != nil {
				t.Fatalf("exact output authority: %v", err)
			}
			foreign := "telegram_sales"
			if key == foreign {
				foreign = "telegram-sales"
			}
			forged := provideroutput.MustAuthorization(foreign, string(normalized.Name), "test.trigger", "1.0.0", "sha256:"+strings.Repeat("a", 64), catalog.Generation())
			if err := catalog.VerifyProviderOutputAuthorization(forged); err == nil {
				t.Fatal("foreign directory key acquired output authority")
			}
		})
	}
}

func TestS1TriggerProviderKeyNeverUsesProtocolNormalization(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", "9-team", strings.Repeat("a", 100)} {
		t.Run(key, func(t *testing.T) {
			fixture := admissionTestManifest(key, signatureTypeTokenEquality, true)
			manifest, err := fixture.admit()
			if err != nil || manifest.Provider() != key {
				t.Fatalf("provider key changed: got=%q err=%v", manifest.Provider(), err)
			}
			catalog, err := NewCatalogSnapshot(admissionTestEntry(fixture, "test.trigger"))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "shop", Provider: key, SigningSecret: "secret"})
			if err != nil || plan.Provider() != key {
				t.Fatalf("plan key changed: got=%q err=%v", plan.Provider(), err)
			}
			request := Request{Provider: key, Target: Target{WebhookSecret: "value"}, Headers: http.Header{
				"X-Signature": {"value"}, "X-Delivery": {"delivery-1"},
			}}
			admitted, err := plan.AdmitRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := plan.ProjectDelivery(admitted)
			if err != nil || len(delivery.Events) != 1 || delivery.Events[0].Payload["provider"] != key {
				t.Fatalf("delivery changed provider identity: %+v err=%v", delivery, err)
			}
			otherKey := "telegram-sales"
			if key == otherKey {
				otherKey = "telegram_sales"
			}
			for _, foreign := range []string{" " + key, key + " ", strings.ToUpper(key), "another", otherKey} {
				request.Provider = foreign
				if _, err := plan.AdmitRequest(request); err == nil {
					t.Errorf("plan accepted foreign provider %q", foreign)
				}
				if _, err := manifest.Accept(request); err == nil {
					t.Errorf("manifest accepted foreign provider %q", foreign)
				}
			}
			request.Provider = ""
			if _, err := plan.AdmitRequest(request); err != nil {
				t.Fatalf("omitted request key must consume its already selected plan: %v", err)
			}
		})
	}
}

func TestS1TriggerCatalogKeepsHyphenAndUnderscoreKeysDistinct(t *testing.T) {
	left := admissionTestEntry(admissionTestManifest("telegram-sales", signatureTypeTokenEquality, true), "test.hyphen")
	right := admissionTestEntry(admissionTestManifest("telegram_sales", signatureTypeTokenEquality, true), "test.underscore")
	catalog, err := NewCatalogSnapshot(left, right)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range []CatalogEntry{left, right} {
		key := entry.Manifest.Provider()
		got, present := catalog.EntryByProvider(key)
		if !present || got.Identity != entry.Identity || got.Manifest.Provider() != key {
			t.Errorf("catalog key %q selected %+v present=%v", key, got, present)
		}
		plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "shop", Provider: key, SigningSecret: "secret"})
		identity, present := plan.PackIdentity()
		if err != nil || !present || identity != entry.Identity || plan.Provider() != key {
			t.Errorf("plan key %q selected %+v present=%v err=%v", key, identity, present, err)
		}
	}
	for _, foreign := range []string{"Telegram_sales", " telegram_sales", "telegram_sales ", "telegram.sales", "telegram sales"} {
		if _, present := catalog.EntryByProvider(foreign); present {
			t.Errorf("catalog lookup repaired %q into an installed key", foreign)
		}
		if plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "shop", Provider: foreign, SigningSecret: "secret"}); err == nil || plan.Valid() {
			t.Errorf("plan repaired %q into authority: %v", foreign, err)
		}
	}
	left.Identity = right.Identity
	first, err := NewCatalogSnapshot(left)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCatalogSnapshot(right)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation().Equal(second.Generation()) {
		t.Fatal("different exact provider keys share a catalog generation")
	}
}

func TestS1TriggerAndRawAdmissionRefuseNonportableProviderKeys(t *testing.T) {
	catalog, err := NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "_team", "con", "lpt9", "Telegram", "telegram.bot", "team bot", " team", "team ", "team\t", strings.Repeat("a", 101), strings.Repeat("a", 99) + "-a"} {
		t.Run(key, func(t *testing.T) {
			if manifest, err := admissionTestManifest(key, signatureTypeTokenEquality, true).admit(); err == nil || manifest.Validate() == nil {
				t.Errorf("manifest admitted nonportable key %q", key)
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "shop", Provider: key,
				Declaration: AdmissionDeclaration{Kind: "raw", Acknowledge: UnsignedWebhookAcknowledgement, Event: "business.message", Payload: "json",
					Authentication: RawAuthenticationDeclaration{Kind: "none"}, DeliveryID: RawDeliveryIDDeclaration{Source: "body_sha256"}},
			})
			if err == nil || plan.Valid() {
				t.Errorf("raw plan admitted nonportable key %q: %v", key, err)
			}
		})
	}
}
