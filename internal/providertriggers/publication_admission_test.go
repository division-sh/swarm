package providertriggers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

func TestA9DeclarationPublicationRequiresAuthenticatedExactOutputs(t *testing.T) {
	catalog, err := NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "shop", Provider: "partner", SigningSecret: "secret", Declaration: AdmissionDeclaration{
			Kind: "raw", Event: "account.opened", Payload: "json",
			Authentication: RawAuthenticationDeclaration{Kind: "hmac_sha256", Header: "X-Signature", Prefix: "sha256=", Encoding: "hex"},
			DeliveryID:     RawDeliveryIDDeclaration{Source: "json_path", JSONPath: "$.delivery_id"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"delivery_id":"one","account_id":"account-7"}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(body)
	request := Request{Provider: "partner", Target: Target{WebhookSecret: "secret"},
		Body: body, Headers: http.Header{"X-Signature": {"sha256=" + hex.EncodeToString(mac.Sum(nil))}}, Received: time.Now()}
	admitted, err := plan.AdmitRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	const bundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	delivery, admission, err := plan.ProjectPublication(admitted, bundleHash, ".")
	if err != nil || len(delivery.Events) != 1 {
		t.Fatalf("project publication=%+v err=%v", delivery, err)
	}
	output := delivery.Events[0]
	routing, err := events.NewExternalIngressRoutingSource(".", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Bytes(output.Payload)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(t.Name()), output.Name, "inbound-gateway", "", payload, 0,
		eventtest.UUID(t.Name()+"run"), events.EventEnvelope{}, routing, time.Now())
	if err := admission.ValidateOutput(bundleHash, "partner", 0, 1, event, provideroutput.KindRaw, provideroutput.Authorization{}); err != nil {
		t.Fatalf("exact authenticated output: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(*PublicationAdmission, *events.Event, *string, *string, *int, *int)
	}{
		{"missing admission", func(a *PublicationAdmission, _ *events.Event, _, _ *string, _, _ *int) { *a = PublicationAdmission{} }},
		{"foreign source", func(_ *PublicationAdmission, e *events.Event, _, _ *string, _, _ *int) {
			foreign, err := events.NewExternalIngressRoutingSource("child", events.RoutingSourceAuthorityProviderAdmissionPlan)
			if err != nil {
				t.Fatal(err)
			}
			*e = eventtest.ExistingRunRootIngressWithRoutingSource(event.ID(), event.Type(), "inbound-gateway", "", payload, 0, event.RunID(), events.EventEnvelope{}, foreign, time.Now())
		}},
		{"changed payload", func(_ *PublicationAdmission, e *events.Event, _, _ *string, _, _ *int) {
			*e = eventtest.ExistingRunRootIngressWithRoutingSource(event.ID(), event.Type(), "inbound-gateway", "", []byte(`{}`), 0, event.RunID(), events.EventEnvelope{}, routing, time.Now())
		}},
		{"foreign bundle", func(_ *PublicationAdmission, _ *events.Event, hash, _ *string, _, _ *int) {
			*hash = strings.ReplaceAll(*hash, "a", "b")
		}},
		{"foreign provider", func(_ *PublicationAdmission, _ *events.Event, _, provider *string, _, _ *int) { *provider = "other" }},
		{"different ordinal", func(_ *PublicationAdmission, _ *events.Event, _, _ *string, ordinal, _ *int) { *ordinal = 1 }},
		{"different count", func(_ *PublicationAdmission, _ *events.Event, _, _ *string, _, count *int) { *count = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate, e, hash, provider, ordinal, count := admission, event, bundleHash, "partner", 0, 1
			test.change(&candidate, &e, &hash, &provider, &ordinal, &count)
			if err := candidate.ValidateOutput(hash, provider, ordinal, count, e, provideroutput.KindRaw, provideroutput.Authorization{}); err == nil {
				t.Fatal("forged or changed publication acquired authority")
			}
		})
	}
	var reconstructed AdmittedRequest
	if err := json.Unmarshal([]byte(`{"ProviderEventID":"different","ProviderEventType":"push"}`), &reconstructed); err != nil {
		t.Fatal(err)
	}
	if _, _, err := plan.ProjectPublication(reconstructed, bundleHash, "."); err == nil {
		t.Fatal("serialized metadata acquired publication authority")
	}
}

func TestAuthenticatedPackProjectionRejectsPostAdmissionMutation(t *testing.T) {
	for _, mutation := range []string{"payload", "digest", "serialized metadata"} {
		t.Run(mutation, func(t *testing.T) {
			fixture := normalizedEventTestManifest()
			fixture.Secret.Required = true
			fixture.Signature = SignatureManifest{Type: signatureTypeHMACSHA256, Header: "X-Signature", SignedPayload: "raw_body"}
			manifest := fixture.mustAdmit()
			catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: manifest, Source: "authenticated-mutation-proof",
				Identity: PackIdentity{ID: "provider.telegram", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("c", 64), Provenance: "test"}})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "telegram", SigningSecret: "signed-proof"})
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"message":{"message_id":7,"chat":{"id":42},"text":"authenticated"}}`)
			var payload any
			if err := canonicaljson.DecodePreservingNumberLexemes(body, &payload); err != nil {
				t.Fatal(err)
			}
			mac := hmac.New(sha256.New, []byte("signed-proof"))
			_, _ = mac.Write(body)
			admitted, err := plan.AdmitRequest(Request{Provider: "telegram", Target: Target{WebhookSecret: "signed-proof"},
				Body: body, Payload: payload, Headers: http.Header{"X-Signature": {hex.EncodeToString(mac.Sum(nil))}}})
			if err != nil {
				t.Fatal(err)
			}
			const bundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			if delivery, _, err := plan.ProjectPublication(admitted, bundleHash, "."); err != nil || len(delivery.Events) != 2 {
				t.Fatalf("exact authenticated positive control: %+v, %v", delivery, err)
			}
			switch mutation {
			case "payload":
				payload.(map[string]any)["message"].(map[string]any)["text"] = "not authenticated"
			case "digest":
				admitted.semanticContentDigest = strings.Repeat("a", 64)
			case "serialized metadata":
				admitted = AdmittedRequest{}
				if err := json.Unmarshal([]byte(`{"ProviderEventID":"delivery-1","ProviderEventType":"update","SemanticContentDigest":"claimed","AcknowledgeBeforeDispatch":true,"Response":{"Status":200,"Body":"Zm9yZ2Vk"}}`), &admitted); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := plan.ProjectDelivery(admitted); err == nil {
				t.Fatal("changed authenticated request reached delivery projection")
			}
			if _, _, err := plan.ProjectPublication(admitted, bundleHash, "."); err == nil {
				t.Fatal("changed authenticated request acquired publication admission")
			}
			if mutation == "payload" {
				payload.(map[string]any)["message"].(map[string]any)["text"] = "authenticated"
				messageID := payload.(map[string]any)["message"].(map[string]any)["message_id"]
				if _, ok := messageID.(json.Number); !ok {
					t.Fatal("request evidence lost its numeric lexeme")
				}
				if _, _, err := plan.ProjectPublication(admitted, bundleHash, "."); err != nil {
					t.Fatalf("original content no longer consumes the same admission: %v", err)
				}
			}
		})
	}
}

func TestAdmittedRequestMetadataHasNoWritableOrSerializableAuthority(t *testing.T) {
	typeInfo := reflect.TypeFor[AdmittedRequest]()
	for index := range typeInfo.NumField() {
		if field := typeInfo.Field(index); field.IsExported() {
			t.Fatalf("authenticated metadata exposes writable field %s", field.Name)
		}
	}
	var absent AdmittedRequest
	if absent.ProviderEventID() != "" || absent.ProviderEventType() != "" || absent.SemanticContentDigest() != "" ||
		absent.Response() != nil || absent.AcknowledgeBeforeDispatch() {
		t.Fatal("absent admission fabricated metadata")
	}
}

func TestAuthenticatedChallengeReadbackCannotMutateAdmission(t *testing.T) {
	fixture := normalizedEventTestManifest()
	fixture.Secret.Required = true
	fixture.Signature = SignatureManifest{Type: signatureTypeHMACSHA256, Header: "X-Signature", SignedPayload: "raw_body"}
	fixture.Challenge = &ChallengeManifest{When: ConditionManifest{JSONPath: "$.challenge", Equals: "original"},
		Response: ResponseManifest{JSONPath: "$.challenge", Status: http.StatusOK, ContentType: "text/plain"}}
	catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: fixture.mustAdmit(), Source: "challenge-copy-proof",
		Identity: PackIdentity{ID: "provider.telegram", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("d", 64), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "telegram", SigningSecret: "signed-proof"})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"challenge":"original"}`)
	mac := hmac.New(sha256.New, []byte("signed-proof"))
	_, _ = mac.Write(body)
	admitted, err := plan.AdmitRequest(Request{Provider: "telegram", Target: Target{WebhookSecret: "signed-proof"}, Body: body,
		Payload: map[string]any{"challenge": "original"}, Headers: http.Header{"X-Signature": {hex.EncodeToString(mac.Sum(nil))}}})
	if err != nil {
		t.Fatal(err)
	}
	view := admitted.Response()
	if view == nil || string(view.Body) != "original" || admitted.ProviderEventID() != "" || admitted.SemanticContentDigest() != "" {
		t.Fatal("authenticated challenge fabricated delivery identity or lost its response")
	}
	view.Body[0] = 'X'
	view.Status = http.StatusTeapot
	for range 2 {
		delivery, err := plan.ProjectDelivery(admitted)
		if err != nil || delivery.Response == nil || delivery.Response.Status != http.StatusOK || string(delivery.Response.Body) != "original" || len(delivery.Events) != 0 {
			t.Fatalf("challenge readback changed its private authority: %+v, %v", delivery, err)
		}
		delivery.Response.Body[0] = 'X'
		if _, _, err := plan.ProjectPublication(admitted, "bundle-v2:sha256:"+strings.Repeat("a", 64), "."); err == nil {
			t.Fatal("challenge acquired business publication authority")
		}
	}
}
