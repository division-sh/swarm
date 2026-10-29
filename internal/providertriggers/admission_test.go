package providertriggers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

func TestA9CompileAdmissionUsesExactAlias(t *testing.T) {
	catalog, err := NewCatalogSnapshotFromAdmittedBodies(admissionTestEntry(admissionTestManifest("acme", signatureTypeTokenEquality, true), "provider.acme").Manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"", " support", "support ", "/support", "support/", "support/child", ".support", "support%2Fchild"} {
		if plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: alias, Provider: "acme", SigningSecret: "webhook_signing.acme"}); err == nil || plan.Valid() {
			t.Fatalf("unadmitted alias %q was normalized into a plan: %v", alias, err)
		}
	}
	if plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "shop.support", Provider: "acme", SigningSecret: "webhook_signing.acme"}); err != nil || !plan.Valid() {
		t.Fatalf("rooted alias was rejected: %v", err)
	}
}

func TestManifestRejectsSignedOptionalSecretAndEmptyKeyExecution(t *testing.T) {
	for _, signatureType := range []string{signatureTypeHMACSHA256, signatureTypeHMACSHA1, signatureTypeTokenEquality} {
		t.Run(signatureType, func(t *testing.T) {
			manifest := admissionTestManifest("acme", signatureType, false)
			if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "signed request authentication requires secret.required true") {
				t.Fatalf("Validate error = %v", err)
			}
			manifest.Secret.Required = true
			_, err := manifest.Accept(Request{Provider: "acme", Headers: http.Header{"X-Signature": []string{"value"}}, Body: []byte(`{}`), Payload: map[string]any{}})
			requireProviderTriggerError(t, err, http.StatusUnauthorized)
		})
	}
}

func TestCompileAdmissionProjectsExactPackAuthentication(t *testing.T) {
	for _, tc := range []struct {
		signature string
		want      RequestAuthentication
	}{
		{signatureTypeTokenEquality, RequestAuthenticationTokenEquality},
		{signatureTypeHMACSHA256, RequestAuthenticationHMACSHA256},
		{signatureTypeHMACSHA1, RequestAuthenticationHMACSHA1},
		{"", RequestAuthenticationNone},
	} {
		t.Run(tc.signature, func(t *testing.T) {
			requiresSecret := tc.want != RequestAuthenticationNone
			secret, requirements := "", 0
			if requiresSecret {
				secret, requirements = "webhook_signing.acme", 1
			}
			catalog, err := NewCatalogSnapshotFromAdmittedBodies(admissionTestEntry(admissionTestManifest("acme", tc.signature, requiresSecret), "provider.acme").Manifest)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme", SigningSecret: secret})
			if err != nil {
				t.Fatal(err)
			}
			if plan.RequestAuthentication() != tc.want || plan.RequiresSecret() != requiresSecret {
				t.Fatalf("authentication = %s requires=%t", plan.RequestAuthentication(), plan.RequiresSecret())
			}
			subject, err := plan.EffectiveCapabilitySubject(EffectiveSubjectRequest{BundleHash: strings.Repeat("a", 64), FlowPath: ".", Alias: "chat", SigningSecret: secret})
			if err != nil {
				t.Fatal(err)
			}
			if subject.TriggerAdmission == nil || subject.TriggerAdmission.RequestAuthentication != string(tc.want) || subject.TriggerAdmission.PolicySource != string(PolicySourceVerifiedPack) || len(subject.Requirements) != requirements {
				t.Fatalf("subject = %#v", subject)
			}
		})
	}
}

func TestRawAdmissionConsumesOnlyDeclaredAuthenticationAndDeliverySources(t *testing.T) {
	catalog, err := NewCatalogSnapshotFromAdmittedBodies()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "partner", Provider: "partner-events", SigningSecret: "webhook_signing.partner",
		Declaration: AdmissionDeclaration{
			Kind: "raw", Event: "inbound.partner", Payload: "json",
			Authentication: RawAuthenticationDeclaration{Kind: "hmac_sha256", Header: "X-Partner-Signature", Encoding: "hex"},
			DeliveryID:     RawDeliveryIDDeclaration{Source: "header", Header: "X-Partner-Delivery"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"payload-must-not-own-id","value":1}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(body)
	headers := http.Header{
		"X-Partner-Signature": []string{hex.EncodeToString(mac.Sum(nil))},
		"X-Partner-Delivery":  []string{"declared-id"},
		"Stripe-Signature":    []string{"ignored"},
		"X-Github-Event":      []string{"ignored"},
	}
	delivery, err := plan.Accept(Request{Provider: "partner-events", Target: Target{WebhookSecret: "secret"}, Headers: headers, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if delivery.ProviderEventID != "declared-id" || delivery.Events[0].Name != "inbound.partner" {
		t.Fatalf("delivery = %+v", delivery)
	}
	headers["X-Partner-Delivery"] = []string{"one", "two"}
	if _, err := plan.Accept(Request{Provider: "partner-events", Target: Target{WebhookSecret: "secret"}, Headers: headers, Body: body}); err == nil {
		t.Fatal("duplicate declared delivery header accepted")
	}
}

func TestRawProviderCannotShadowInstalledPack(t *testing.T) {
	catalog, err := NewCatalogSnapshotFromAdmittedBodies(admissionTestEntry(admissionTestManifest("acme", signatureTypeTokenEquality, true), "provider.acme").Manifest)
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "chat", Provider: "acme",
		Declaration: AdmissionDeclaration{Kind: "raw", Event: "inbound.acme", Payload: "raw", Authentication: RawAuthenticationDeclaration{Kind: "none"}, DeliveryID: RawDeliveryIDDeclaration{Source: "body_sha256"}},
	})
	if err == nil || !strings.Contains(err.Error(), `rename the intentional raw namespace to "acme-raw"`) {
		t.Fatalf("CompileAdmission error = %v", err)
	}
}

func TestRawAdmissionBase64SignatureComparisonIsCaseSensitive(t *testing.T) {
	catalog, err := NewCatalogSnapshotFromAdmittedBodies()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "partner", Provider: "partner-events", SigningSecret: "webhook_signing.partner",
		Declaration: AdmissionDeclaration{
			Kind: "raw", Event: "inbound.partner", Payload: "raw",
			Authentication: RawAuthenticationDeclaration{Kind: "hmac_sha256", Header: "X-Signature", Encoding: "base64"},
			DeliveryID:     RawDeliveryIDDeclaration{Source: "body_sha256"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("case-sensitive")
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(body)
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	mutated := strings.ToLower(signature)
	if mutated == signature {
		t.Fatal("test vector unexpectedly has no uppercase Base64 characters")
	}
	_, err = plan.Accept(Request{
		Provider: "partner-events", Target: Target{WebhookSecret: "secret"}, Body: body,
		Headers: http.Header{"X-Signature": []string{mutated}},
	})
	requireProviderTriggerError(t, err, http.StatusUnauthorized)
}

func TestRawAdmissionPreservesRawPayloadWhileResolvingJSONPathDeliveryID(t *testing.T) {
	catalog, err := NewCatalogSnapshotFromAdmittedBodies()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "partner", Provider: "partner-events",
		Declaration: AdmissionDeclaration{
			Kind:  "raw",
			Event: "inbound.partner", Payload: "raw",
			Authentication: RawAuthenticationDeclaration{Kind: "none"},
			DeliveryID:     RawDeliveryIDDeclaration{Source: "json_path", JSONPath: "$.delivery.id"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"delivery":{"id":"evt-raw-json-path"},"value":1}`)
	delivery, err := plan.Accept(Request{Provider: "partner-events", Body: body})
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if delivery.ProviderEventID != "evt-raw-json-path" {
		t.Fatalf("delivery id = %q", delivery.ProviderEventID)
	}
	data, ok := delivery.Events[0].Payload["data"].(string)
	if !ok || data != string(body) {
		t.Fatalf("raw emitted payload = %#v, want exact body string", delivery.Events[0].Payload["data"])
	}
	if _, err := plan.Accept(Request{Provider: "partner-events", Body: []byte("not-json")}); err == nil || !strings.Contains(err.Error(), "requires a valid JSON request body") {
		t.Fatalf("invalid JSON error = %v", err)
	}
}

func admissionTestEntry(manifest triggerFixture, id string) CatalogEntry {
	admitted := manifest.mustAdmit()
	identity, err := packs.NewProviderBodyIdentity(admitted.value.providerBody)
	if err != nil {
		panic(err)
	}
	return catalogEntryValue{identity: identity, manifest: admitted, source: admitted.value.providerBody.Label()}.readback()
}

func admissionTestManifest(provider, signatureType string, secretRequired bool) triggerFixture {
	signature := SignatureManifest{Type: signatureType, Header: "X-Signature"}
	switch signatureType {
	case signatureTypeHMACSHA256:
		signature.SignedPayload = "raw_body"
	case signatureTypeHMACSHA1:
		signature.SignedPayload = "raw_body"
	case signatureTypeTokenEquality:
	default:
		signature = SignatureManifest{}
	}
	return triggerFixture{
		Provider: provider, Secret: SecretManifest{Required: secretRequired}, Signature: signature,
		DeliveryID: ValueSource{Header: "X-Delivery", Required: true}, EventType: ValueSource{Literal: "event", Required: true},
		EventName: EventNameManifest{Literal: "raw"},
	}
}
