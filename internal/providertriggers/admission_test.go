package providertriggers

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
)

func TestManifestRejectsSignedOptionalSecretAndEmptyKeyExecution(t *testing.T) {
	for _, signatureType := range []string{signatureTypeHMACSHA256, signatureTypeHMACSHA1, signatureTypeTokenEquality} {
		t.Run(signatureType, func(t *testing.T) {
			manifest := admissionTestManifest("acme", signatureType, false)
			if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "signed request authentication requires secret.required true") {
				t.Fatalf("Validate error = %v", err)
			}
			_, err := manifest.Accept(Request{Provider: "acme", Headers: http.Header{"X-Signature": []string{"value"}}, Body: []byte(`{}`), Payload: map[string]any{}})
			requireProviderTriggerError(t, err, http.StatusUnauthorized)
		})
	}
}

func TestCatalogGenerationIsSemanticAndOrderIndependent(t *testing.T) {
	a := admissionTestEntry(admissionTestManifest("a", signatureTypeTokenEquality, true), "provider.a")
	b := admissionTestEntry(admissionTestManifest("b", signatureTypeHMACSHA256, true), "provider.b")
	first, err := NewCatalogSnapshot(a, b)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCatalogSnapshot(b, a)
	if err != nil {
		t.Fatal(err)
	}
	if first.GenerationID() == "" || first.GenerationID() != second.GenerationID() {
		t.Fatalf("generation ids = %q %q", first.GenerationID(), second.GenerationID())
	}
	b.Identity.ManifestHash = strings.Repeat("b", 64)
	changed, err := NewCatalogSnapshot(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if changed.GenerationID() == first.GenerationID() {
		t.Fatal("manifest identity change did not change catalog generation")
	}
}

func TestCatalogSnapshotDoesNotExposeMutableManifestState(t *testing.T) {
	manifest := admissionTestManifest("acme", signatureTypeHMACSHA256, true)
	manifest.Metadata = map[string]string{"delivery": "delivery_id"}
	entry := admissionTestEntry(manifest, "provider.acme")
	catalog, err := NewCatalogSnapshot(entry)
	if err != nil {
		t.Fatal(err)
	}

	manifest.Metadata["delivery"] = "event_type"
	returned, ok := catalog.EntryByProvider("acme")
	if !ok {
		t.Fatal("catalog entry missing")
	}
	returned.Manifest.Metadata["delivery"] = "user_agent"
	again, _ := catalog.EntryByProvider("acme")
	if got := again.Manifest.Metadata["delivery"]; got != "delivery_id" {
		t.Fatalf("catalog manifest metadata = %q, want immutable delivery_id", got)
	}
	entries := catalog.Entries()
	entries[0].Manifest.Metadata["delivery"] = "event_type"
	again, _ = catalog.EntryByID("provider.acme")
	if got := again.Manifest.Metadata["delivery"]; got != "delivery_id" {
		t.Fatalf("catalog Entries exposed mutable state: %q", got)
	}
}

func TestCompileAdmissionRequiresExplicitUnsignedPackAcknowledgement(t *testing.T) {
	for _, provenance := range []string{"platform", "external"} {
		t.Run(provenance, func(t *testing.T) {
			manifest := admissionTestManifest("acme", "", false)
			entry := admissionTestEntry(manifest, "provider.acme")
			entry.Identity.Provenance = provenance
			catalog, err := NewCatalogSnapshot(entry)
			if err != nil {
				t.Fatal(err)
			}
			_, err = catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme"})
			if err == nil || !strings.Contains(err.Error(), "admission.acknowledge: unsigned_webhook") {
				t.Fatalf("CompileAdmission error = %v", err)
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
				Alias: "chat", Provider: "acme",
				Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement},
			})
			if err != nil {
				t.Fatal(err)
			}
			identity, ok := plan.PackIdentity()
			if plan.PolicySource() != PolicySourceVerifiedPack || plan.RequestAuthentication() != RequestAuthenticationNone || !plan.AcknowledgedUnsigned() || !ok || identity.Provenance != provenance {
				t.Fatalf("plan = %+v identity=%+v/%t", plan, identity, ok)
			}
			if _, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme", SigningSecret: "unexpected", Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement}}); err == nil || !strings.Contains(err.Error(), "must not declare signing_secret") {
				t.Fatalf("unsigned signing-secret error = %v", err)
			}
		})
	}
}

func TestPackAuthenticationTransitionRequiresNewExplicitTargetContract(t *testing.T) {
	signedEntry := admissionTestEntry(admissionTestManifest("acme", signatureTypeTokenEquality, true), "provider.acme")
	signed, err := NewCatalogSnapshot(signedEntry)
	if err != nil {
		t.Fatal(err)
	}
	signedPlan, err := signed.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme", SigningSecret: "webhook_signing.acme"})
	if err != nil {
		t.Fatal(err)
	}
	unsignedEntry := admissionTestEntry(admissionTestManifest("acme", "", false), "provider.acme")
	unsignedEntry.Identity.ManifestHash = strings.Repeat("b", 64)
	unsigned, err := NewCatalogSnapshot(unsignedEntry)
	if err != nil {
		t.Fatal(err)
	}
	if signed.GenerationID() == unsigned.GenerationID() {
		t.Fatal("signed-to-unsigned pack transition retained catalog generation")
	}
	_, err = unsigned.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme", SigningSecret: "webhook_signing.acme"})
	if err == nil || !strings.Contains(err.Error(), "admission.acknowledge: unsigned_webhook") {
		t.Fatalf("unsigned transition error = %v", err)
	}
	unsignedPlan, err := unsigned.CompileAdmission(CompileAdmissionRequest{
		Alias: "chat", Provider: "acme", Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement},
	})
	if err != nil {
		t.Fatal(err)
	}
	if signedPlan.RequestAuthentication() != RequestAuthenticationTokenEquality || unsignedPlan.RequestAuthentication() != RequestAuthenticationNone {
		t.Fatalf("transition plans = %s -> %s", signedPlan.RequestAuthentication(), unsignedPlan.RequestAuthentication())
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
	} {
		t.Run(tc.signature, func(t *testing.T) {
			catalog, err := NewCatalogSnapshot(admissionTestEntry(admissionTestManifest("acme", tc.signature, true), "provider.acme"))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme", SigningSecret: "webhook_signing.acme"})
			if err != nil {
				t.Fatal(err)
			}
			if plan.RequestAuthentication() != tc.want || !plan.RequiresSecret() {
				t.Fatalf("authentication = %s requires=%t", plan.RequestAuthentication(), plan.RequiresSecret())
			}
			subject, err := plan.EffectiveCapabilitySubject(EffectiveSubjectRequest{BundleHash: strings.Repeat("a", 64), Alias: "chat", SigningSecret: "webhook_signing.acme"})
			if err != nil {
				t.Fatal(err)
			}
			if subject.TriggerAdmission == nil || subject.TriggerAdmission.RequestAuthentication != string(tc.want) || subject.TriggerAdmission.PolicySource != string(PolicySourceVerifiedPack) || len(subject.Requirements) != 1 {
				t.Fatalf("subject = %#v", subject)
			}
		})
	}
}

func TestRawAdmissionConsumesOnlyDeclaredAuthenticationAndDeliverySources(t *testing.T) {
	catalog, err := NewCatalogSnapshot()
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
	catalog, err := NewCatalogSnapshot(admissionTestEntry(admissionTestManifest("acme", signatureTypeTokenEquality, true), "provider.acme"))
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
	catalog, err := NewCatalogSnapshot()
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
	catalog, err := NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "partner", Provider: "partner-events",
		Declaration: AdmissionDeclaration{
			Kind: "raw", Acknowledge: UnsignedWebhookAcknowledgement,
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

func TestCompileAdmissionTeachingFailures(t *testing.T) {
	catalog, err := NewCatalogSnapshot(admissionTestEntry(admissionTestManifest("acme", signatureTypeTokenEquality, true), "provider.acme"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		req  CompileAdmissionRequest
		want string
	}{
		{name: "missing pack", req: CompileAdmissionRequest{Alias: "chat", Provider: "missing"}, want: "pack-required"},
		{name: "wrong id", req: CompileAdmissionRequest{Alias: "chat", Provider: "acme", Declaration: AdmissionDeclaration{PackID: "provider.acmee"}}, want: `verified pack for "acme" is "provider.acme"`},
		{name: "provider mismatch", req: CompileAdmissionRequest{Alias: "chat", Provider: "other", Declaration: AdmissionDeclaration{PackID: "provider.acme"}}, want: `which provides "acme"`},
		{name: "raw fields in pack", req: CompileAdmissionRequest{Alias: "chat", Provider: "acme", Declaration: AdmissionDeclaration{Event: "inbound.acme"}}, want: "remove raw-only fields"},
		{name: "invalid acknowledgement", req: CompileAdmissionRequest{Alias: "chat", Provider: "acme", SigningSecret: "secret", Declaration: AdmissionDeclaration{Acknowledge: "yes"}}, want: "canonical token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := catalog.CompileAdmission(tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileAdmission error = %v, want %q", err, tc.want)
			}
		})
	}
}

func admissionTestEntry(manifest Manifest, id string) CatalogEntry {
	return CatalogEntry{
		Identity: PackIdentity{ID: id, Version: "1.0.0", ManifestHash: strings.Repeat("a", 64), Provenance: "external"},
		Manifest: manifest, Source: "test", SourcePath: "/tmp/" + id,
	}
}

func admissionTestManifest(provider, signatureType string, secretRequired bool) Manifest {
	signature := SignatureManifest{Type: signatureType, Header: "X-Signature"}
	switch signatureType {
	case signatureTypeHMACSHA256:
		signature.Encoding = "hex"
		signature.SignedPayload = "raw_body"
	case signatureTypeHMACSHA1:
		signature.Encoding = "hex"
		signature.SignedPayload = "raw_body"
	case signatureTypeTokenEquality:
	default:
		signature = SignatureManifest{}
	}
	return Manifest{
		Provider: provider, Secret: SecretManifest{Required: secretRequired}, Signature: signature,
		DeliveryID: ValueSource{Header: "X-Delivery", Required: true}, EventType: ValueSource{Literal: "event", Required: true},
		EventName: EventNameManifest{Literal: "inbound." + provider}, Ack: AckManifest{Mode: "after_publish"},
	}
}

func signAdmissionTestHMACSHA1(secret string, body []byte) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
