package providertriggers

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

const sessionTriggerFixture = `provider: acme
transport: session
payload_object_required: true
delivery_id: {json_path: '$.id', required: true}
event_type: {json_path: '$.kind', required: true}
event_name: {literal: inbound.acme}
normalized_events:
  - event: inbound.acme.message
    fields:
      text: {from: text, schema: {type: string}}
ack: {mode: durable_before_dispatch}
`

func sessionTriggerCatalog(t *testing.T) (*CatalogSnapshot, Manifest) {
	t.Helper()
	manifest, err := ParseManifest([]byte(sessionTriggerFixture))
	if err != nil {
		t.Fatal(err)
	}
	entry := CatalogEntry{Manifest: manifest,
		Identity: PackIdentity{ID: "provider.acme", Version: "1.0.0",
			ManifestHash: packs.ManifestHash([]byte(sessionTriggerFixture)), Provenance: "external"},
		Source: "session-transport-proof", SourcePath: "packs/acme/trigger.yaml"}
	catalog, err := NewCatalogSnapshot(entry)
	if err != nil {
		t.Fatal(err)
	}
	return catalog, manifest
}

func TestSessionTriggerTransportDoesNotGrantWebhookAdmission(t *testing.T) {
	catalog, manifest := sessionTriggerCatalog(t)
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme"})
	if err != nil || !plan.Valid() || plan.Transport() != packs.ChannelTransportSession || plan.RequiresSecret() || plan.AcknowledgedUnsigned() {
		t.Fatalf("session compilation = %+v, %v", plan, err)
	}
	for _, req := range []Request{
		{Provider: "acme", Payload: map[string]any{"id": "message", "kind": "message", "text": "hello"}},
		{Provider: "acme", Target: Target{WebhookSecret: "dummy"}, Headers: http.Header{"X-Signature": {"dummy"}},
			Payload: map[string]any{"id": "message", "kind": "message", "text": "hello"}},
	} {
		if _, err := plan.AdmitRequest(req); err == nil || !strings.Contains(err.Error(), "session") {
			t.Fatalf("session plan admitted HTTP/unsigned request: %v", err)
		}
		if _, err := manifest.Accept(req); err == nil || !strings.Contains(err.Error(), "session") {
			t.Fatalf("BODY bypass admitted HTTP/unsigned request: %v", err)
		}
	}
	if _, err := plan.ProjectDelivery(AdmittedRequest{}); err == nil {
		t.Fatal("empty authority projected session output")
	}
	for _, req := range []CompileAdmissionRequest{
		{Alias: "chat", Provider: "acme", SigningSecret: "dummy"},
		{Alias: "chat", Provider: "acme", Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement}},
		{Alias: "chat", Provider: "acme", Declaration: AdmissionDeclaration{Kind: "raw", Payload: "json"}},
	} {
		if plan, err := catalog.CompileAdmission(req); err == nil || plan.Valid() {
			t.Fatalf("session selected webhook/raw alternative: %+v, %v", plan, err)
		}
	}
}

func TestSessionTriggerCapabilityReadbackHasNoHTTPOrSecretClaim(t *testing.T) {
	catalog, _ := sessionTriggerCatalog(t)
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	subject, err := plan.EffectiveCapabilitySubject(EffectiveSubjectRequest{
		BundleHash: strings.Repeat("a", 64), FlowPath: ".", Alias: "chat"})
	if err != nil {
		t.Fatal(err)
	}
	if subject.Status != packs.StatusNotReady || len(subject.Requirements) != 0 ||
		subject.TriggerAdmission.RequestAuthentication != "SESSION_ACCOUNT" ||
		subject.TriggerAdmission.SignedPayload != "" || subject.TriggerAdmission.DigestEncoding != "" {
		t.Fatalf("unadmitted session readback = %+v", subject)
	}
	encoded, err := json.Marshal(subject.TriggerAdmission)
	if err != nil {
		t.Fatal(err)
	}
	var readback map[string]any
	if err := json.Unmarshal(encoded, &readback); err != nil || readback["transport"] != "session" {
		t.Fatalf("missing explicit session transport: %s, %v", encoded, err)
	}
	for _, subjects := range [][]packs.Subject{{subject}, mustInstalledTriggerSubjects(t, catalog)} {
		for _, value := range subjects {
			found := false
			for _, capability := range value.Capabilities {
				if capability.Code == packs.CapabilityReceiveHTTPSRoute || capability.Code == packs.CapabilityVerifySecret {
					t.Fatalf("session claims HTTP or signing: %+v", value)
				}
				found = found || capability.Code == "receive_session_events"
			}
			if !found || strings.Contains(packs.RenderSubject(value, false), "/webhooks/") {
				t.Fatalf("session capability projection is missing or misleading: %+v", value)
			}
		}
	}
	if text := packs.RenderEffectiveTriggerReadiness(subject); !strings.Contains(text, "session admission required") || strings.Contains(text, "UNAUTHENTICATED") {
		t.Fatalf("session readiness = %q", text)
	}
	for _, cell := range []struct {
		name string
		edit func(*packs.Subject)
	}{
		{"transport missing", func(s *packs.Subject) { s.TriggerAdmission.Transport = "" }},
		{"wrong transport", func(s *packs.Subject) { s.TriggerAdmission.Transport = packs.ChannelTransportWebhook }},
		{"unsigned", func(s *packs.Subject) { s.TriggerAdmission.RequestAuthentication = "UNAUTHENTICATED" }},
		{"signature", func(s *packs.Subject) { s.TriggerAdmission.SignedPayload = "raw_body" }},
		{"encoding", func(s *packs.Subject) { s.TriggerAdmission.DigestEncoding = "hex" }},
		{"secret", func(s *packs.Subject) {
			s.Requirements = []packs.Requirement{packs.TargetScopedRequirement(packs.RequirementSecret, "dummy")}
		}},
		{"HTTP route", func(s *packs.Subject) {
			s.Capabilities = append(s.Capabilities, packs.Capability{Code: packs.CapabilityReceiveHTTPSRoute, Target: "/webhooks/chat/acme"})
		}},
		{"duplicate session receive", func(s *packs.Subject) {
			s.Capabilities = append(s.Capabilities, packs.Capability{Code: packs.CapabilityReceiveSessionEvents, Target: "acme"})
		}},
		{"foreign session receive", func(s *packs.Subject) {
			s.Capabilities = append(s.Capabilities, packs.Capability{Code: packs.CapabilityReceiveSessionEvents, Target: "foreign"})
		}},
		{"ready without admission", func(s *packs.Subject) { s.Status = packs.StatusReady }},
	} {
		t.Run(cell.name, func(t *testing.T) {
			changed := packs.CloneSubjects([]packs.Subject{subject})[0]
			changed.Status = ""
			cell.edit(&changed)
			if _, err := packs.NormalizeSubjects([]packs.Subject{changed}); err == nil {
				t.Fatal("contradictory session readback admitted")
			}
		})
	}
	recovering := packs.CloneSubjects([]packs.Subject{subject})[0]
	recovering.TriggerAdmission.BindingBlockReason = "recovery_required"
	recovering.TriggerAdmission.RecoveryOperationID = "original-operation"
	recovering.TriggerAdmission.RecoveryCommand = "swarm channels retry original-operation"
	if text := packs.RenderEffectiveTriggerReadiness(recovering); !strings.Contains(text, recovering.TriggerAdmission.RecoveryCommand) || strings.Contains(text, "fresh credentials") {
		t.Fatalf("session recovery readback lost its owner or requires a dummy credential: %s", text)
	}
}

func mustInstalledTriggerSubjects(t *testing.T, catalog *CatalogSnapshot) []packs.Subject {
	t.Helper()
	subjects, err := catalog.InstalledCapabilitySubjects()
	if err != nil || len(subjects) != 1 {
		t.Fatalf("installed session subjects = %+v, %v", subjects, err)
	}
	return subjects
}

func TestSessionTriggerTransportRejectsHTTPPolicyWithExactSource(t *testing.T) {
	for _, cell := range []struct{ name, old, replacement string }{
		{"secret", "payload_object_required: true", "secret: {required: false}"},
		{"signature", "payload_object_required: true", "signature: {}"},
		{"challenge", "payload_object_required: true", "challenge: {when: {json_path: '$.kind', equals: challenge}, response: {json_path: '$.challenge'}}"},
		{"form", "payload_object_required: true", "payload_source: form"},
		{"header id", "delivery_id: {json_path: '$.id', required: true}", "delivery_id: {header: X-ID, required: true}"},
		{"query id", "delivery_id: {json_path: '$.id', required: true}", "delivery_id: {query_param: id, required: true}"},
		{"form kind", "event_type: {json_path: '$.kind', required: true}", "event_type: {form_param: kind, required: true}"},
		{"header kind", "event_type: {json_path: '$.kind', required: true}", "event_type: {header: X-Kind, required: true}"},
		{"optional id", "delivery_id: {json_path: '$.id', required: true}", "delivery_id: {json_path: '$.id'}"},
		{"optional kind", "event_type: {json_path: '$.kind', required: true}", "event_type: {json_path: '$.kind'}"},
		{"missing ack", "ack: {mode: durable_before_dispatch}", ""},
	} {
		t.Run(cell.name, func(t *testing.T) {
			body := strings.Replace(sessionTriggerFixture, cell.old, cell.replacement, 1)
			manifest, err := parseManifestAt([]byte(body), "packs/acme/trigger.yaml")
			if err == nil || manifest.Validate() == nil || !strings.Contains(err.Error(), "packs/acme/trigger.yaml:") {
				t.Fatalf("HTTP policy admitted into session BODY: %v", err)
			}
		})
	}
	for _, value := range []string{"null", "''", "false", "1", "{}", "[]", "' session '", "SESSION", "socket"} {
		body := strings.Replace(sessionTriggerFixture, "transport: session", "transport: "+value, 1)
		if _, err := parseManifestAt([]byte(body), "packs/acme/trigger.yaml"); err == nil || !strings.Contains(err.Error(), "transport") {
			t.Fatalf("invalid transport %s admitted: %v", value, err)
		}
	}
}

func TestTriggerWebhookTransportIsExplicitOrOmittedWithoutProviderGuessing(t *testing.T) {
	for _, provider := range []string{"acme", "whatsapp"} {
		for _, transport := range []string{"", "transport: webhook\n"} {
			body := transport + "provider: " + provider + "\nevent_name: {literal: inbound." + provider + "}\ndelivery_id: {literal: receipt, required: true}\nevent_type: {literal: message, required: true}\n"
			manifest, err := ParseManifest([]byte(body))
			if err != nil || manifest.Transport() != packs.ChannelTransportWebhook {
				t.Fatalf("webhook declaration = %+v, %v", manifest, err)
			}
			catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: manifest,
				Identity: PackIdentity{ID: "provider." + provider, Version: "1.0.0", ManifestHash: packs.ManifestHash([]byte(body)), Provenance: "external"}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: provider}); err == nil {
				t.Fatal("omitted signature guessed a session instead of requiring explicit unsigned acknowledgement")
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: provider,
				Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement}})
			if err != nil || plan.Transport() != packs.ChannelTransportWebhook || plan.RequestAuthentication() != RequestAuthenticationNone {
				t.Fatalf("explicit unsigned webhook = %+v, %v", plan, err)
			}
			if _, err := plan.AdmitRequest(Request{Payload: map[string]any{}}); err != nil {
				t.Fatal(err)
			}
		}
	}
}
