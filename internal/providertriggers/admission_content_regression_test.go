package providertriggers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

func TestSignedFormProjectionBindsBothContentViews(t *testing.T) {
	for _, normalized := range []bool{false, true} {
		for _, mutation := range []struct {
			name string
			edit func(Request)
		}{
			{"form value", func(r Request) { r.Form["Body"][0] = "changed" }},
			{"form identity", func(r Request) { r.Form["MessageSid"][0] = "changed" }},
			{"form shape", func(r Request) { r.Form["Body"] = append(r.Form["Body"], "changed") }},
			{"payload raw", func(r Request) { r.Payload.(map[string]any)["raw"] = "changed" }},
			{"payload nested", func(r Request) { r.Payload.(map[string]any)["nested"].(map[string]any)["text"] = "changed" }},
			{"payload array", func(r Request) { r.Payload.(map[string]any)["items"].([]any)[0] = "changed" }},
			{"payload number lexeme", func(r Request) { r.Payload.(map[string]any)["number"] = json.Number("1e3") }},
		} {
			name := mutation.name
			if normalized {
				name += "/normalized"
			} else {
				name += "/raw"
			}
			t.Run(name, func(t *testing.T) {
				plan, request := signedFormContentFixture(t, normalized)
				admitted, err := plan.AdmitRequest(request)
				if err != nil {
					t.Fatal(err)
				}
				identity := sha256.Sum256([]byte(`{"Body":"authenticated","MessageSid":"message-1"}`))
				if admitted.SemanticContentDigest() != hex.EncodeToString(identity[:]) {
					t.Fatal("form retry identity changed to the normalized-payload identity")
				}
				const bundle = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				delivery, err := plan.ProjectDelivery(admitted)
				if err != nil {
					t.Fatal(err)
				}
				published, _, err := plan.ProjectPublication(admitted, bundle, ".")
				if err != nil || !reflect.DeepEqual(delivery, published) {
					t.Fatalf("delivery/publication parity: %+v %+v %v", delivery, published, err)
				}
				if !reflect.DeepEqual(delivery.Events[0].Payload["payload"], map[string]any{"MessageSid": "message-1", "Body": "authenticated"}) {
					t.Fatalf("raw output no longer consumes form values: %+v", delivery)
				}
				if normalized {
					want := map[string]any{"text": request.Form.Encode(), "nested_text": "nested", "number": json.Number("1000")}
					if len(delivery.Events) != 2 || !reflect.DeepEqual(delivery.Events[1].Payload, want) {
						t.Fatalf("normalization changed its payload view or numeric lexemes: %+v", delivery)
					}
				} else if len(delivery.Events) != 1 {
					t.Fatalf("raw-only form gained normalized output: %+v", delivery)
				}
				mutation.edit(request)
				if _, err := plan.ProjectDelivery(admitted); err == nil {
					t.Fatal("mutated form/payload reached delivery projection")
				}
				if _, _, err := plan.ProjectPublication(admitted, bundle, "."); err == nil {
					t.Fatal("mutated form/payload acquired sealed publication authority")
				}
			})
		}
	}
}

func signedFormContentFixture(t *testing.T, normalized bool) (InboundAdmissionPlan, Request) {
	t.Helper()
	body := `provider: twilio
payload_source: form
secret: {required: true}
signature: {type: hmac_sha1, header: X-Twilio-Signature, signed_payload: url_plus_sorted_form, encoding: base64}
delivery_id: {form_param: MessageSid, required: true}
event_type: {literal: message_received, required: true}
event_name: {literal: inbound.twilio}
ack: {mode: durable_before_dispatch}
`
	if normalized {
		body += `normalized_events:
  - event: inbound.twilio.message
    fields:
      text: {from: raw, schema: {type: string}}
      nested_text: {from: nested.text, schema: {type: string}}
      number: {from: number, schema: {type: number}}
`
	}
	manifest, err := ParseManifest([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: manifest, Identity: PackIdentity{
		ID: "provider.twilio", Version: "1.0.0", ManifestHash: packs.ManifestHash([]byte(body)), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "twilio", SigningSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"MessageSid": {"message-1"}, "Body": {"authenticated"}}
	requestURL := "https://example.com/webhooks/chat/twilio"
	return plan, Request{Provider: "twilio", Target: Target{WebhookSecret: "secret"}, URL: requestURL,
		Body: []byte(form.Encode()), Payload: map[string]any{"raw": form.Encode(), "nested": map[string]any{"text": "nested"}, "items": []any{"item"}, "number": json.Number("1000")},
		Form: form, FormParsed: true, ContentType: "application/x-www-form-urlencoded",
		Headers: http.Header{"X-Twilio-Signature": {twilioSignatureBase64("secret", requestURL, form)}}}
}

func TestTriggerReadbackReceiverTransportMatrix(t *testing.T) {
	for _, mode := range []string{"signed HTTP", "unsigned HTTP", "session"} {
		catalog, _ := sessionTriggerCatalog(t)
		compile := CompileAdmissionRequest{Alias: "chat", Provider: "acme"}
		if mode == "signed HTTP" {
			plan, _ := signedFormContentFixture(t, true)
			checkTriggerReadbackReceiverMatrix(t, mode, plan)
			continue
		}
		if mode == "unsigned HTTP" {
			body := strings.Replace(sessionTriggerFixture, "transport: session", "transport: webhook", 1)
			manifest, err := ParseManifest([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			catalog, err = NewCatalogSnapshot(CatalogEntry{Manifest: manifest, Identity: PackIdentity{
				ID: "provider.acme", Version: "1.0.0", ManifestHash: packs.ManifestHash([]byte(body)), Provenance: "test"}})
			if err != nil {
				t.Fatal(err)
			}
			compile.Declaration.Acknowledge = UnsignedWebhookAcknowledgement
		}
		plan, err := catalog.CompileAdmission(compile)
		if err != nil {
			t.Fatal(err)
		}
		checkTriggerReadbackReceiverMatrix(t, mode, plan)
	}
}

func checkTriggerReadbackReceiverMatrix(t *testing.T, mode string, plan InboundAdmissionPlan) {
	t.Helper()
	request := EffectiveSubjectRequest{BundleHash: strings.Repeat("a", 64), FlowPath: ".", Alias: "chat"}
	if plan.RequiresSecret() {
		request.SigningSecret = "secret"
	}
	effective, err := plan.EffectiveCapabilitySubject(request)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: *plan.manifest, Identity: PackIdentity{
		ID: plan.packIdentity.ID(), Version: plan.packIdentity.Version(), ManifestHash: plan.packIdentity.ManifestHash(), Provenance: plan.packIdentity.Source().Provenance()}})
	if err != nil {
		t.Fatal(err)
	}
	installed := mustInstalledTriggerSubjects(t, catalog)[0]
	for _, original := range []packs.Subject{effective, installed} {
		t.Run(mode+"/"+original.Applicability, func(t *testing.T) {
			if _, err := packs.NormalizeSubjects([]packs.Subject{original}); err != nil {
				t.Fatalf("genuine compiled subject rejected: %v", err)
			}
			mutations := []string{"missing", "duplicate", "padded duplicate", "mixed", "foreign target"}
			if original.Applicability == "effective" {
				mutations = append(mutations, "reverse", "padded authentication")
			} else {
				mutations = append(mutations, "target admission")
			}
			if plan.Transport() == packs.ChannelTransportWebhook {
				mutations = append(mutations, "wrong scope")
			}
			for _, mutation := range mutations {
				t.Run(mutation, func(t *testing.T) {
					subject := packs.CloneSubjects([]packs.Subject{original})[0]
					subject.Status = ""
					index := -1
					for i, capability := range subject.Capabilities {
						if capability.Code == packs.CapabilityReceiveHTTPSRoute || capability.Code == packs.CapabilityReceiveSessionEvents {
							index = i
						}
					}
					if index == -1 {
						t.Fatal("compiled subject lacks receiver")
					}
					other := packs.Capability{Code: packs.CapabilityReceiveSessionEvents, Target: subject.Provider}
					if plan.Transport() == packs.ChannelTransportSession {
						alias := "{alias}"
						if subject.Applicability == "effective" {
							alias = "chat"
						}
						other = packs.Capability{Code: packs.CapabilityReceiveHTTPSRoute, Target: "/webhooks/" + alias + "/" + subject.Provider}
					}
					switch mutation {
					case "missing":
						subject.Capabilities = append(subject.Capabilities[:index], subject.Capabilities[index+1:]...)
					case "duplicate", "padded duplicate":
						duplicate := subject.Capabilities[index]
						if mutation == "padded duplicate" {
							duplicate.Code = " " + duplicate.Code + " "
						}
						subject.Capabilities = append(subject.Capabilities, duplicate)
					case "mixed":
						subject.Capabilities = append(subject.Capabilities, other)
					case "reverse":
						subject.Capabilities[index] = other
					case "foreign target":
						subject.Capabilities[index].Target = "foreign"
					case "wrong scope":
						alias := "chat"
						if subject.Applicability == "effective" {
							alias = "{alias}"
						}
						subject.Capabilities[index].Target = "/webhooks/" + alias + "/" + subject.Provider
					case "padded authentication":
						subject.TriggerAdmission.RequestAuthentication = " SESSION_ACCOUNT "
						if plan.Transport() == packs.ChannelTransportSession {
							subject.TriggerAdmission.RequestAuthentication = " UNAUTHENTICATED "
						}
					case "target admission":
						subject.TriggerAdmission = packs.CloneSubjects([]packs.Subject{effective})[0].TriggerAdmission
					}
					if _, err := packs.NormalizeSubjects([]packs.Subject{subject}); err == nil {
						t.Fatalf("contradictory %s readback admitted: %+v", mutation, subject)
					}
				})
			}
		})
	}
}

// The three reviewer probes are retained verbatim from the f92da4ee8 review.
func TestReviewerSignedFormProjectionMutation(t *testing.T) {
	body := `provider: twilio
payload_source: form
secret: {required: true}
signature: {type: hmac_sha1, header: X-Twilio-Signature, signed_payload: url_plus_sorted_form, encoding: base64}
delivery_id: {form_param: MessageSid, required: true}
event_type: {literal: message_received, required: true}
event_name: {literal: inbound.twilio}
normalized_events:
  - event: inbound.twilio.message
    fields:
      text: {from: raw, schema: {type: string}}
ack: {mode: durable_before_dispatch}
`
	manifest, err := ParseManifest([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: manifest, Identity: PackIdentity{ID: "provider.twilio", Version: "1.0.0", ManifestHash: packs.ManifestHash([]byte(body)), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "twilio", SigningSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"MessageSid": {"message-1"}, "Body": {"authenticated"}}
	requestURL := "https://example.com/webhooks/chat/twilio"
	payload := map[string]any{"raw": form.Encode()}
	request := Request{Provider: "twilio", Target: Target{WebhookSecret: "secret"}, URL: requestURL, Body: []byte(form.Encode()), Payload: payload, Form: form, FormParsed: true, ContentType: "application/x-www-form-urlencoded", Headers: http.Header{"X-Twilio-Signature": {twilioSignatureBase64("secret", requestURL, form)}}}
	admitted, err := plan.AdmitRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	before, _, err := plan.ProjectPublication(admitted, bundle, ".")
	if err != nil || len(before.Events) != 2 {
		t.Fatalf("positive control: %+v %v", before, err)
	}
	payload["raw"] = "not authenticated"
	after, _, err := plan.ProjectPublication(admitted, bundle, ".")
	if err == nil {
		t.Fatalf("changed payload acquired publication authority: before=%+v after=%+v", before.Events, after.Events)
	}
}

func TestReviewerWebhookReadbackRejectsSessionCapability(t *testing.T) {
	catalog, _ := sessionTriggerCatalog(t)
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	subject, err := plan.EffectiveCapabilitySubject(EffectiveSubjectRequest{BundleHash: strings.Repeat("a", 64), FlowPath: ".", Alias: "chat"})
	if err != nil {
		t.Fatal(err)
	}
	subject.Status = ""
	subject.TriggerAdmission.Transport = packs.ChannelTransportWebhook
	subject.TriggerAdmission.RequestAuthentication = "UNAUTHENTICATED"
	if result, err := packs.NormalizeSubjects([]packs.Subject{subject}); err == nil {
		t.Fatalf("webhook accepted session receive capability: %+v", result)
	}
}

func TestReviewerWebhookReadbackRejectsTrimmedSessionAuthentication(t *testing.T) {
	catalog, _ := sessionTriggerCatalog(t)
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "chat", Provider: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	subject, err := plan.EffectiveCapabilitySubject(EffectiveSubjectRequest{BundleHash: strings.Repeat("a", 64), FlowPath: ".", Alias: "chat"})
	if err != nil {
		t.Fatal(err)
	}
	subject.Status = ""
	subject.TriggerAdmission.Transport = packs.ChannelTransportWebhook
	subject.TriggerAdmission.RequestAuthentication = " SESSION_ACCOUNT "
	subject.Requirements = []packs.Requirement{packs.TargetScopedRequirement(packs.RequirementSecret, "signing")}
	for i := range subject.Capabilities {
		if subject.Capabilities[i].Code == packs.CapabilityReceiveSessionEvents {
			subject.Capabilities[i] = packs.Capability{Code: packs.CapabilityReceiveHTTPSRoute, Target: "/webhooks/chat/acme"}
		}
	}
	if result, err := packs.NormalizeSubjects([]packs.Subject{subject}); err == nil {
		t.Fatalf("webhook accepted whitespace-normalized session authentication: %+v auth=%s", result, result[0].TriggerAdmission.RequestAuthentication)
	}
}
