package providertriggers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
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
	admitted.ProviderEventID = "different"
	if _, _, err := plan.ProjectPublication(admitted, bundleHash, "."); err == nil {
		t.Fatal("mutated admitted identity acquired publication authority")
	}
}
