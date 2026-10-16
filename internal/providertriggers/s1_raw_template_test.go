package providertriggers

import (
	"net/http"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
)

func TestS1RawTemplatesUseDirectoryIdentityNotProtocolName(t *testing.T) {
	for _, key := range []string{"github", "slack", "github-sales", "github_sales", "9-team"} {
		for _, local := range []string{"{event_type}", "raw.{event_type}"} {
			t.Run(key+"/"+local, func(t *testing.T) {
				fixture := admissionTestManifest(key, signatureTypeTokenEquality, true)
				fixture.EventName = EventNameManifest{Template: "inbound." + key + "." + local}
				fixture.EventType.Literal = "Issue.Opened-V2"
				manifest, err := fixture.admit()
				if err != nil {
					t.Fatalf("raw-template policy inferred a protocol from provider key %q: %v", key, err)
				}
				catalog, err := NewCatalogSnapshot(CatalogEntry{
					Manifest: manifest, Identity: PackIdentity{ID: "test.trigger", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("a", 64), Provenance: "external"},
					Source: "test", SourcePath: "/trigger",
				})
				if err != nil {
					t.Fatal(err)
				}
				plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "shop", Provider: key, SigningSecret: "secret"})
				if err != nil {
					t.Fatal(err)
				}
				request := Request{Provider: key, Target: Target{WebhookSecret: "value"}, Headers: http.Header{"X-Signature": {"value"}, "X-Delivery": {"delivery-1"}}}
				delivery, err := plan.Accept(request)
				want := events.EventType("inbound." + key + "." + strings.ReplaceAll(local, "{event_type}", "issue_opened_v2"))
				if err != nil || len(delivery.Events) != 1 || delivery.Events[0].Name != want || delivery.ProviderEventType != "issue_opened_v2" {
					t.Fatalf("compiled raw-template output: %+v err=%v want=%s", delivery, err, want)
				}
				if delivery.Events[0].Kind != OutputKindRaw || !delivery.Events[0].Authorization.Empty() || delivery.Events[0].Payload["provider"] != key {
					t.Fatalf("template changed key, kind or authority: %+v", delivery.Events[0])
				}
				request.Headers.Set("X-Signature", "wrong")
				if _, err := plan.Accept(request); err == nil {
					t.Fatal("template admission weakened request authentication")
				}
			})
		}
	}
}

func TestS1RawTemplatesRefuseForeignRepeatedAndMalformedNames(t *testing.T) {
	for _, key := range []string{"github", "github-sales"} {
		prefix := "inbound." + key + "."
		for _, template := range []string{
			"inbound.foreign.{event_type}",
			prefix + "raw.{event_type}.{event_type}",
			prefix + "raw.{other}",
			prefix + "raw.{event_type} ",
			" " + prefix + "raw.{event_type}",
			prefix + "{event_type}.suffix",
			prefix + "raw.{event_type}/foreign",
			prefix + "*.{event_type}",
			prefix + "RAW.{event_type}",
		} {
			t.Run(key+"/"+template, func(t *testing.T) {
				fixture := admissionTestManifest(key, signatureTypeTokenEquality, true)
				fixture.EventName = EventNameManifest{Template: template}
				manifest, err := fixture.admit()
				if err == nil || manifest.Validate() == nil {
					t.Fatalf("invalid raw template %q gained policy", template)
				}
				if !strings.Contains(err.Error(), "event_name.template") || !strings.Contains(err.Error(), "raw.{event_type}") || !strings.Contains(err.Error(), "trigger.yaml:") {
					t.Fatalf("template refusal lost its policy, valid form or source: %v", err)
				}
			})
		}
	}
}
