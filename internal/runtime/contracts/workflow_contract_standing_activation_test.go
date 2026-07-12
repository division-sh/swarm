package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProjectPackageDocumentStandingIngressStrictDecode(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: demo
flows:
  - id: chat
    flow: chat
    mode: singleton
    activation: standing
    ingress:
      alias: support
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
`), &doc); err != nil {
		t.Fatalf("Unmarshal standing ingress: %v", err)
	}
	if len(doc.Flows) != 1 || !doc.Flows[0].HasStandingActivation() || doc.Flows[0].Ingress == nil || doc.Flows[0].Ingress.Alias != "support" {
		t.Fatalf("standing flow = %#v", doc.Flows)
	}
	providers := doc.Flows[0].Ingress.Providers
	if len(providers) != 1 || providers[0].Provider != "telegram" || providers[0].SigningSecret != "webhook_signing.telegram" {
		t.Fatalf("standing providers = %#v", providers)
	}

	for _, tc := range []struct {
		name  string
		yaml  string
		field string
	}{
		{name: "flow field", field: "lifecycle", yaml: `
name: demo
flows:
  - id: chat
    flow: chat
    lifecycle: standing
`},
		{name: "ingress field", field: "route", yaml: `
name: demo
flows:
  - id: chat
    flow: chat
    activation: standing
    ingress:
      route: support
`},
		{name: "provider field", field: "secret", yaml: `
name: demo
flows:
  - id: chat
    flow: chat
    activation: standing
    ingress:
      providers:
        - provider: telegram
          secret: webhook_signing.telegram
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var invalid ProjectPackageDocument
			err := yaml.Unmarshal([]byte(tc.yaml), &invalid)
			if err == nil || !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("strict decode error = %v, want unsupported field %q", err, tc.field)
			}
		})
	}
}

func TestProjectPackageDocumentInboundAdmissionStrictDecode(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: demo
flows:
  - id: events
    flow: events
    mode: singleton
    activation: standing
    ingress:
      providers:
        - provider: partner-events
          signing_secret: webhook_signing.partner
          admission:
            kind: raw
            authentication:
              kind: hmac_sha256
              header: X-Partner-Signature
              prefix: sha256=
              encoding: hex
            event: inbound.partner
            delivery_id:
              source: json_path
              json_path: $.event_id
            payload: json
`), &doc); err != nil {
		t.Fatalf("Unmarshal raw admission: %v", err)
	}
	provider := doc.Flows[0].Ingress.Providers[0]
	if provider.Admission.Kind != "raw" || provider.Admission.Authentication == nil || provider.Admission.Authentication.Header != "X-Partner-Signature" || provider.Admission.DeliveryID == nil || provider.Admission.DeliveryID.JSONPath != "$.event_id" {
		t.Fatalf("raw admission = %#v", provider.Admission)
	}

	for _, tc := range []struct {
		name, block, field string
	}{
		{name: "admission", field: "fallback", block: "admission:\n            fallback: raw"},
		{name: "pack", field: "version", block: "admission:\n            pack:\n              id: provider.telegram\n              version: latest"},
		{name: "authentication", field: "algorithm", block: "admission:\n            kind: raw\n            authentication:\n              kind: token\n              algorithm: constant_time"},
		{name: "delivery_id", field: "query", block: "admission:\n            kind: raw\n            delivery_id:\n              source: header\n              query: id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "name: demo\nflows:\n  - id: chat\n    flow: chat\n    mode: singleton\n    activation: standing\n    ingress:\n      providers:\n        - provider: telegram\n          " + tc.block + "\n"
			var invalid ProjectPackageDocument
			err := yaml.Unmarshal([]byte(body), &invalid)
			if err == nil || !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("strict decode error = %v, want unsupported field %q", err, tc.field)
			}
		})
	}
}
