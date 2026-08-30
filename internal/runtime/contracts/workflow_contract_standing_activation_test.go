package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestFlowSchemaStandingIngressStrictDecode(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: chat
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
	if doc.Activation != FlowActivationStanding || doc.Ingress == nil || doc.Ingress.Alias != "support" {
		t.Fatalf("standing flow = %#v", doc)
	}
	providers := doc.Ingress.Providers
	if len(providers) != 1 || providers[0].Provider != "telegram" || providers[0].SigningSecret != "webhook_signing.telegram" {
		t.Fatalf("standing providers = %#v", providers)
	}

	for _, tc := range []struct {
		name  string
		yaml  string
		field string
	}{
		{name: "flow field", field: "lifecycle", yaml: "name: chat\nlifecycle: standing\n"},
		{name: "ingress field", field: "route", yaml: "name: chat\nactivation: standing\ningress:\n  route: support\n"},
		{name: "provider field", field: "secret", yaml: "name: chat\nactivation: standing\ningress:\n  providers:\n    - provider: telegram\n      secret: webhook_signing.telegram\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var invalid FlowSchemaDocument
			err := yaml.Unmarshal([]byte(tc.yaml), &invalid)
			if err == nil || !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("strict decode error = %v, want unsupported field %q", err, tc.field)
			}
		})
	}
}

func TestFlowSchemaInboundAdmissionStrictDecode(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: events
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
	provider := doc.Ingress.Providers[0]
	if provider.Admission.Kind != "raw" || provider.Admission.Authentication == nil || provider.Admission.Authentication.Header != "X-Partner-Signature" || provider.Admission.DeliveryID == nil || provider.Admission.DeliveryID.JSONPath != "$.event_id" {
		t.Fatalf("raw admission = %#v", provider.Admission)
	}

	for _, tc := range []struct {
		name, block, field string
	}{
		{name: "admission", field: "fallback", block: "admission:\n      fallback: raw"},
		{name: "pack", field: "version", block: "admission:\n      pack:\n        id: provider.telegram\n        version: latest"},
		{name: "authentication", field: "algorithm", block: "admission:\n      kind: raw\n      authentication:\n        kind: token\n        algorithm: constant_time"},
		{name: "delivery_id", field: "query", block: "admission:\n      kind: raw\n      delivery_id:\n        source: header\n        query: id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := strings.ReplaceAll(tc.block, "\n", "\n      ")
			body := "name: chat\nmode: singleton\nactivation: standing\ningress:\n  providers:\n    - provider: telegram\n      " + block + "\n"
			var invalid FlowSchemaDocument
			err := yaml.Unmarshal([]byte(body), &invalid)
			if err == nil || !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("strict decode error = %v, want unsupported field %q", err, tc.field)
			}
		})
	}
}
