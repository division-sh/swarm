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
