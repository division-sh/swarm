package providerconnectors

import (
	"strings"
	"testing"

	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const nativeConnectorProfileBody = `provider: whatsapp
tools:
  whatsapp.send_text:
    category: provider_connector
    handler_type: in_process
    in_process: whatsapp.send_text
    effect_class: non_idempotent_write
    input_schema: {type: object, properties: {destination: {type: string}, text: {type: string}}, required: [destination, text]}
    output_schema: {type: object, properties: {id: {type: string}}, required: [id]}
`

func TestNativeProviderProfileConsumesClosedTargetWithoutHTTPExceptions(t *testing.T) {
	manifest, err := ParseConnectorManifest([]byte(nativeConnectorProfileBody))
	if err != nil {
		t.Fatal(err)
	}
	if errs := validateTool("whatsapp.send_text", manifest.Tools["whatsapp.send_text"]); len(errs) != 0 {
		t.Fatal(errs)
	}
	for _, edit := range []struct{ from, to string }{
		{"  whatsapp.send_text:", "  slack.send_text:"},
		{"    in_process: whatsapp.send_text", "    in_process: caller.custom"},
		{"    category: provider_connector", "    category: provider_registration"},
		{"    effect_class: non_idempotent_write", "    effect_class: read_only"},
		{"    handler_type: in_process", "    handler_type: http"},
		{"    in_process: whatsapp.send_text", "    in_process: whatsapp.send_text\n    credentials: [dummy]"},
		{"    in_process: whatsapp.send_text", "    in_process: whatsapp.send_text\n    http: {method: POST, url: 'https://alternate.example'}"},
		{"    in_process: whatsapp.send_text", "    in_process: whatsapp.send_text\n    response_success: {kind: http_status_2xx}"},
		{"    in_process: whatsapp.send_text", "    in_process: whatsapp.send_text\n    rate_limit: 1/s"},
	} {
		t.Run(edit.to, func(t *testing.T) {
			candidate, err := ParseConnectorManifest([]byte(strings.Replace(nativeConnectorProfileBody, edit.from, edit.to, 1)))
			if err == nil && len(ValidateSource(semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: candidate.Tools}))) == 0 {
				t.Fatal("native declaration acquired foreign provider, effect or transport authority")
			}
		})
	}
	if errs := ValidateSource(semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: manifest.Tools})); len(errs) != 0 {
		t.Fatal("flow-local native declaration disagrees with packed declaration", errs)
	}
}

func TestNativeProviderProfileNeverGetsSchemaGeneratedMockExecution(t *testing.T) {
	manifest, err := ParseConnectorManifest([]byte(nativeConnectorProfileBody))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: manifest.Tools})
	plan, err := CompileMockResponsePlan(source)
	if err != nil || plan != nil {
		t.Fatal("native-only source acquired an HTTP mock response plan", plan, err)
	}
	foreign, err := ParseConnectorManifest([]byte(strings.Replace(nativeConnectorProfileBody, "  whatsapp.send_text:", "  slack.send_text:", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CompileMockResponsePlan(semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: foreign.Tools})); err == nil {
		t.Fatal("mock eligibility silently skipped an invalid native declaration")
	}
}
