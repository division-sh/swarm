package canonicalrouting

import (
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	"gopkg.in/yaml.v3"
)

// CopyInboundTransport preserves the isolated transport declarations. Native
// HTTP lifecycle and provider construction use their separate source fixtures.
func CopyInboundTransport(t testing.TB, flows []string, observe bool) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: inbound-fixture\n")
	events := []string{"inbound.telegram", "inbound.github.push", "inbound.github.issue_comment", "inbound.slack.message_channels", "inbound.slack.message", "inbound.stripe", "inbound.twilio", "inbound.shopify", "inbound.typeform", "inbound.intercom", "inbound.partner", "inbound.json"}
	flows = append([]string(nil), flows...)
	sort.Strings(flows)
	for i, flow := range flows {
		if i != 0 && flow == flows[i-1] {
			continue
		}
		switch flow {
		case "test-inbound-flow", "chat-flow", "ingress", "partner-flow", "json-flow", "chat", "coordinator":
		default:
			t.Fatalf("unclassified inbound transport flow %q", flow)
		}
		writeClosedVariantFile(t, root, flow+"/schema.yaml", "name: "+flow+"\npins:\n  inputs: ["+strings.Join(events, ", ")+"]\n")
		if observe {
			catalog := make(map[string]any, len(events))
			handlers := make(map[string]any, len(events))
			fields := make(map[string]string)
			for name, field := range providertriggers.RawEventCatalogEntry().Payload.Properties {
				fields[name] = field.Type
			}
			for _, event := range events {
				catalog[event] = fields
				handlers[event] = map[string]any{}
			}
			for name, document := range map[string]any{
				"events.yaml": catalog,
				"nodes.yaml":  map[string]any{"observer": map[string]any{"execution_type": "system_node", "subscribes_to": events, "event_handlers": handlers}},
			} {
				raw, err := yaml.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				writeClosedVariantFile(t, root, flow+"/"+name, string(raw))
			}
		}
	}
	return root
}
