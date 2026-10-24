package canonicalrouting

import (
	"strings"
	"testing"
)

// CopyStandingRootTreePublic supplies two independent standing declarations
// with sibling receivers and a deep keyless descendant.
func CopyStandingRootTreePublic(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	const imports = "imports:\n  provider_trigger_events:\n    - {provider: telegram, event: inbound.telegram.text_message}\n    - {provider: telegram, event: inbound.telegram.callback_action}\n"
	const pins = "inputs:\n  - inbound.telegram\n  - inbound.telegram.text_message\n  - inbound.telegram.callback_action\n"
	const outputs = "  outputs: [inbound.telegram.text_message, inbound.telegram.callback_action]\n"
	const nodes = "observer:\n  inbound.telegram:\n    guard: true\n  inbound.telegram.text_message:\n    guard: true\n  inbound.telegram.callback_action:\n    guard: true\n"
	files := map[string]string{
		"events.yaml":                       "inbound.telegram:\n",
		"beta/events.yaml":                  "inbound.telegram:\n",
		"schema.yaml":                       "name: standing-root-tree\nstages: []\n" + imports + pins + outputs + "ingress:\n  alias: alpha\n  providers:\n    - {provider: telegram, signing_secret: webhook_signing.alpha}\nconnect:\n    - {event: inbound.telegram.text_message, from: ., to: alpha-receiver}\n    - {event: inbound.telegram.callback_action, from: ., to: alpha-receiver}\n    - {event: inbound.telegram.text_message, from: beta, to: beta-receiver}\n    - {event: inbound.telegram.callback_action, from: beta, to: beta-receiver}\n",
		"nodes.yaml":                        nodes,
		"beta/schema.yaml":                  "name: beta\nstages: []\n" + imports + pins + outputs + "ingress:\n  alias: beta\n  providers:\n    - {provider: telegram, signing_secret: webhook_signing.beta}\n",
		"beta/nodes.yaml":                   nodes,
		"alpha-receiver/detail/schema.yaml": "name: detail\n",
	}
	for _, alias := range []string{"alpha", "beta"} {
		files[alias+"-receiver/schema.yaml"] = "name: " + alias + "-receiver\n" + imports + "inputs: [inbound.telegram.text_message, inbound.telegram.callback_action]\n"
		files[alias+"-receiver/nodes.yaml"] = strings.Replace(nodes, "  inbound.telegram:\n    guard: true\n", "", 1)
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
