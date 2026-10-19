package providerconnectors

import (
	"strings"
	"testing"
)

func TestConnectorRetiredEffectDeclarationUsesSharedSlotTeaching(t *testing.T) {
	const teaching = "effect_class is not a tool key; a connector tool takes its class from the operation or registration slot it is bound to"
	for _, value := range []string{"read_only", "non_idempotent_write", "idempotent_write", "long_running", "null", "''", "false", "0", "[]", "{}"} {
		for _, merged := range []bool{false, true} {
			name := value
			declaration := "effect_class: " + value
			if merged {
				name += "/merged"
				declaration = "<<: {effect_class: " + value + "}"
			}
			t.Run(name, func(t *testing.T) {
				body := "provider: telegram\ntools:\n  telegram.inspect:\n    " + declaration + "\n    handler_type: http\n    http: {method: GET, url: 'https://proof.invalid'}\n"
				_, err := parseConnectorManifestAt([]byte(body), "connector.yaml")
				if err == nil || !strings.Contains(err.Error(), teaching) || !strings.Contains(err.Error(), "connector.yaml:4:") {
					t.Fatalf("connector declaration did not teach through shared admission at its source: %v", err)
				}
			})
		}
	}
}
