package releasee2e

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestGoldenRuntimeConfigUsesSharedEphemeralListeners(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store := goldenStoreSelection{name: backend, configYAML: "store:\n  backend: " + backend + "\n"}
			var document map[string]map[string]any
			if err := yaml.Unmarshal([]byte(goldenRuntimeConfig(store)), &document); err != nil {
				t.Fatal(err)
			}
			if len(document) != 5 || len(document["serve"]) != 2 ||
				document["serve"]["api_listen_addr"] != "127.0.0.1:0" ||
				document["serve"]["mcp_listen_addr"] != "127.0.0.1:0" ||
				document["store"]["backend"] != backend ||
				document["runtime"]["recovery_on_startup"] != true ||
				document["llm"]["backend"] != "claude_cli" ||
				document["workspace"]["backend"] != "host" {
				t.Fatalf("golden listener policy, backend or workload profile changed: %#v", document)
			}
		})
	}
}
