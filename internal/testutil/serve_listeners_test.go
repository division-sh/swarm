package testutil

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestEphemeralServeListenerConfigHasOnlyExactLoopbackBindings(t *testing.T) {
	var document map[string]map[string]string
	if err := yaml.Unmarshal([]byte(EphemeralServeListenerConfig()), &document); err != nil {
		t.Fatal(err)
	}
	if len(document) != 1 || len(document["serve"]) != 2 ||
		document["serve"]["api_listen_addr"] != "127.0.0.1:0" ||
		document["serve"]["mcp_listen_addr"] != "127.0.0.1:0" {
		t.Fatalf("fixture listener policy changed or acquired unrelated configuration: %#v", document)
	}
}
