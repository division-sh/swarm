package releasee2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const fullLifecycleFixtureSource = "standing_telegram"

func TestFullLifecycleJourneyMatrixIsClosed(t *testing.T) {
	want := map[string]fullLifecycleJourney{
		"J1-sqlite-graceful":                  {name: "J1-sqlite-graceful", backend: "sqlite", kind: fullLifecycleGraceful},
		"J2-postgres-graceful":                {name: "J2-postgres-graceful", backend: "postgres", kind: fullLifecycleGraceful},
		"J3-sqlite-crash-intrinsic-recover":   {name: "J3-sqlite-crash-intrinsic-recover", backend: "sqlite", kind: fullLifecycleCrashIntrinsic},
		"J4-postgres-crash-intrinsic-recover": {name: "J4-postgres-crash-intrinsic-recover", backend: "postgres", kind: fullLifecycleCrashIntrinsic},
		"J5-sqlite-dev-fresh":                 {name: "J5-sqlite-dev-fresh", backend: "sqlite", kind: fullLifecycleDevFresh},
	}
	if len(fullLifecycleJourneys) != len(want) {
		t.Fatalf("full lifecycle journey count = %d, want %d", len(fullLifecycleJourneys), len(want))
	}
	seen := make(map[string]bool, len(fullLifecycleJourneys))
	for _, journey := range fullLifecycleJourneys {
		if seen[journey.name] {
			t.Fatalf("duplicate full lifecycle journey %s", journey.name)
		}
		seen[journey.name] = true
		if expected, ok := want[journey.name]; !ok || journey != expected {
			t.Fatalf("unsupported full lifecycle journey %#v", journey)
		}
	}
}

func fullLifecycleFixtureRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(releaseE2ERepoRoot(t), "internal", "releasee2e", "testdata", "full_lifecycle")
}

func fullLifecycleExecutableSource(t *testing.T) string {
	t.Helper()
	return filepath.Join(fullLifecycleFixtureRoot(t), fullLifecycleFixtureSource)
}

func addFullLifecycleBusinessInput(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "schema.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Stage order defines the initial stage; retain it while changing only pins.
	schema := string(raw)
	for _, replacement := range [][2]string{
		{"  inputs:\n    - inbound.telegram.text_message", "  inputs:\n    - lifecycle.message"},
		{"  outputs:\n    - inbound.telegram.text_message", "  outputs:\n    - lifecycle.message_ready"},
		{"  - event: inbound.telegram.text_message\n    from: .\n    to: telegram-chat\n", "  - event: lifecycle.message_ready\n    from: .\n    to: telegram-chat\n    rename: inbound.telegram.text_message\n"},
	} {
		if strings.Count(schema, replacement[0]) != 1 {
			t.Fatalf("lifecycle fixture lost its exact business-input replacement: %q", replacement[0])
		}
		schema = strings.Replace(schema, replacement[0], replacement[1], 1)
	}
	writeReleaseFile(t, path, schema)
	for _, name := range []string{"events.yaml", "nodes.yaml", "types.yaml"} {
		if _, err := os.Stat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("lifecycle business input would overwrite %s: %v", name, err)
		}
	}
	writeReleaseFile(t, filepath.Join(root, "types.yaml"), "enums:\n  LifecycleConversationScope: {values: [direct, shared], default: direct}\n")
	writeReleaseFile(t, filepath.Join(root, "events.yaml"), `lifecycle.message:
  text: text
  external_account: text
  conversation_reference: text
  provider_message_reference: integer
lifecycle.message_ready:
  key: conversation_reference
  operator_account: {type: text, pattern: '^[0-9]+$', length: {min: 1}}
  text: {type: text, length: {min: 1, max: 4096}}
  external_account: {type: text, pattern: '^[0-9]+$', length: {min: 1, max: 17}}
  conversation_reference: {type: text, pattern: '^-?[0-9]+$', length: {min: 1, max: 17}}
  conversation_scope: LifecycleConversationScope
  provider_message_reference: {type: integer, range: {min: 1, max: 2147483647}}
`)
	writeReleaseFile(t, filepath.Join(root, "nodes.yaml"), `lifecycle-input:
  execution_type: system_node
  subscribes_to: [lifecycle.message]
  produces: [lifecycle.message_ready]
  event_handlers:
    lifecycle.message:
      emit:
        event: lifecycle.message_ready
        fields:
          operator_account: policy.operator_account
          text: payload.text
          external_account: payload.external_account
          conversation_reference: payload.conversation_reference
          conversation_scope: "direct"
          provider_message_reference: payload.provider_message_reference
`)
}

func mutateFullLifecycleFixtureWithoutExactConnectorResponse(t *testing.T, contracts string) {
	t.Helper()
	connectorPath := filepath.Join(contracts, "telegram", "connector.yaml")
	raw, err := os.ReadFile(connectorPath)
	if err != nil {
		t.Fatalf("read lifecycle connector BODY: %v", err)
	}
	var body struct {
		Tools map[string]map[string]any `yaml:"tools"`
	}
	if err := yaml.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	tool, present := body.Tools["send_message"]
	if !present || tool["category"] != "provider_connector" {
		t.Fatal("lifecycle connector BODY has no exact send_message connector")
	}
	tool["category"] = "provider_registration"
	mutated, err := yaml.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	writeReleaseFile(t, connectorPath, string(mutated))
}
