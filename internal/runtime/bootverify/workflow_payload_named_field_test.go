package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRunAcceptsNormalizerReadingNestedPathBelowPayloadNamedField(t *testing.T) {
	// routing-example-census: different-concept issue=none owner=contracts.payload_named_field proof=internal/runtime/bootverify/workflow_payload_named_field_test.go:TestRunAcceptsNormalizerReadingNestedPathBelowPayloadNamedField
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: payload-normalizer
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: payload-normalizer
initial_state: active
states: [active, done]
terminal_states: [done]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), `
chat:
  chat_id: text
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
inbound.telegram:
  entity_id: text
  payload: json
  swarm:
    source: external
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
normalizer:
  id: normalizer
  execution_type: system_node
  subscribes_to: [inbound.telegram]
  event_handlers:
    inbound.telegram:
      data_accumulation:
        writes:
          - target_field: chat_id
            value:
              ref: payload.payload.message.chat.id
      advances_to: done
`)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, file), "{}\n")
	}
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "payload_field_coverage", "payload.payload.message.chat.id") || reportContains(report.Errors(), "payload_field_coverage", "payload.message.chat.id") {
		t.Fatalf("normalizer path rejected: %#v", report.Errors())
	}
}
