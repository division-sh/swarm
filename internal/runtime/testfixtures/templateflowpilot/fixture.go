package templateflowpilot

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Options toggles the template-flow pilot variants used by conformance and
// runtime tests.
type Options struct {
	BadConnectMapping            bool
	UnsupportedReceiverSelection bool
	ProducerTarget               bool
	ProducerBroadcast            bool
}

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := Write(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "package.yaml"), `
name: template-flow-pilot
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: scoring
    flow: scoring
    mode: template
connect:
  - from: producer.validation_requested
    to: scoring.validation_requested
    delivery: one
`+connectAdapterYAML(opts))
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: template-flow-pilot\n")
	writeFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeProducer(t, root, opts)
	writeScoring(t, root, opts)
	return root
}

func connectAdapterYAML(opts Options) string {
	if !opts.BadConnectMapping {
		return ""
	}
	return `    using:
      instance:
        source: missing_account_id
        target: account_id
`
}

func writeProducer(t testing.TB, root string, opts Options) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  inputs:
    events:
      - name: intake_received
        event: intake.received
        source: external
  outputs:
    events:
      - name: validation_requested
        event: validation.requested
        key: account_id
        carries: [account_id, score, decision]
`)
	writeFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
intake.received:
  account_id: string
  score: string
  decision: string
validation.requested:
  account_id: string
  score: string
  decision: string
`)
	writeFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
intake-validator:
  id: intake-validator
  execution_type: system_node
  subscribes_to: [intake.received]
  produces: [validation.requested]
  event_handlers:
    intake.received:
      emit:
        event: validation.requested
`+producerEmitRouteYAML(opts)+`        fields:
          account_id: payload.account_id
          score: payload.score
          decision: payload.decision
`)
}

func producerEmitRouteYAML(opts Options) string {
	if opts.ProducerTarget {
		return `        target:
          flow: scoring
          match:
            account_id: payload.account_id
`
	}
	if opts.ProducerBroadcast {
		return "        broadcast: true\n"
	}
	return ""
}

func writeScoring(t testing.TB, root string, opts Options) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), `
name: scoring
mode: template
initial_state: pending
terminal_states: [done]
states: [pending, done]
instance:
  by: account_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events:
      - name: validation_requested
        event: validation.requested
  outputs:
    events: []
`)
	writeFile(t, filepath.Join(root, "flows", "scoring", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "scoring", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "scoring", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), `
validation.requested:
  account_id: string
  score: string
  decision: string
`)
	writeFile(t, filepath.Join(root, "flows", "scoring", "entities.yaml"), `
validation:
  account_id:
    type: string
  score:
    type: string
  decision:
    type: string
`)
	writeFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), `
scoring-handler:
  id: scoring-handler
  execution_type: system_node
  subscribes_to: [validation.requested]
  event_handlers:
    validation.requested:
`+receiverSelectionYAML(opts)+`      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
          - source_field: score
            target_field: score
          - source_field: decision
            target_field: decision
      advances_to: done
`)
}

func receiverSelectionYAML(opts Options) string {
	if !opts.UnsupportedReceiverSelection {
		return ""
	}
	return `      select_entity:
        by:
          account_id: payload.account_id
`
}

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func writeFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
