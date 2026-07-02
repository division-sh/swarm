package fanoutpinroute

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Options toggles the fan_out.emit pin-route fixture variants used by
// conformance and verifier tests.
type Options struct {
	OmitOutputPin     bool
	OmitConnect       bool
	BadConnectMapping bool
	MissingEmitCarry  bool
	ProducerTarget    bool
	ProducerBroadcast bool
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
name: fanout-pin-route
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: coordinator
    flow: coordinator
    mode: singleton
  - id: account
    flow: account
    mode: template
`+connectYAML(opts))
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: fanout-pin-route\n")
	writeFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeCoordinator(t, root, opts)
	writeAccount(t, root)
	return root
}

func connectYAML(opts Options) string {
	if opts.OmitConnect {
		return ""
	}
	out := `
connect:
  - from: coordinator.account_classified
    to: account.account_classified
    delivery: one
`
	if opts.BadConnectMapping {
		out += `    using:
      instance:
        source: missing_account_id
        target: account_id
`
	}
	return out
}

func writeCoordinator(t testing.TB, root string, opts Options) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "coordinator", "schema.yaml"), `
name: coordinator
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: batch_classify_completed
        event: batch.classify.completed
  outputs:
`+coordinatorOutputPinYAML(opts))
	writeFile(t, filepath.Join(root, "flows", "coordinator", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "coordinator", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "coordinator", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "coordinator", "types.yaml"), `
types:
  ClassificationResult:
    account_id: text
    bucket: text
    score: integer
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "entities.yaml"), `
coordinator_state:
  coordinator_id: text
  classifications:
    type: map[text]ClassificationResult
    _unused_reason: singleton coordinator fan-out route proof field
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "events.yaml"), `
batch.classify.completed:
  coordinator_id: text
  results: "[ClassificationResult]"
account.classified:
  account_id: text
  bucket: text
  score: integer
  required: [account_id, bucket, score]
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "nodes.yaml"), `
classifier-coordinator:
  id: classifier-coordinator
  execution_type: system_node
  subscribes_to: [batch.classify.completed]
  event_handlers:
    batch.classify.completed:
      select_entity:
        by:
          coordinator_id: payload.coordinator_id
      data_accumulation:
        writes:
          - source_field: coordinator_id
            target_field: coordinator_id
      fan_out:
        items_from: payload.results
        emit:
          event: account.classified
`+producerRouteYAML(opts)+`          fields:
            account_id: fan_out.item.account_id
`+emitCarryFieldsYAML(opts))
}

func coordinatorOutputPinYAML(opts Options) string {
	if opts.OmitOutputPin {
		return "    events: []\n"
	}
	return `    events:
      - name: account_classified
        event: account.classified
        key: account_id
        carries: [account_id, bucket, score]
`
}

func producerRouteYAML(opts Options) string {
	if opts.ProducerTarget {
		return `          target:
            flow: account
            match:
              account_id: fan_out.item.account_id
`
	}
	if opts.ProducerBroadcast {
		return "          broadcast: true\n"
	}
	return ""
}

func emitCarryFieldsYAML(opts Options) string {
	if opts.MissingEmitCarry {
		return ""
	}
	return `            bucket: fan_out.item.bucket
            score: fan_out.item.score
`
}

func writeAccount(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "account", "schema.yaml"), `
name: account
mode: template
initial_state: awaiting_classification
terminal_states: [classified]
states: [awaiting_classification, classified]
instance:
  by: account_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events:
      - name: account_classified
        event: account.classified
  outputs:
    events: []
`)
	writeFile(t, filepath.Join(root, "flows", "account", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "account", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "account", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "account", "events.yaml"), `
account.classified:
  account_id: text
  bucket: text
  score: integer
`)
	writeFile(t, filepath.Join(root, "flows", "account", "entities.yaml"), `
account_state:
  account_id: text
  bucket: text
  score: integer
`)
	writeFile(t, filepath.Join(root, "flows", "account", "nodes.yaml"), `
account-classifier:
  id: account-classifier
  execution_type: system_node
  subscribes_to: [account.classified]
  event_handlers:
    account.classified:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
          - source_field: bucket
            target_field: bucket
          - source_field: score
            target_field: score
      advances_to: classified
`)
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
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
