package templateselectorcreate

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	PackageName = "template-select-or-create"

	ProducerFlowID    = "producer"
	ProducerOutputPin = "account_ready"
	ProducerOutput    = "account.ready"

	TemplateFlowID     = "account"
	TemplateInputPin   = "account_ready"
	TemplateInput      = "account.ready"
	TemplateInstanceBy = "account_id"
)

func LoadBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := Write(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadSource(t testing.TB) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t))
}

func Write(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeRoot(t, root)
	writeProducer(t, root)
	writeTemplate(t, root)
	return root
}

func writeRoot(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "package.yaml"), `
name: template-select-or-create
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: account
    flow: account
    mode: template
connect:
  - from: producer.account_ready
    to: account.account_ready
    delivery: one
`)
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: template-select-or-create\n")
	writeFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
}

func writeProducer(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: account_ready
        event: account.ready
`)
	writeFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
account.ready:
  account_id: string
`)
	writeFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
}

func writeTemplate(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "account", "schema.yaml"), `
name: account
mode: template
instance:
  by: account_id
  on_missing: reject
  on_conflict: reject
pins:
  inputs:
    events:
      - name: account_ready
        event: account.ready
        resolution:
          mode: select-or-create
          instance_key: account_id
        carries:
          account_id:
            from: payload.account_id
            type: string
`)
	writeFile(t, filepath.Join(root, "flows", "account", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "account", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "account", "events.yaml"), `
account.ready:
  account_id: string
`)
	writeFile(t, filepath.Join(root, "flows", "account", "entities.yaml"), `
account_state:
  account_id:
    type: string
    _unused_reason: template-select-or-create route key fixture field
`)
	writeFile(t, filepath.Join(root, "flows", "account", "nodes.yaml"), `
account-node:
  id: account-node-{instance_id}
  execution_type: system_node
  event_handlers:
    account.ready: {}
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
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
