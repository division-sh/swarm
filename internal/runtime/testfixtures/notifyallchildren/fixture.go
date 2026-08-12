package notifyallchildren

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	OwnerFlowID            = "portfolio"
	OwnerOutputPin         = "account_notify_requested"
	OwnerTriggerEvent      = "portfolio.notify.requested"
	NotifyEvent            = "account.notify.requested"
	ChildFlowID            = "account"
	ChildInputPin          = "account_notify_requested"
	canonicalAccountAgents = `account-worker:
  type: generic
  role: account_worker
  intent: prompts/account-worker.md
  model: regular
  memory: false
  subscriptions:
    - account.notify.requested
  emit_events:
    - account.notification.completed
  mock:
    kind: python
    module: mocks/account-worker.py
`
)

// Options produces verifier-only corruptions of the checked-in canonical example.
type Options struct {
	OmitOutputPin               bool
	OmitConnect                 bool
	MissingEmitCarry            bool
	ProducerTarget              bool
	ProducerBroadcast           bool
	ObjectMembership            bool
	UndeclaredPayloadMembership bool
	ExplicitAgentName           bool
	AgentTopologyRevision       int
	AutoEmitOnCreate            bool
	AutoEmitEventRevision       int
}

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := LoadBundleResult(t, opts)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadBundleResult(t testing.TB, opts Options) (*runtimecontracts.WorkflowContractBundle, error) {
	t.Helper()
	root := ExampleRoot(t)
	if opts != (Options{}) {
		root = WriteVariant(t, opts)
	}
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func ExampleRoot(t testing.TB) string {
	t.Helper()
	return filepath.Join(repoRoot(t), "examples", "routing", "notify-all-children")
}

func WriteVariant(t testing.TB, opts Options) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, ExampleRoot(t), root)

	packageFile := filepath.Join(root, "package.yaml")
	ownerSchema := filepath.Join(root, "flows", OwnerFlowID, "schema.yaml")
	ownerNodes := filepath.Join(root, "flows", OwnerFlowID, "nodes.yaml")
	ownerEntities := filepath.Join(root, "flows", OwnerFlowID, "entities.yaml")
	ownerTypes := filepath.Join(root, "flows", OwnerFlowID, "types.yaml")
	accountAgents := filepath.Join(root, "flows", ChildFlowID, "agents.yaml")
	accountEvents := filepath.Join(root, "flows", ChildFlowID, "events.yaml")
	accountSchema := filepath.Join(root, "flows", ChildFlowID, "schema.yaml")
	if opts.OmitConnect {
		replaceFile(t, packageFile, `  - from: portfolio.account_notify_requested
    to: account.account_notify_requested
`, "")
	}
	if opts.OmitOutputPin {
		replaceFile(t, ownerSchema, `      - name: account_notify_requested
        event: account.notify.requested
        key: account_id
        carries: [account_id, command]
`, "")
	}
	if opts.MissingEmitCarry {
		replaceFile(t, ownerNodes, "            command: payload.command\n", "")
	}
	if opts.ProducerTarget {
		replaceFile(t, ownerNodes, "          event: account.notify.requested\n", `          event: account.notify.requested
          target:
            flow: account
            match:
              account_id: account_id
`)
	}
	if opts.ProducerBroadcast {
		replaceFile(t, ownerNodes, "          event: account.notify.requested\n", "          event: account.notify.requested\n          broadcast: true\n")
	}
	if opts.ObjectMembership {
		replaceFile(t, ownerEntities, `  account_ids: "[text]"
`, `  account_ids: "[AccountRef]"
`)
		replaceFile(t, ownerTypes, "{}\n", `types:
  AccountRef:
    account_id: text
`)
		replaceFile(t, ownerNodes, "            account_id: account_id\n", "            account_id: account_id.account_id\n")
	}
	if opts.UndeclaredPayloadMembership {
		replaceFile(t, ownerNodes, "        items_from: entity.account_ids\n", `        items_from: payload.account_ids
        identity: account_id
`)
	}
	if opts.AutoEmitOnCreate {
		replaceFile(t, accountEvents, `account.notify.requested:
`, `account.created:
  account_id: text
  template_instance_key: text
  template_instance_source_event: text
  required: [account_id]
account.notify.requested:
`)
		replaceFile(t, accountSchema, `states: [active, completed]
`, `states: [active, completed]
auto_emit_on_create:
  event: account.created
`)
		replaceFile(t, accountSchema, `  outputs:
    events: []
`, `  outputs:
    events:
      - name: account_created
        event: account.created
        key: account_id
        carries: [account_id]
`)
	}
	if opts.AutoEmitEventRevision == 2 {
		replaceFile(t, accountEvents, "account.created", "account.revised")
		replaceFile(t, accountSchema, "account.created", "account.revised")
		replaceFile(t, accountSchema, "account.created", "account.revised")
	} else if opts.AutoEmitEventRevision != 0 && opts.AutoEmitEventRevision != 1 {
		t.Fatalf("unsupported auto-emit event revision %d", opts.AutoEmitEventRevision)
	}
	switch opts.AgentTopologyRevision {
	case 0:
	case 1:
		replaceFile(t, accountAgents, canonicalAccountAgents, `reader:
  id: account-reader
  type: generic
  role: reader-v1
  intent: {inline: "Read account registration events."}
  model: regular
  subscriptions:
    - account.registered
retired:
  id: account-retired
  type: generic
  role: retired
  intent: {inline: "Handle account notification requests."}
  model: regular
  subscriptions:
    - account.notify.requested
`)
	case 2:
		replaceFile(t, packageFile, `version: "1.0.0"`, `version: "2.0.0"`)
		replaceFile(t, accountAgents, canonicalAccountAgents, `reader:
  id: account-reader
  type: generic
  role: reader-v2
  intent: {inline: "Read account registration and notification events."}
  model: regular
  subscriptions:
    - account.registered
    - account.notify.requested
writer:
  id: account-writer
  type: generic
  role: writer
  intent: {inline: "Write account notification results."}
  model: regular
  subscriptions:
    - account.notify.requested
`)
	case 3:
		replaceFile(t, packageFile, `version: "1.0.0"`, `version: "3.0.0"`)
		replaceFile(t, accountAgents, canonicalAccountAgents, `reader:
  id: account-reader
  type: generic
  role: reader-v3
  intent: {inline: "Read account registration and notification events."}
  model: regular
  subscriptions:
    - account.registered
    - account.notify.requested
writer:
  id: account-writer
  type: generic
  role: writer
  intent: {inline: "Write account notification results."}
  model: regular
  subscriptions:
    - account.notify.requested
retired:
  id: account-retired
  type: generic
  role: returned
  intent: {inline: "Handle account notification requests."}
  model: regular
  subscriptions:
    - account.notify.requested
`)
	default:
		t.Fatalf("unsupported agent topology revision %d", opts.AgentTopologyRevision)
	}
	if opts.ExplicitAgentName {
		replaceFile(t, accountAgents, "account-worker:\n", "account-worker:\n  id: account-handler\n")
	}
	return root
}

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func copyTree(t testing.TB, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, rel)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, contents, 0o644)
	})
	if err != nil {
		t.Fatalf("copy canonical notify-all-children example: %v", err)
	}
}

func replaceFile(t testing.TB, path, old, replacement string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(contents), old) {
		t.Fatalf("variant mutation target missing in %s", path)
	}
	updated := strings.Replace(string(contents), old, replacement, 1)
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
