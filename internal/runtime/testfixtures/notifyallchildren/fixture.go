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
	OwnerFlowID       = "portfolio"
	OwnerOutputPin    = "account_notify_requested"
	OwnerTriggerEvent = "portfolio.notify.requested"
	NotifyEvent       = "account.notify.requested"
	ChildFlowID       = "account"
	ChildInputPin     = "account_notify_requested"
)

// Options produces verifier-only corruptions of the checked-in canonical example.
type Options struct {
	OmitOutputPin     bool
	OmitConnect       bool
	BadConnectMapping bool
	MissingEmitCarry  bool
	ProducerTarget    bool
	ProducerBroadcast bool
	ObjectMembership  bool
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
	if opts.OmitConnect {
		replaceFile(t, packageFile, `  - from: portfolio.account_notify_requested
    to: account.account_notify_requested
`, "")
	}
	if opts.BadConnectMapping {
		replaceFile(t, packageFile, `  - from: portfolio.account_notify_requested
    to: account.account_notify_requested
`, `  - from: portfolio.account_notify_requested
    to: account.account_notify_requested
    using:
      instance:
        source: missing_account_id
        target: account_id
`)
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
