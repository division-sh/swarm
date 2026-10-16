package contracts

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestS1ContractLoaderExactSourcePath(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "parent with spaces ")
	writeBundleIdentityFile(t, filepath.Join(parent, "shop-a", "schema.yaml"), "{}\n")
	repo := repoRootForContractsTest(t)
	for _, name := range []string{" shop-a", "shop-a ", "shop-a\t", "shop-a\n"} {
		root := filepath.Join(parent, name)
		writeBundleIdentityFile(t, filepath.Join(root, "schema.yaml"), "required_agents: []\n")
		bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
		if err == nil || bundle != nil || !strings.Contains(err.Error(), "rename it") {
			t.Errorf("executable loader selected a different sibling: root=%q bundle=%v err=%v", root, bundle, err)
		}
	}
	if _, err := LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(parent, "shop-a"), DefaultPlatformSpecFile(repo)); err != nil {
		t.Fatalf("valid root with whitespace parent refused: %v", err)
	}
}
