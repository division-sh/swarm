package contracts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBootBundleIdentityStableAcrossRootsAndFileOrder(t *testing.T) {
	repo := repoRootForContractsTest(t)
	platformSpec := DefaultPlatformSpecFile(repo)
	rootA := t.TempDir()
	rootB := t.TempDir()

	writeBundleIdentityFile(t, filepath.Join(rootA, "manifest.yaml"), "name: identity-test\nversion: 1.0.0\n")
	writeBundleIdentityFile(t, filepath.Join(rootA, "agents.yaml"), "guide:\n  id: guide\n  role: guide\n  intent: prompts/a.md\n  model: regular\n  subscriptions: [guide.requested]\n")
	writeBundleIdentityFile(t, filepath.Join(rootA, "prompts", "a.md"), "alpha\n")
	writeBundleIdentityFile(t, filepath.Join(rootA, "prompts", "b.md"), "beta\n")

	writeBundleIdentityFile(t, filepath.Join(rootB, "prompts", "b.md"), "beta\r\n")
	writeBundleIdentityFile(t, filepath.Join(rootB, "prompts", "a.md"), "alpha\r\n")
	writeBundleIdentityFile(t, filepath.Join(rootB, "manifest.yaml"), "version: 1.0.0\r\nname: identity-test\r\n")
	writeBundleIdentityFile(t, filepath.Join(rootB, "agents.yaml"), "guide:\r\n  id: guide\r\n  role: guide\r\n  intent: prompts/a.md\r\n  model: regular\r\n  subscriptions: [guide.requested]\r\n")

	bundleA, err := LoadWorkflowContractBundleWithOverrides(repo, rootA, platformSpec)
	if err != nil {
		t.Fatalf("load bundle A: %v", err)
	}
	bundleB, err := LoadWorkflowContractBundleWithOverrides(repo, rootB, platformSpec)
	if err != nil {
		t.Fatalf("load bundle B: %v", err)
	}
	identityA, err := BootBundleIdentity(bundleA)
	if err != nil {
		t.Fatalf("BootBundleIdentity A: %v", err)
	}
	identityB, err := BootBundleIdentity(bundleB)
	if err != nil {
		t.Fatalf("BootBundleIdentity B: %v", err)
	}
	if identityA.WorkflowName != "." || identityA.WorkflowVersion != identityA.BundleHash {
		t.Fatalf("runtime identity is not path/hash-derived: %#v", identityA)
	}
	if identityA.BundleHash == identityB.BundleHash {
		t.Fatalf("exact resolved intent bytes did not distinguish roots:\nA=%s\nB=%s", identityA.BundleHash, identityB.BundleHash)
	}
	if err := ValidateBundleHash(identityA.BundleHash); err != nil {
		t.Fatalf("bundle_hash = %q: %v", identityA.BundleHash, err)
	}
}

func TestBootBundleIdentityChangesWithLoadedContent(t *testing.T) {
	repo := repoRootForContractsTest(t)
	platformSpec := DefaultPlatformSpecFile(repo)
	rootA := t.TempDir()
	rootB := t.TempDir()

	writeBundleIdentityFile(t, filepath.Join(rootA, "manifest.yaml"), "name: identity-test\nversion: 1.0.0\n")
	writeBundleIdentityFile(t, filepath.Join(rootA, "agents.yaml"), "guide:\n  id: guide\n  role: guide\n  intent: prompts/a.md\n  model: regular\n  subscriptions: [guide.requested]\n")
	writeBundleIdentityFile(t, filepath.Join(rootA, "prompts", "a.md"), "alpha\n")
	writeBundleIdentityFile(t, filepath.Join(rootB, "manifest.yaml"), "name: identity-test\nversion: 1.0.0\n")
	writeBundleIdentityFile(t, filepath.Join(rootB, "agents.yaml"), "guide:\n  id: guide\n  role: guide\n  intent: prompts/a.md\n  model: regular\n  subscriptions: [guide.requested]\n")
	writeBundleIdentityFile(t, filepath.Join(rootB, "prompts", "a.md"), "changed\n")

	bundleA, err := LoadWorkflowContractBundleWithOverrides(repo, rootA, platformSpec)
	if err != nil {
		t.Fatalf("load bundle A: %v", err)
	}
	bundleB, err := LoadWorkflowContractBundleWithOverrides(repo, rootB, platformSpec)
	if err != nil {
		t.Fatalf("load bundle B: %v", err)
	}
	identityA, err := BootBundleIdentity(bundleA)
	if err != nil {
		t.Fatalf("BootBundleIdentity A: %v", err)
	}
	identityB, err := BootBundleIdentity(bundleB)
	if err != nil {
		t.Fatalf("BootBundleIdentity B: %v", err)
	}
	if identityA.BundleHash == identityB.BundleHash {
		t.Fatalf("bundle_hash did not change after loaded content changed: %s", identityA.BundleHash)
	}
}

func writeBundleIdentityFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
