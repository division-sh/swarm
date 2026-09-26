package testcatalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestCatalogFullLifecycleFixtureIsFiniteFilesystemFlowTree(t *testing.T) {
	repo := catalogRepoRoot(t)
	root := filepath.Join(repo, "internal", "releasee2e", "testdata", "full_lifecycle", "standing_telegram")
	for _, label := range []string{
		"telegram-chat/schema.yaml",
		"telegram-ingress/schema.yaml",
	} {
		if info, err := os.Stat(filepath.Join(root, filepath.FromSlash(label))); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("full lifecycle source is missing %s", label)
		}
	}
	retired, err := retiredFullLifecycleTopology(repo, root)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range retired {
		t.Errorf("full lifecycle source retains retired topology at %s", path)
	}
}

func TestCatalogFullLifecycleFixtureExcludesNestedCheckout(t *testing.T) {
	repo := t.TempDir()
	root := filepath.Join(repo, "full_lifecycle")
	foreign := filepath.Join(root, "foreign")
	if err := os.MkdirAll(foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	localFile := filepath.Join(root, "package.yaml")
	for _, path := range []string{localFile, filepath.Join(foreign, "package.yaml")} {
		if err := os.WriteFile(path, []byte("retired\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(foreign, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	retired, err := retiredFullLifecycleTopology(repo, root)
	if err != nil || len(retired) != 1 || retired[0] != localFile {
		t.Fatalf("retired topology = %v, %v; want only current-local file", retired, err)
	}
	if err := os.Remove(localFile); err != nil {
		t.Fatal(err)
	}
	retired, err = retiredFullLifecycleTopology(repo, root)
	if err != nil || len(retired) != 0 {
		t.Fatalf("foreign-only retired topology was rejected: %v, %v", retired, err)
	}
}

func retiredFullLifecycleTopology(repo, root string) ([]string, error) {
	var retired []string
	err := checkoutsource.WalkDir(repo, root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Name() == "package.yaml" || entry.IsDir() && entry.Name() == "flows" {
			retired = append(retired, path)
		}
		return nil
	})
	return retired, err
}
