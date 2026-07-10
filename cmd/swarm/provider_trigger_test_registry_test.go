package main

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
)

func testProviderTriggerRegistry(t *testing.T) *providertriggers.Registry {
	t.Helper()
	dirs := testProviderTriggerPackDirs(t)
	registry, _, err := providertriggers.NewRegistryFromPackDirs("0.7.0", dirs, nil)
	if err != nil {
		t.Fatalf("load provider trigger registry: %v", err)
	}
	return registry
}

func testProviderTriggerPackDirs(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..", "packs", "provider-triggers")
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("resolve provider trigger pack root: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read provider trigger pack root: %v", err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(root, entry.Name()))
		}
	}
	sort.Strings(dirs)
	return dirs
}

func emptyProviderTriggerRegistry(t *testing.T) *providertriggers.Registry {
	t.Helper()
	registry, err := providertriggers.NewRegistry()
	if err != nil {
		t.Fatalf("create empty provider trigger registry: %v", err)
	}
	return registry
}
