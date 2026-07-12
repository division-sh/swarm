package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
)

func testProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	dirs := testProviderTriggerPackDirs(t)
	registry, _, err := providertriggers.NewCatalogSnapshotFromPackDirs("0.7.0", dirs, nil)
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

func emptyProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	registry, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatalf("create empty provider trigger registry: %v", err)
	}
	return registry
}

func withTestProviderTriggerPlatformInventory(t *testing.T, configText string) string {
	t.Helper()
	if strings.Contains(configText, "\nprovider_triggers:") || strings.HasPrefix(configText, "provider_triggers:") {
		t.Fatalf("test runtime config already declares provider_triggers; compose the intended inventory explicitly")
	}
	lines := []string{"provider_triggers:", "  packs:", "    platform_dirs:"}
	for _, dir := range testProviderTriggerPackDirs(t) {
		lines = append(lines, fmt.Sprintf("      - %q", dir))
	}
	return strings.TrimRight(configText, "\n") + "\n" + strings.Join(lines, "\n") + "\n"
}
