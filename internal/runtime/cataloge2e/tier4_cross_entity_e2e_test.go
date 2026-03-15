package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier4CrossEntityFixtures = []string{
	"test-clear-multiple-targets",
	"test-clear-state",
	"test-query-filter",
	"test-query-group-by",
}

var tier4ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-create-entity": {kind: "validation-gap", reason: "real validator rejects action create_flow_instance because the runtime has no executable implementation for it in this path"},
}

func TestTier4CrossEntityCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier4CrossEntityFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier4-cross-entity", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			h := newRuntimeHarness(t, fixtureRoot, false)
			h.seedEntityFields(expected)
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, 2*time.Second)
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}

func TestTier4CrossEntityCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier4-cross-entity"))
	if err != nil {
		t.Fatalf("read tier4 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier4CrossEntityFixtures))
	for _, name := range tier4CrossEntityFixtures {
		supported[name] = struct{}{}
	}
	found := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		found = append(found, name)
		if _, ok := supported[name]; ok {
			continue
		}
		if _, ok := tier4ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier4 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier4CrossEntityFixtures) + len(tier4ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier4 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier4CrossEntityFixtures), len(tier4ExcludedFixtures))
	}
}
