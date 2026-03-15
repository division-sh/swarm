package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier2AccumulationFixtures = []string{
	"test-accumulate-all",
	"test-accumulate-crash-recovery",
	"test-accumulate-expected-from-entity",
	"test-accumulate-from-filter",
	"test-accumulate-idempotent",
	"test-accumulate-on-timeout",
	"test-accumulate-partial",
	"test-accumulate-threshold",
	"test-accumulate-timeout",
	"test-accumulate-with-compute",
}

var tier2ExcludedFixtures = map[string]catalogExcludedFixture{}

func TestTier2AccumulationCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier2AccumulationFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", fixtureName)
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

func TestTier2AccumulationCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier2-accumulation"))
	if err != nil {
		t.Fatalf("read tier2 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier2AccumulationFixtures))
	for _, name := range tier2AccumulationFixtures {
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
		if _, ok := tier2ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier2 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier2AccumulationFixtures) + len(tier2ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier2 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier2AccumulationFixtures), len(tier2ExcludedFixtures))
	}
}
