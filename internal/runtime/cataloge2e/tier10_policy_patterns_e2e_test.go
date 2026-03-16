package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier10PolicyPatternFixtures = []string{
	"test-policy-capacity-query",
	"test-policy-counter-escalate",
	"test-policy-hard-gate-override",
	"test-policy-multi-guard-partial",
	"test-policy-threshold-three-way",
	"test-policy-timeout-elapsed",
}
var tier10ExcludedFixtures = map[string]catalogExcludedFixture{}

func TestTier10PolicyPatternCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier10PolicyPatternFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier10-policy-patterns", fixtureName)
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

func TestTier10PolicyPatternCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier10-policy-patterns"))
	if err != nil {
		t.Fatalf("read tier10 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier10PolicyPatternFixtures))
	for _, name := range tier10PolicyPatternFixtures {
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
		if _, ok := tier10ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier10 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier10PolicyPatternFixtures) + len(tier10ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier10 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier10PolicyPatternFixtures), len(tier10ExcludedFixtures))
	}
}
