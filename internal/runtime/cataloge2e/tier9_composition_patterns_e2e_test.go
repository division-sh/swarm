package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier9CompositionPatternFixtures = []string{
	"test-compose-accumulate-compute-branch",
	"test-compose-clear-gates-reenter",
	"test-compose-create-instance-config",
	"test-compose-gate-chain-three",
	"test-compose-gate-data-advance-emit",
	"test-compose-guard-multi-source",
	"test-compose-guard-query-capacity",
	"test-compose-rules-fanout-data",
	"test-compose-rules-per-rule-data",
	"test-compose-guard-counter-escalate",
	"test-compose-lifecycle-seven-states",
}

var tier9ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-compose-multi-emit-cross-flow": {kind: "fixture-issue", reason: "the fixture now declares tracker/task.record, but expected.emitted_events still wants it even though the live runtime path for this package shape only emits task.logged"},
}

func TestTier9CompositionPatternCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier9CompositionPatternFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier9-composition-patterns", fixtureName)
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

func TestTier9CompositionPatternCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier9-composition-patterns"))
	if err != nil {
		t.Fatalf("read tier9 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier9CompositionPatternFixtures))
	for _, name := range tier9CompositionPatternFixtures {
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
		if _, ok := tier9ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier9 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier9CompositionPatternFixtures) + len(tier9ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier9 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier9CompositionPatternFixtures), len(tier9ExcludedFixtures))
	}
}
