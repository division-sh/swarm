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
	"test-compose-guard-counter-escalate",
	"test-compose-lifecycle-seven-states",
	"test-compose-multi-emit-cross-flow",
}

var tier9ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-compose-gate-chain-three":       {kind: "runtime-gap", reason: "real runtime still only persists the final gate in the sequence; g_a and g_b are missing when the fixture expects all three gates"},
	"test-compose-gate-data-advance-emit": {kind: "runtime-gap", reason: "real runtime applies the state transition and emits, but does not persist the shorthand data_accumulation fields expected by the fixture"},
	"test-compose-guard-multi-source":     {kind: "validation-gap", reason: "real boot path still does not expose query_entities to CEL guard parsing for this multi-source guard fixture"},
	"test-compose-guard-query-capacity":   {kind: "validation-gap", reason: "real boot path does not expose query_entities to CEL guard parsing for this fixture shape"},
	"test-compose-rules-fanout-data":      {kind: "runtime-gap", reason: "real runtime selects the rule and transitions, but does not persist the expected per-rule dispatch_count field"},
	"test-compose-rules-per-rule-data":    {kind: "runtime-gap", reason: "real runtime selects the winning rule and transitions, but does not persist the expected per-rule data_accumulation fields"},
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
