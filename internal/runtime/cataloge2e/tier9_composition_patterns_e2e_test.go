package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
	"test-compose-multi-emit-cross-flow": {reason: "legacy multi-emit handler shape is retired by #457; fixture migration belongs to a later explicit child if multi-emit semantics return"},
}

func TestTier9CompositionPatternCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-accumulate-compute-branch"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-clear-gates-reenter"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-create-instance-config"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-gate-chain-three"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-gate-data-advance-emit"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-guard-counter-escalate"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-guard-multi-source"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-guard-query-capacity"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-lifecycle-seven-states"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-multi-emit-cross-flow"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-rules-fanout-data"),
		canonicalrouting.ArtifactID("tests/tier9-composition-patterns/test-compose-rules-per-rule-data"),
	)
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier9CompositionPatternFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier9-composition-patterns", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			h := newRuntimeHarness(t, fixtureRoot, false)
			h.seedEntityFields(expected)
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, catalogRuntimePublishTimeout)
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
