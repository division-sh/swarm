package cataloge2e

import (
	"github.com/division-sh/swarm/internal/testutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var tier7CompositionFixtures = []string{
	"test-agent-emits-to-node",
	"test-cross-flow-subscription",
	"test-dual-delivery",
	"test-full-lifecycle",
	"test-multi-gate-pipeline",
	"test-two-node-chain",
	"test-wildcard-cross-flow",
}

var tier7ExcludedFixtures = map[string]catalogExcludedFixture{}

var tier7StartedRuntimeFixtures = map[string]struct{}{
	"test-agent-emits-to-node": {},
}

var tier7StaticMultiEntityRetiredFixtures = map[string]struct{}{
	"test-cross-flow-subscription": {},
	"test-wildcard-cross-flow":     {},
}

func TestTier7CompositionCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier7CompositionFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier7-composition", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			if _, retired := tier7StaticMultiEntityRetiredFixtures[fixtureName]; retired {
				assertCatalogStaticMultiEntityRetirement(t, fixtureRoot)
				return
			}

			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			_, startRuntime := tier7StartedRuntimeFixtures[fixtureName]
			h := newRuntimeHarness(t, fixtureRoot, startRuntime, testutil.PostgresRowState())
			h.seedEntityFields(expected)
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}

func TestTier7CompositionCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier7-composition"))
	if err != nil {
		t.Fatalf("read tier7 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier7CompositionFixtures))
	for _, name := range tier7CompositionFixtures {
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
		if _, ok := tier7ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier7 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier7CompositionFixtures) + len(tier7ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier7 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier7CompositionFixtures), len(tier7ExcludedFixtures))
	}
}
