package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier7CompositionFixtures = []string{
	"test-full-lifecycle",
	"test-two-node-chain",
}

var tier7ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-agent-emits-to-node":     {kind: "harness-gap", reason: "fixture now boots, but cataloge2e still needs scripted agent responses for the agent emit path to drive the node transition"},
	"test-cross-flow-subscription": {kind: "fixture-issue", reason: "prefixed cross-flow events like flow-b/order.completed are still not declared in the real event catalog"},
	"test-dual-delivery":           {kind: "fixture-issue", reason: "real boot now reaches emit-schema enforcement, and the fixture is still missing an explicit schema entry for the agent-emitted audit event"},
	"test-multi-gate-pipeline":     {kind: "fixture-issue", reason: "gate-setter nodes still omit required produces entries, so real boot validation rejects the package"},
	"test-wildcard-cross-flow":     {kind: "fixture-issue", reason: "prefixed wildcard triggers like */job.* are still not declared in the real event catalog"},
}

func TestTier7CompositionCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier7CompositionFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier7-composition", fixtureName)
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
