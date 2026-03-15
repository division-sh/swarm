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
	"test-two-node-chain",
}

var tier7ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-agent-emits-to-node":     {kind: "fixture-issue", reason: "fixture agents.yaml omits required model_tier, conversation_mode, subscriptions, and emit_events fields for the real loader"},
	"test-cross-flow-subscription": {kind: "fixture-issue", reason: "fixture uses a flat multi-flow package shape without flow-level workflow semantics, so real module construction fails with workflow.name missing"},
	"test-dual-delivery":           {kind: "fixture-issue", reason: "fixture agents.yaml omits required model_tier, conversation_mode, subscriptions, and emit_events fields for the real loader"},
	"test-full-lifecycle":          {kind: "fixture-issue", reason: "fixture still uses sets_gates, which the real loader rejects; it must use the live sets_gate dialect"},
	"test-multi-gate-pipeline":     {kind: "fixture-issue", reason: "fixture still uses sets_gates, which the real loader rejects; it must use the live sets_gate dialect"},
	"test-wildcard-cross-flow":     {kind: "fixture-issue", reason: "fixture uses a flat multi-flow package shape without flow-level workflow semantics, so real module construction fails with workflow.name missing"},
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
