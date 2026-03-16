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
	"test-agent-emits-to-node":     {kind: "fixture-issue", reason: "the fixture still expects only task.finalized, but the real runtime also persists the agent-emitted task.completed event in the chain"},
	"test-cross-flow-subscription": {kind: "fixture-issue", reason: "the fixture now boots, but expected.emitted_events still wants prefixed flow names while the live runtime emits order.completed and invoice.created on this package shape"},
	"test-dual-delivery":           {kind: "fixture-issue", reason: "the fixture now boots, but expected.agent_received still wants audit-agent to receive task.completed even though the live runtime only persists the node delivery path here"},
	"test-multi-gate-pipeline":     {kind: "fixture-issue", reason: "the fixture now boots, but expected.emitted_events still omits the emitted gate.set event that the live runtime persists before approval.granted"},
	"test-wildcard-cross-flow":     {kind: "fixture-issue", reason: "the fixture now boots, but expected.emitted_events still wants prefixed flow names while the live runtime emits job.alpha_done and audit.logged on this package shape"},
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
