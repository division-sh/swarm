package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier11FlowCompositionFixtures = []string{
	"test-child-flow-absolute-path",
	"test-child-flow-loads",
	"test-child-flow-local-events",
	"test-nested-three-levels",
	"test-child-flow-pin-wiring",
	"test-child-flow-policy-inherit",
	"test-child-flow-tool-inherit",
	"test-data-pin-wiring",
	"test-data-pin-write-conflict",
	"test-gates-in-child-flow",
	"test-required-agents-child",
	"test-child-flow-sibling-isolation",
	"test-multi-level-policy-inherit",
	"test-sibling-both-instantiated-isolated",
	"test-subject-id-cross-flow-inherit",
	"test-tool-override",
	"test-wildcard-deep-subscription",
}

var tier11ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-dynamic-flow-instance":       {reason: "create_flow_instance fixture now fails closed without required config_from; fixture migration belongs to #416"},
	"test-subject-id-first-flow-seeds": {reason: "new conformance fixture not yet wired into runtime catalog execution set"},
}

var tier11StartedRuntimeFixtures = map[string]struct{}{
	"test-required-agents-child": {},
}

func TestTier11FlowCompositionCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier11FlowCompositionFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			if expected.Trigger.Boot || strings.TrimSpace(expected.Expected.BootResult) != "" {
				runBootCatalogFixture(t, fixtureRoot)
				return
			}

			_, startRuntime := tier11StartedRuntimeFixtures[fixtureName]
			h := newRuntimeHarness(t, fixtureRoot, startRuntime)
			h.seedEntityFields(expected)
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, 5*time.Second)
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}

func TestTier11FlowCompositionCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier11-flow-composition"))
	if err != nil {
		t.Fatalf("read tier11 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier11FlowCompositionFixtures))
	for _, name := range tier11FlowCompositionFixtures {
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
		if _, ok := tier11ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier11 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier11FlowCompositionFixtures) + len(tier11ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier11 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier11FlowCompositionFixtures), len(tier11ExcludedFixtures))
	}
}
