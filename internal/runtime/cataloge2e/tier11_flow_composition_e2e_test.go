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
	"test-child-flow-local-events",
	"test-nested-three-levels",
	"test-child-flow-pin-wiring",
	"test-child-flow-policy-inherit",
	"test-required-agents-child",
	"test-child-flow-sibling-isolation",
	"test-multi-level-policy-inherit",
}

var tier11ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-dynamic-flow-instance":        {kind: "fixture-issue", reason: "the fixture still uses legacy action keys type/flow_template/instance_id, so the real loader never executes create_flow_instance and no worker instance is created"},
	"test-child-flow-loads":             {kind: "fixture-issue", reason: "fixture now boots, but it still expects a clean success while the real runtime emits producer/consumer warnings for the unwired parent and child events"},
	"test-child-flow-tool-inherit":      {kind: "fixture-issue", reason: "fixture now boots, but it still expects clean success while the real runtime emits producer/consumer and prompt warnings"},
	"test-data-pin-wiring":              {kind: "fixture-issue", reason: "prefixed child output event processor/process.done is still not declared in the parent-visible event catalog"},
	"test-data-pin-write-conflict":      {kind: "fixture-issue", reason: "fixture still fails earlier because shared_field is written via data_accumulation without being declared in entity_schema, so it never reaches the intended DATA-PIN-CONFLICT validation"},
	"test-gates-in-child-flow":          {kind: "fixture-issue", reason: "prefixed child event child/validation.done is still not declared in the parent-visible event catalog"},
	"test-tool-override":                {kind: "fixture-issue", reason: "fixture still references missing tool lookup_data and does not declare child/child.done in the parent-visible event catalog"},
	"test-wildcard-deep-subscription":   {kind: "fixture-issue", reason: "deep wildcard and prefixed grandchild events are still not declared in the real event catalog"},
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
				h.publishAndWait(step, 2*time.Second)
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
