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
	"test-dynamic-flow-instance",
}

var tier11ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-child-flow-absolute-path":     {kind: "fixture-issue", reason: "prefixed child event child/task.done is still not declared in the parent-visible event catalog"},
	"test-child-flow-loads":             {kind: "fixture-issue", reason: "fixture now boots, but it still expects a clean success while the real runtime emits producer/consumer warnings for the unwired parent and child events"},
	"test-child-flow-local-events":      {kind: "harness-gap", reason: "fixture expects parent_state assertion, which cataloge2e does not support yet"},
	"test-child-flow-pin-wiring":        {kind: "fixture-issue", reason: "prefixed child output event child/work.completed is still not declared in the parent-visible event catalog"},
	"test-child-flow-policy-inherit":    {kind: "runtime-gap", reason: "fixture now boots, but the real runtime still leaves the child entity in pending instead of reaching approved"},
	"test-child-flow-sibling-isolation": {kind: "harness-gap", reason: "fixture expects flow_b_state assertion, which cataloge2e does not support yet"},
	"test-child-flow-tool-inherit":      {kind: "fixture-issue", reason: "fixture now boots, but it still expects clean success while the real runtime emits producer/consumer and prompt warnings"},
	"test-data-pin-wiring":              {kind: "fixture-issue", reason: "prefixed child output event processor/process.done is still not declared in the parent-visible event catalog"},
	"test-data-pin-write-conflict":      {kind: "validation-gap", reason: "fixture now boots cleanly enough that the real validator should catch DATA-PIN-CONFLICT, but it currently does not"},
	"test-gates-in-child-flow":          {kind: "fixture-issue", reason: "prefixed child event child/validation.done is still not declared in the parent-visible event catalog"},
	"test-multi-level-policy-inherit":   {kind: "runtime-gap", reason: "fixture now boots, but the real runtime still leaves the nested child flow in pending instead of reaching approved"},
	"test-nested-three-levels":          {kind: "fixture-issue", reason: "prefixed nested events grandchild/micro.done and child/step.result are still not declared in the parent-visible event catalog"},
	"test-required-agents-child":        {kind: "fixture-issue", reason: "prefixed child event analyzer-flow/analysis.done is still not declared in the parent-visible event catalog"},
	"test-tool-override":                {kind: "fixture-issue", reason: "fixture still references missing tool lookup_data and does not declare child/child.done in the parent-visible event catalog"},
	"test-wildcard-deep-subscription":   {kind: "fixture-issue", reason: "deep wildcard and prefixed grandchild events are still not declared in the real event catalog"},
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

			h := newRuntimeHarness(t, fixtureRoot, false)
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
