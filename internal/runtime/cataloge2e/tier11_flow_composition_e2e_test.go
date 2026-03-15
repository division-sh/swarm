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
}

var tier11ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-child-flow-absolute-path":     {kind: "fixture-issue", reason: "child flow schema is missing required field name, and the parent fixture also references prefixed child events that are not declared in the real event catalog"},
	"test-child-flow-loads":             {kind: "fixture-issue", reason: "child flow schema is missing required field name, so boot fails before child-flow loading is exercised"},
	"test-child-flow-local-events":      {kind: "harness-gap", reason: "fixture expects parent_state assertion, which cataloge2e does not support yet"},
	"test-child-flow-pin-wiring":        {kind: "fixture-issue", reason: "child flow schema is missing required field name, and the prefixed child output event is not declared in the parent-visible event catalog"},
	"test-child-flow-policy-inherit":    {kind: "fixture-issue", reason: "child flow schema is missing required field name, so boot fails before policy inheritance behavior is exercised"},
	"test-child-flow-sibling-isolation": {kind: "harness-gap", reason: "fixture expects flow_b_state assertion, which cataloge2e does not support yet"},
	"test-child-flow-tool-inherit":      {kind: "fixture-issue", reason: "child flow schema is missing required field name, so boot fails before tool inheritance is exercised"},
	"test-data-pin-wiring":              {kind: "fixture-issue", reason: "child flow schema is missing required field name, and the prefixed child output event is not declared in the parent-visible event catalog"},
	"test-data-pin-write-conflict":      {kind: "fixture-issue", reason: "both child flow schemas are missing required field name, so boot fails before the intended DATA-PIN-CONFLICT validation"},
	"test-dynamic-flow-instance":        {kind: "fixture-issue", reason: "template child flow schema is missing required field name, so boot fails before dynamic instance creation is exercised"},
	"test-gates-in-child-flow":          {kind: "fixture-issue", reason: "child flow fixture still uses sets_gates, which the real loader rejects; it must use the live sets_gate dialect"},
	"test-multi-level-policy-inherit":   {kind: "fixture-issue", reason: "child and grandchild flow schemas are missing required field name, so boot fails before multi-level policy inheritance is exercised"},
	"test-nested-three-levels":          {kind: "fixture-issue", reason: "child and grandchild flow schemas are missing required field name, and prefixed nested events are not declared in the parent-visible event catalog"},
	"test-required-agents-child":        {kind: "harness-gap", reason: "fixture requires scripted child-agent behavior, but cataloge2e does not yet provide agent fixtures for this case"},
	"test-tool-override":                {kind: "harness-gap", reason: "fixture expects tool_resolution assertions, which cataloge2e does not support yet"},
	"test-wildcard-deep-subscription":   {kind: "fixture-issue", reason: "child and grandchild flow schemas are missing required field name, and the deep prefixed wildcard events are not declared in the real event catalog"},
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
