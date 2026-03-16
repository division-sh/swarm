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
	"test-compose-gate-chain-three",
	"test-compose-guard-multi-source",
	"test-compose-guard-query-capacity",
	"test-compose-rules-fanout-data",
	"test-compose-rules-per-rule-data",
	"test-compose-guard-counter-escalate",
	"test-compose-lifecycle-seven-states",
}

var tier9ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-compose-accumulate-compute-branch": {kind: "fixture-issue", reason: "the fixture still uses unsupported accumulate keys completion_mode and expected_count, so the real loader falls back to default completion and completes after the first score with composite=80"},
	"test-compose-clear-gates-reenter":    {kind: "fixture-issue", reason: "the fixture re-enters from terminal state approved without declaring an explicit terminal exit, so the hardened runtime now correctly keeps the entity in approved"},
	"test-compose-create-instance-config": {kind: "fixture-issue", reason: "the fixture still uses legacy action keys type/flow_template/instance_id, so the real loader never executes create_flow_instance and no instance is created"},
	"test-compose-gate-data-advance-emit": {kind: "fixture-issue", reason: "the fixture now fails real validation because stage_one_result and stage_two_result are written via data_accumulation but still missing from the declared entity_schema"},
	"test-compose-multi-emit-cross-flow":  {kind: "fixture-issue", reason: "the fixture expects tracker/task.record but still does not declare that prefixed cross-flow event in events.yaml, so only task.logged is emitted on the real runtime path"},
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
