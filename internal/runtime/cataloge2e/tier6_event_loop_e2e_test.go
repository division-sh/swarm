package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier6EventLoopFixtures = []string{
	"test-atomicity-commit",
	"test-atomicity-rollback",
	"test-cross-entity-concurrent",
	"test-entity-serialization",
	"test-event-persisted-before-delivery",
	"test-event-validation",
}

var tier6ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-atomicity-guard-rollback": {kind: "fixture-issue", reason: "fixture writes counter via data_accumulation but still does not declare counter in workflow entity_schema, so real validation now rejects it"},
	"test-chain-depth-limit":        {kind: "fixture-issue", reason: "fixture still boot-fails with EVENT-CYCLE before any chain-depth runtime behavior is exercised"},
	"test-dead-letter":              {kind: "fixture-issue", reason: "fixture now expects pipeline.dead_letter, but the live runtime still reports this unroutable event as a discard path instead"},
	"test-guards-pre-handler-state": {kind: "fixture-issue", reason: "fixture writes counter via data_accumulation but still does not declare counter in workflow entity_schema, so real validation now rejects it"},
}

func TestTier6EventLoopCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier6EventLoopFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier6-event-loop", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			h := newRuntimeHarness(t, fixtureRoot, false)
			h.seedEntityFields(expected)
			if len(expected.Trigger.Concurrent) > 0 {
				h.publishConcurrentAndWait(expected.Trigger.Concurrent, 2*time.Second)
			} else {
				for _, step := range expected.triggerSequence() {
					h.publishAndWait(step, 2*time.Second)
				}
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}

func TestTier6EventLoopCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier6-event-loop"))
	if err != nil {
		t.Fatalf("read tier6 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier6EventLoopFixtures))
	for _, name := range tier6EventLoopFixtures {
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
		if _, ok := tier6ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier6 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier6EventLoopFixtures) + len(tier6ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier6 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier6EventLoopFixtures), len(tier6ExcludedFixtures))
	}
}
