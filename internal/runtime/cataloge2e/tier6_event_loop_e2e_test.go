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
	"test-event-persisted-before-delivery",
}

var tier6ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-atomicity-commit":         {kind: "validation-gap", reason: "initial E2E harness does not yet assert atomic commit semantics against the real runtime receipts/state tables"},
	"test-atomicity-guard-rollback": {kind: "validation-gap", reason: "initial E2E harness does not yet assert guard rollback semantics against the real runtime receipts/state tables"},
	"test-atomicity-rollback":       {kind: "validation-gap", reason: "initial E2E harness does not yet assert rollback semantics against the real runtime receipts/state tables"},
	"test-chain-depth-limit":        {kind: "validation-gap", reason: "real runtime does not currently surface the expected chain-depth dead-letter outcome for this fixture"},
	"test-cross-entity-concurrent":  {kind: "validation-gap", reason: "initial E2E harness does not yet assert cross-entity concurrency behavior"},
	"test-dead-letter":              {kind: "validation-gap", reason: "real runtime emits contradiction diagnostics instead of the catalog dead_letter outcome for this fixture shape"},
	"test-entity-serialization":     {kind: "validation-gap", reason: "initial E2E harness does not yet assert entity serialization guarantees"},
	"test-event-validation":         {kind: "validation-gap", reason: "real runtime does not currently produce the catalog reject-plus-dead-letter outcome for this invalid event fixture"},
	"test-guards-pre-handler-state": {kind: "validation-gap", reason: "initial E2E harness does not yet assert pre-handler guard state ordering"},
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
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, 2*time.Second)
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
