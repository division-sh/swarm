package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier3ListProcessingFixtures = []string{
	"test-filter-basic",
	"test-filter-empty",
	"test-reduce-count",
	"test-reduce-max",
	"test-reduce-min",
	"test-reduce-operation-count",
}

var tier3ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-fan-out-basic":           {kind: "validation-gap", reason: "real runtime currently keeps the instance in pending for this fan-out fixture shape"},
	"test-fan-out-count":           {kind: "validation-gap", reason: "real runtime currently keeps the instance in pending instead of advancing to processing for this fan-out fixture"},
	"test-fan-out-emit-mapping":    {kind: "fixture-issue", reason: "nodes.yaml still uses an emit_mapping shape the real loader cannot unmarshal"},
	"test-fan-out-empty":           {kind: "validation-gap", reason: "real runtime does not persist fan_out_count for this empty fan-out fixture"},
	"test-group-by-standalone":     {kind: "validation-gap", reason: "runtime handler field allowlist does not include group_by"},
	"test-reduce-pick-or-average":  {kind: "validation-gap", reason: "real runtime leaves entity field result at 0 instead of computing the expected value for this reduce fixture"},
	"test-reduce-sum":              {kind: "validation-gap", reason: "real runtime currently leaves the reduced entity field at its zero value for this fixture shape"},
	"test-reduce-weighted-average": {kind: "validation-gap", reason: "real runtime leaves entity field composite at 0 instead of computing the expected weighted average"},
}

func TestTier3ListProcessingCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier3ListProcessingFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier3-list-processing", fixtureName)
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

func TestTier3ListProcessingCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier3-list-processing"))
	if err != nil {
		t.Fatalf("read tier3 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier3ListProcessingFixtures))
	for _, name := range tier3ListProcessingFixtures {
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
		if _, ok := tier3ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier3 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier3ListProcessingFixtures) + len(tier3ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier3 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier3ListProcessingFixtures), len(tier3ExcludedFixtures))
	}
}
