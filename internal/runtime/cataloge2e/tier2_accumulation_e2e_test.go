package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier2AccumulationFixtures = []string{
	"test-accumulate-idempotent",
	"test-accumulate-partial",
}

var tier2ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-accumulate-all":                  {kind: "validation-gap", reason: "real runtime currently leaves the instance in collecting for this all-complete fixture shape"},
	"test-accumulate-crash-recovery":       {kind: "validation-gap", reason: "real runtime leaves the instance in collecting instead of advancing to complete for this crash-recovery accumulation fixture"},
	"test-accumulate-expected-from-entity": {kind: "validation-gap", reason: "real runtime leaves the instance in collecting instead of advancing to complete for this expected-from-entity accumulation fixture"},
	"test-accumulate-from-filter":          {kind: "validation-gap", reason: "real runtime leaves the instance in collecting instead of advancing to complete for this accumulate-from-filter fixture"},
	"test-accumulate-on-timeout":           {kind: "validation-gap", reason: "real runtime leaves the instance in collecting instead of advancing to partial for this accumulate-on-timeout fixture"},
	"test-accumulate-threshold":            {kind: "validation-gap", reason: "real runtime leaves the instance in collecting instead of advancing to complete for this threshold accumulation fixture"},
	"test-accumulate-timeout":              {kind: "validation-gap", reason: "real runtime leaves the instance in collecting instead of advancing to complete for this timeout accumulation fixture"},
	"test-accumulate-with-compute":         {kind: "validation-gap", reason: "real runtime currently leaves the instance in collecting for this accumulate-with-compute fixture shape"},
}

func TestTier2AccumulationCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier2AccumulationFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", fixtureName)
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

func TestTier2AccumulationCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier2-accumulation"))
	if err != nil {
		t.Fatalf("read tier2 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier2AccumulationFixtures))
	for _, name := range tier2AccumulationFixtures {
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
		if _, ok := tier2ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier2 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier2AccumulationFixtures) + len(tier2ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier2 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier2AccumulationFixtures), len(tier2ExcludedFixtures))
	}
}
