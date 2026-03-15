package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var tier1PrimitiveFixtures = []string{
	"test-advances-to",
	"test-advances-to-terminal",
	"test-clear-gates",
	"test-compute-standalone",
	"test-data-accumulation-literal",
	"test-data-accumulation-direct",
	"test-data-accumulation-mapped",
	"test-emits-multiple",
	"test-emits-payload-transform",
	"test-emits-single",
	"test-from-filter",
	"test-guard-compound-condition",
	"test-guard-discard",
	"test-guard-escalate",
	"test-guard-entity-ref",
	"test-guard-kill",
	"test-guard-multi-fail",
	"test-guard-multi",
	"test-guard-pass",
	"test-guard-policy-ref",
	"test-guard-reject",
	"test-on-complete-first-match",
	"test-on-complete-second-match",
	"test-on-complete-with-state",
	"test-payload-transform-multi-source",
	"test-record-evidence",
	"test-rules-advances-to",
	"test-rules-data-accumulation",
	"test-rules-else",
	"test-rules-match",
	"test-rules-no-match",
	"test-sets-gate",
}

var tier1ExcludedFixtures = map[string]catalogExcludedFixture{}

func TestTier1PrimitiveCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier1PrimitiveFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier1-primitives", fixtureName)
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

func TestTier1PrimitiveCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier1-primitives"))
	if err != nil {
		t.Fatalf("read tier1 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier1PrimitiveFixtures))
	for _, name := range tier1PrimitiveFixtures {
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
		if _, ok := tier1ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier1 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier1PrimitiveFixtures) + len(tier1ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier1 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier1PrimitiveFixtures), len(tier1ExcludedFixtures))
	}
}
