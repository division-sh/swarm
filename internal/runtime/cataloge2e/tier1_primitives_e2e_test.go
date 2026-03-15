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
	"test-data-accumulation-direct",
	"test-data-accumulation-mapped",
	"test-emits-multiple",
	"test-emits-payload-transform",
	"test-emits-single",
	"test-from-filter",
	"test-guard-entity-ref",
	"test-guard-multi",
	"test-guard-pass",
	"test-guard-policy-ref",
	"test-payload-transform-multi-source",
	"test-record-evidence",
	"test-rules-else",
	"test-rules-match",
	"test-rules-no-match",
	"test-sets-gate",
}

var tier1ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-data-accumulation-literal": {kind: "validation-gap", reason: "real runtime records data_accumulation bookkeeping but does not persist entity field category for this literal-write fixture"},
	"test-guard-compound-condition":  {kind: "validation-gap", reason: "real runtime leaves the entity in pending instead of advancing to done for this compound guard fixture"},
	"test-guard-discard":             {kind: "validation-gap", reason: "real runtime records handler_outcome success instead of discard for this guard on_fail fixture"},
	"test-guard-escalate":            {kind: "harness-gap", reason: "cataloge2e does not yet assert handler_outcome=escalate"},
	"test-guard-kill":                {kind: "validation-gap", reason: "real runtime records handler_outcome success instead of dead_letter for this guard on_fail kill fixture"},
	"test-guard-multi-fail":          {kind: "harness-gap", reason: "cataloge2e does not yet assert handler_outcome=reject"},
	"test-guard-reject":              {kind: "harness-gap", reason: "cataloge2e does not yet assert handler_outcome=reject"},
	"test-on-complete-first-match":   {kind: "validation-gap", reason: "real runtime leaves the entity in pending instead of advancing to passed for this on_complete fixture"},
	"test-on-complete-second-match":  {kind: "validation-gap", reason: "real runtime leaves the entity in pending instead of advancing to failed for this on_complete fixture"},
	"test-on-complete-with-state":    {kind: "validation-gap", reason: "real runtime leaves the entity in pending instead of advancing to done for this on_complete fixture"},
	"test-rules-advances-to":         {kind: "validation-gap", reason: "real runtime leaves the entity in pending instead of advancing to approved for this rules fixture"},
	"test-rules-data-accumulation":   {kind: "fixture-issue", reason: "fixture still omits required produces, so the real validator rejects it before runtime execution"},
}

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
