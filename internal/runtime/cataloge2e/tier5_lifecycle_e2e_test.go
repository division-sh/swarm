package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

type catalogExcludedFixture struct {
	kind   string
	reason string
}

var tier5LifecycleFixtures = []string{
	"test-template-no-boot-instance",
	"test-wildcard-subscription",
}

var tier5ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-auto-emit-on-create":            {kind: "fixture-issue", reason: "fixture node uses produces: [] which real boot validation rejects"},
	"test-create-flow-instance":           {kind: "fixture-issue", reason: "fixture uses handler field action_params that the real loader rejects as UNDEFINED-FIELD"},
	"test-create-flow-instance-config":    {kind: "fixture-issue", reason: "fixture uses handler field action_params that the real loader rejects as UNDEFINED-FIELD"},
	"test-create-flow-instance-duplicate": {kind: "fixture-issue", reason: "fixture uses handler field action_params that the real loader rejects as UNDEFINED-FIELD"},
	"test-terminal-state-preserves":       {kind: "fixture-issue", reason: "fixture update-node uses produces: [] which real boot validation rejects"},
	"test-terminal-state-rejects":         {kind: "fixture-issue", reason: "fixture reopen-node uses produces: [] which real boot validation rejects"},
	"test-timer-cancel":                   {kind: "fixture-issue", reason: "fixture cancel-node uses produces: [] which real boot validation rejects"},
	"test-timer-fire":                     {kind: "fixture-issue", reason: "fixture uses legacy handler.timer syntax that the real loader rejects as UNDEFINED-FIELD"},
	"test-timer-recurring":                {kind: "fixture-issue", reason: "fixture uses legacy handler.timer syntax that the real loader rejects as UNDEFINED-FIELD"},
	"test-timer-start-on":                 {kind: "fixture-issue", reason: "fixture timer contract does not boot under the real loader because the timer fire event is incomplete"},
}

func TestTier5LifecycleCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier5LifecycleFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			h := newRuntimeHarness(t, fixtureRoot, false)
			h.seedEntityFields(expected)
			if expected.Trigger.Boot || strings.TrimSpace(expected.Expected.BootResult) != "" {
				assertCatalogRuntimeOutcome(t, h, expected)
				return
			}
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, 2*time.Second)
			}
			h.waitForExpectedEmittedEvents(expected, 2*time.Second)
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}

func TestTier5LifecycleCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle"))
	if err != nil {
		t.Fatalf("read tier5 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier5LifecycleFixtures))
	for _, name := range tier5LifecycleFixtures {
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
		if _, ok := tier5ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier5 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier5LifecycleFixtures) + len(tier5ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier5 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier5LifecycleFixtures), len(tier5ExcludedFixtures))
	}
}
