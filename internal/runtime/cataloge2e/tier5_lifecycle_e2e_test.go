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
	"test-create-flow-instance",
	"test-template-no-boot-instance",
	"test-timer-start-on",
	"test-wildcard-subscription",
}

var tier5ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-auto-emit-on-create":            {kind: "fixture-issue", reason: "schema.yaml still uses scalar auto_emit_on_create; the real loader expects the structured {event,...} form"},
	"test-create-flow-instance-config":    {kind: "fixture-issue", reason: "config_from is still encoded in an unsupported YAML shape, so the real loader rejects the action payload"},
	"test-create-flow-instance-duplicate": {kind: "runtime-gap", reason: "real runtime still completes duplicate instance creation instead of surfacing the expected duplicate-instance error"},
	"test-terminal-state-preserves":       {kind: "fixture-issue", reason: "fixture update-node uses produces: [] which real boot validation rejects"},
	"test-terminal-state-rejects":         {kind: "fixture-issue", reason: "fixture reopen-node uses produces: [] which real boot validation rejects"},
	"test-timer-cancel":                   {kind: "fixture-issue", reason: "fixture cancel-node uses produces: [] which real boot validation rejects"},
	"test-timer-fire":                     {kind: "runtime-gap", reason: "fixture now boots, but the real runtime still does not deliver the expected one-shot timer.check event"},
	"test-timer-recurring":                {kind: "runtime-gap", reason: "fixture now boots, but the real runtime still does not deliver the expected recurring timer.tick event"},
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
