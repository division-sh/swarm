package cataloge2e

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type catalogExcludedFixture struct {
	kind   string
	reason string
}

var tier5LifecycleFixtures = []string{
	"test-auto-emit-on-create",
	"test-create-flow-instance",
	"test-create-flow-instance-config",
	"test-create-flow-instance-duplicate",
	"test-template-no-boot-instance",
	"test-terminal-state-preserves",
	"test-terminal-state-rejects",
	"test-timer-cancel",
	"test-timer-fire",
	"test-timer-recurring",
	"test-timer-start-on",
	"test-wildcard-subscription",
}

var tier5ExcludedFixtures = map[string]catalogExcludedFixture{}

func TestTier5LifecycleCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-auto-emit-on-create"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-create-flow-instance"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-create-flow-instance-config"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-create-flow-instance-duplicate"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-template-no-boot-instance"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-terminal-state-preserves"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-terminal-state-rejects"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-timer-cancel"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-timer-fire"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-timer-recurring"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-timer-start-on"),
		canonicalrouting.ArtifactID("tests/tier5-flow-lifecycle/test-wildcard-subscription"),
	)
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier5LifecycleFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			startRuntime := fixtureName == "test-auto-emit-on-create" ||
				fixtureName == "test-create-flow-instance" ||
				fixtureName == "test-create-flow-instance-config" ||
				fixtureName == "test-create-flow-instance-duplicate" ||
				fixtureName == "test-timer-fire" ||
				fixtureName == "test-timer-recurring"
			h := newRuntimeHarness(t, fixtureRoot, startRuntime)
			h.seedEntityFields(expected)
			if expected.Trigger.Boot || strings.TrimSpace(expected.Expected.BootResult) != "" {
				assertCatalogRuntimeOutcome(t, h, expected)
				return
			}
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			h.waitForExpectedEmittedEvents(expected, catalogRuntimePublishTimeout)
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
