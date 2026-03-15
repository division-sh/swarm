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
	"test-cross-entity-concurrent",
	"test-event-persisted-before-delivery",
	"test-guards-pre-handler-state",
}

var tier6ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-atomicity-commit":         {kind: "fixture-issue", reason: "fixture still uses sets_gates, which the real loader rejects; it must use the live sets_gate dialect"},
	"test-atomicity-guard-rollback": {kind: "fixture-issue", reason: "fixture still uses sets_gates, which the real loader rejects; it must use the live sets_gate dialect"},
	"test-atomicity-rollback":       {kind: "fixture-issue", reason: "fixture uses unsupported sets_gates and simulate_failure handler fields, so it does not boot under the real loader"},
	"test-chain-depth-limit":        {kind: "fixture-issue", reason: "fixture self-emits chain.continue from the chain.continue handler, so real boot validation rejects it before any chain-depth runtime behavior"},
	"test-dead-letter":              {kind: "fixture-issue", reason: "live runtime treats unroutable contract events as spec.contradiction_detected diagnostics, not event-loop dead_letter receipts"},
	"test-entity-serialization":     {kind: "fixture-issue", reason: "fixture guard uses stale entity.state dialect; live runtime expression context exposes entity.current_state"},
	"test-event-validation":         {kind: "fixture-issue", reason: "default runtime payload validation is warning-only unless strict mode is enabled, so this fixture's reject-plus-dead-letter expectation does not match live runtime mode"},
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
