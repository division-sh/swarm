package cataloge2e

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

var tier11FlowCompositionFixtures = []string{
	"test-child-flow-loads",
	"test-child-flow-local-events",
	"test-nested-three-levels",
	"test-child-flow-pin-wiring",
	"test-child-flow-policy-inherit",
	"test-child-flow-tool-inherit",
	"test-data-pin-wiring",
	"test-data-pin-write-conflict",
	"test-dynamic-flow-instance",
	"test-gates-in-child-flow",
	"test-required-agents-child",
	"test-child-flow-sibling-isolation",
	"test-multi-level-policy-inherit",
	"test-sibling-both-instantiated-isolated",
	"test-subject-id-cross-flow-inherit",
	"test-subject-id-first-flow-seeds",
}

var tier11ExcludedFixtures = map[string]catalogExcludedFixture{
	"test-child-flow-absolute-path":   {reason: "parent listener/back-propagation fixture depends on legacy cross-flow subject-link semantics; authored migration belongs to #416"},
	"test-tool-override":              {reason: "parent listener/back-propagation fixture depends on legacy cross-flow subject-link semantics; authored migration belongs to #416"},
	"test-wildcard-deep-subscription": {reason: "parent wildcard back-propagation fixture depends on legacy cross-flow subject-link semantics; authored migration belongs to #416"},
}

var tier11StartedRuntimeFixtures = map[string]struct{}{
	"test-required-agents-child": {},
}

func TestTier11FlowCompositionCatalogFixtures_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier11FlowCompositionFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			if expected.Trigger.Boot || strings.TrimSpace(expected.Expected.BootResult) != "" {
				runBootCatalogFixture(t, fixtureRoot)
				return
			}

			_, startRuntime := tier11StartedRuntimeFixtures[fixtureName]
			h := newRuntimeHarness(t, fixtureRoot, startRuntime)
			h.seedEntityFields(expected)
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			assertCatalogRuntimeOutcome(t, h, expected)
		})
	}
}

func TestTier11FlowCompositionCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier11-flow-composition"))
	if err != nil {
		t.Fatalf("read tier11 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier11FlowCompositionFixtures))
	for _, name := range tier11FlowCompositionFixtures {
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
		if _, ok := tier11ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier11 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier11FlowCompositionFixtures) + len(tier11ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier11 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier11FlowCompositionFixtures), len(tier11ExcludedFixtures))
	}
}

func TestTier11DynamicFlowInstanceFlowMatchTarget_RealRuntime(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-dynamic-flow-instance")
	var expected catalogExpectedDocument
	loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

	h := newRuntimeHarness(t, fixtureRoot, false)
	h.seedEntityFields(expected)
	for _, step := range expected.triggerSequence() {
		h.publishAndWait(step, catalogRuntimePublishTimeout)
	}
	assertCatalogRuntimeOutcome(t, h, expected)
	assertDynamicFlowInstanceFlowMatchDescriptorResolution(t, h, "worker/work.assign", "worker/w-001")
}

func assertDynamicFlowInstanceFlowMatchDescriptorResolution(t testing.TB, h *runtimeHarness, eventName, flowInstance string) {
	t.Helper()
	if h == nil || h.db == nil {
		t.Fatal("runtime harness database is required")
	}
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	wantEntityID := runtimeflowidentity.EntityID(flowInstance)
	var eventID, targetFlowInstance, targetEntityID, targetSet string
	err := h.db.QueryRowContext(context.Background(), `
		SELECT event_id::text,
		       COALESCE(target_route->>'flow_instance', ''),
		       COALESCE(target_route->>'entity_id', ''),
		       COALESCE(target_set::text, '')
		FROM events
		WHERE event_name = $1
		ORDER BY created_at DESC, event_id DESC
		LIMIT 1
	`, strings.TrimSpace(eventName)).Scan(&eventID, &targetFlowInstance, &targetEntityID, &targetSet)
	if err == sql.ErrNoRows {
		t.Fatalf("targeted event %q not persisted", strings.TrimSpace(eventName))
	}
	if err != nil {
		t.Fatalf("query targeted event %q: %v", strings.TrimSpace(eventName), err)
	}
	if targetFlowInstance != flowInstance || targetEntityID != wantEntityID {
		t.Fatalf("targeted event route = flow_instance:%q entity_id:%q target_set:%s, want flow_instance:%q entity_id:%q", targetFlowInstance, targetEntityID, targetSet, flowInstance, wantEntityID)
	}

	var deliveryCount int
	if err := h.db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND COALESCE(delivery_target_route->>'flow_instance', '') = $2
		  AND COALESCE(delivery_target_route->>'entity_id', '') = $3
	`, eventID, flowInstance, wantEntityID).Scan(&deliveryCount); err != nil {
		t.Fatalf("query targeted event deliveries: %v", err)
	}
	if deliveryCount != 0 {
		t.Fatalf("targeted event %s node delivery count = %d, want 0 while #1410 owns targeted internal-node delivery row materialization", eventID, deliveryCount)
	}

	var failureReason, failureFlowInstance, failureEntityID string
	err = h.db.QueryRowContext(context.Background(), `
		SELECT COALESCE(target_failure_reason, ''),
		       COALESCE(target_context->'target'->>'flow_instance', ''),
		       COALESCE(target_context->'target'->>'entity_id', '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
		  AND failure_type = 'target_resolution_failed'
		ORDER BY created_at DESC
		LIMIT 1
	`, eventID).Scan(&failureReason, &failureFlowInstance, &failureEntityID)
	if err == sql.ErrNoRows {
		t.Fatalf("targeted event %s did not record #1410 split delivery-planning dead letter", eventID)
	}
	if err != nil {
		t.Fatalf("query targeted event dead letter: %v", err)
	}
	if failureReason != "target_not_subscribed" || failureFlowInstance != flowInstance || failureEntityID != wantEntityID {
		t.Fatalf("targeted event dead letter reason=%q route=%q/%q, want target_not_subscribed for %q/%q", failureReason, failureFlowInstance, failureEntityID, flowInstance, wantEntityID)
	}
}
