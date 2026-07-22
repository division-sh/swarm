package cataloge2e

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"

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

var tier11StartedRuntimeFixtures = map[string]struct{}{}

var tier11StaticMultiEntityRetiredFixtures = map[string]struct{}{
	"test-child-flow-loads":                   {},
	"test-child-flow-local-events":            {},
	"test-nested-three-levels":                {},
	"test-child-flow-pin-wiring":              {},
	"test-child-flow-policy-inherit":          {},
	"test-child-flow-tool-inherit":            {},
	"test-data-pin-wiring":                    {},
	"test-data-pin-write-conflict":            {},
	"test-gates-in-child-flow":                {},
	"test-child-flow-sibling-isolation":       {},
	"test-multi-level-policy-inherit":         {},
	"test-sibling-both-instantiated-isolated": {},
	"test-subject-id-cross-flow-inherit":      {},
	"test-subject-id-first-flow-seeds":        {},
}

func TestTier11FlowCompositionCatalogFixtures_RealRuntime(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-child-flow-absolute-path"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-child-flow-loads"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-child-flow-local-events"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-child-flow-pin-wiring"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-child-flow-policy-inherit"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-child-flow-sibling-isolation"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-child-flow-tool-inherit"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-data-pin-wiring"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-data-pin-write-conflict"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-gates-in-child-flow"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-multi-level-policy-inherit"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-nested-three-levels"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-sibling-both-instantiated-isolated"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-subject-id-cross-flow-inherit"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-subject-id-first-flow-seeds"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-tool-override"),
		canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-wildcard-deep-subscription"),
	)
	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier11FlowCompositionFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			if _, retired := tier11StaticMultiEntityRetiredFixtures[fixtureName]; retired {
				assertCatalogStaticMultiEntityRetirement(t, fixtureRoot)
				return
			}

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
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-dynamic-flow-instance"))
	repoRoot := repoRootFromCatalogE2E(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-dynamic-flow-instance")
	var expected catalogExpectedDocument
	loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

	h := newRuntimeHarness(t, fixtureRoot, true)
	h.seedEntityFields(expected)
	for _, step := range expected.triggerSequence() {
		h.publishAndWait(step, catalogRuntimePublishTimeout)
	}
	assertCatalogRuntimeOutcome(t, h, expected)
	assertDynamicFlowInstanceFlowMatchTargetedNodeDelivery(t, h, "worker/work.assign", "worker/w-001", "task-handler")
}

func assertDynamicFlowInstanceFlowMatchTargetedNodeDelivery(t testing.TB, h *runtimeHarness, eventName, flowInstance, nodeID string) {
	t.Helper()
	if h == nil || h.db == nil {
		t.Fatal("runtime harness database is required")
	}
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	nodeID = strings.TrimSpace(nodeID)
	wantEntityID := runtimeflowidentity.EntityID(flowInstance)
	var eventID, targetFlowInstance, targetEntityID, targetSet string
	err := h.db.QueryRowContext(testAuthorActivityContext(context.Background()), `
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

	var deliveryStatus, reasonCode, deliveryFlowInstance, deliveryEntityID string
	if err := h.db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT COALESCE(status, ''),
		       COALESCE(reason_code, ''),
		       COALESCE(delivery_target_route->>'flow_instance', ''),
		       COALESCE(delivery_target_route->>'entity_id', '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND COALESCE(delivery_target_route->>'flow_instance', '') = $3
		  AND COALESCE(delivery_target_route->>'entity_id', '') = $4
		ORDER BY created_at DESC, delivery_id DESC
		LIMIT 1
	`, eventID, nodeID, flowInstance, wantEntityID).Scan(&deliveryStatus, &reasonCode, &deliveryFlowInstance, &deliveryEntityID); err == sql.ErrNoRows {
		t.Fatalf("targeted event %s did not persist node delivery for %s route %s/%s; deliveries=%s", eventID, nodeID, flowInstance, wantEntityID, dumpEventDeliveries(t, h.db, eventID))
	} else if err != nil {
		t.Fatalf("query targeted event delivery: %v", err)
	}
	if deliveryStatus != "delivered" || reasonCode != "" || deliveryFlowInstance != flowInstance || deliveryEntityID != wantEntityID {
		t.Fatalf("targeted event delivery = status:%q reason:%q route:%q/%q, want canonically delivered with no failure reason for %q/%q", deliveryStatus, reasonCode, deliveryFlowInstance, deliveryEntityID, flowInstance, wantEntityID)
	}

	var deliveryCount int
	if err := h.db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND COALESCE(delivery_target_route->>'flow_instance', '') = $3
		  AND COALESCE(delivery_target_route->>'entity_id', '') = $4
	`, eventID, nodeID, flowInstance, wantEntityID).Scan(&deliveryCount); err != nil {
		t.Fatalf("query targeted event deliveries: %v", err)
	}
	if deliveryCount != 1 {
		t.Fatalf("targeted event %s node delivery count = %d, want exactly one %s semantic node delivery", eventID, deliveryCount, nodeID)
	}

	var deadLetterCount int
	if err := h.db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT COUNT(*)
		FROM dead_letters
		WHERE original_event_id = $1::uuid
		  AND failure->>'class' IN ('platform.target_unreachable', 'platform.target_ambiguous')
	`, eventID).Scan(&deadLetterCount); err != nil {
		t.Fatalf("query targeted event dead letters: %v", err)
	}
	if deadLetterCount != 0 {
		t.Fatalf("targeted event %s target_resolution_failed dead letters = %d, want none", eventID, deadLetterCount)
	}
}

func dumpEventDeliveries(t testing.TB, db *sql.DB, eventID string) string {
	t.Helper()
	rows, err := db.QueryContext(testAuthorActivityContext(context.Background()), `
		SELECT COALESCE(subscriber_type, ''),
		       COALESCE(subscriber_id, ''),
		       COALESCE(status, ''),
		       COALESCE(reason_code, ''),
		       COALESCE(delivery_target_route->>'flow_instance', ''),
		       COALESCE(delivery_target_route->>'entity_id', '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		ORDER BY created_at ASC, delivery_id ASC
	`, eventID)
	if err != nil {
		return "query_error:" + err.Error()
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var subscriberType, subscriberID, status, reason, flowInstance, entityID string
		if err := rows.Scan(&subscriberType, &subscriberID, &status, &reason, &flowInstance, &entityID); err != nil {
			return "scan_error:" + err.Error()
		}
		out = append(out, subscriberType+"/"+subscriberID+" status="+status+" reason="+reason+" route="+flowInstance+"/"+entityID)
	}
	if err := rows.Err(); err != nil {
		return "rows_error:" + err.Error()
	}
	if len(out) == 0 {
		return "<none>"
	}
	return strings.Join(out, "; ")
}
