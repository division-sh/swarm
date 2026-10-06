package store_test

import (
	"strings"
	"testing"
)

func observationBatchAuthorityViolations(findings []authorityFinding) []string {
	var violations []string
	for _, finding := range findings {
		closed := false
		switch finding.File {
		case "internal/store/storetest/conformance_storage_columns.go",
			"internal/store/storetest/tracked_entity_mutation_projection.go",
			"internal/store/storetest/notify_all_children_observation.go",
			"internal/store/storetest/notify_execution_storage.go":
			closed = true
		case "internal/runtime/conformance/persisted_surfaces_test.go":
			switch finding.Enclosing {
			case "requireCanonicalConversationSurface", "requireCanonicalRuntimeLogSurface", "requireMutationSurface", "trackedMutationStateMatchesEntityState", "newEntityToolConformanceHarness", "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites", "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForToolWrites":
				closed = true
			}
		case "internal/runtime/conformance/delivery_lifecycle_conformance_test.go":
			closed = finding.Enclosing == "requireCanonicalDeliveryLifecycleSurface"
		case "internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go":
			closed = finding.Enclosing == "newFanInBarrierRuntime" || finding.Enclosing == "newFanInBarrierRuntimeForSource"
		case "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go":
			switch finding.Enclosing {
			case "loadNotifyAllChildrenItemEvents", "assertNotifyAllChildrenMetadata", "dumpNotifyAllChildrenRuntimeState", "assertNotifyAllChildrenCompletedTurns", "countNotifyAllChildrenLifecycleTransitions", "assertNotifyAllChildrenRunPersisted", "loadNotifyAllChildrenFailure", "assertNotifyAllChildrenFlowInstanceCount", "waitNotifyAllChildrenFanOutCursor", "logNotifyAllChildrenFanOutWork":
				closed = true
			}
		}
		if closed && (finding.RawSQL || finding.Kind == "callback-type") {
			violations = append(violations, finding.key())
		}
	}
	return violations
}

func TestObservationBatchCompletedConsumersStayClosed(t *testing.T) {
	findings := loadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	if violations := observationBatchAuthorityViolations(findings); len(violations) != 0 {
		t.Fatalf("completed observations regained raw authority:\n%s", strings.Join(violations, "\n"))
	}
}

func TestObservationBatchGuardRejectsRawAndCallbackAuthority(t *testing.T) {
	for _, item := range []struct{ path, declaration string }{
		{"internal/store/storetest/notify_execution_storage.go", "ReadNotifyCompletedTurns"},
		{"internal/store/storetest/tracked_entity_mutation_projection.go", "ReadTrackedEntityMutationProjectionStorage"},
		{"internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", "assertNotifyAllChildrenCompletedTurns"},
		{"internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", "loadNotifyAllChildrenItemEvents"},
		{"internal/runtime/conformance/persisted_surfaces_test.go", "trackedMutationStateMatchesEntityState"},
		{"internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go", "newFanInBarrierRuntimeForSource"},
	} {
		for _, signature := range []string{"db *sql.DB", "query func(context.Context,*sql.Tx) error"} {
			source := "package fixture\nimport (\"context\";\"database/sql\")\nfunc " + item.declaration + "(ctx context.Context," + signature + "){}"
			if violations := observationBatchAuthorityViolations(authorityFindingsFromSource(t, item.path, source)); len(violations) == 0 {
				t.Fatalf("%s accepted raw or callback capability", item.declaration)
			}
		}
	}
}
