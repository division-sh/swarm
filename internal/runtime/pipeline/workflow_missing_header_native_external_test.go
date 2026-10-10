package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestDeliveryTargetApplicationRejectsStateOnlyChildRelabeledAsParentOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyDeliveryTargetApplicationRejectsStateOnlyChildRelabeledAsParentOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestDeliveryTargetApplicationRejectsWrongRunRootTargetsBeforeMutationOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyDeliveryTargetApplicationRejectsWrongRunRootTargetsBeforeMutationOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestDeliveryTargetApplicationRejectsInvalidPersistencePresenceAndLifecycleWithoutMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyDeliveryTargetApplicationRejectsInvalidPersistencePresenceAndLifecycleWithoutMutationForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestNonActiveDeliveryTargetRejectsDelayedAndReplayedExecutionBeforeMutationOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNonActiveDeliveryTargetRejectsDelayedAndReplayedExecutionBeforeMutationOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}
