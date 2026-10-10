package manager_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/manager"
)

func TestCanceledDeliveryConsumerKeepsExactCommitAndCleanupEvidence(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeCanceledDeliveryConsumerKeepsExactCommitAndCleanupEvidence(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestStartupCanceledTurnCommitsBeforeAdmissionWithoutExecutingReaction(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeStartupCanceledTurnCommitsBeforeAdmissionWithoutExecutingReaction(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}
