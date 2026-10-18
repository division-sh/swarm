package sqlite

import (
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func (b *Backend) HoldWorkflowDomainForTest(original startupownership.MutationAuthority, claim deliverylifecycle.Claim) (*transactiontest.WorkflowDomainObservation, error) {
	return b.testTransactions.HoldWorkflowDomainForTest(original, claim)
}
