package selectedtest

import (
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/selected"
)

// ManagerDeliveryExecution composes the original selected owner with a gate after
// its genuine execution claim. It neither constructs a store nor issues authority.
func ManagerDeliveryExecution(original any, ready chan<- runforkexecution.ManagerDeliveryExecutionClaim, release <-chan struct{}) (runforkexecution.SelectedContractExecutionOwner, error) {
	owner, err := selected.ManagerDeliveryExecutionForTest(original)
	if err != nil {
		return runforkexecution.SelectedContractExecutionOwner{}, err
	}
	return runforkexecution.WithManagerDeliveryClaimGateForTest(owner, ready, release)
}
