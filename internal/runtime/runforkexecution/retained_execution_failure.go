package runforkexecution

import (
	"context"
	"errors"
)

type selectedForkExecutionFailure struct {
	container selectedContractForkLocalRuntimeContainer
	cause     error
}

func (p *PreparedSelectedFork) retainExecutionFailure(container selectedContractForkLocalRuntimeContainer, cause error) {
	p.failureMu.Lock()
	defer p.failureMu.Unlock()
	p.pendingExecutionFailure = &selectedForkExecutionFailure{container: container, cause: cause}
}

// Runtime work is already joined. Keep preparation possession until the exact
// failure mutation is acknowledged; only an unfinished disposition is retried.
func (p *PreparedSelectedFork) settleExecutionFailure() error {
	p.failureMu.Lock()
	defer p.failureMu.Unlock()
	pending := p.pendingExecutionFailure
	if pending == nil {
		return nil
	}
	ctx := context.WithoutCancel(p.operation.Context())
	if selectedStopOwnsDisposition(ctx) {
		// The accepted stop owns the child's terminal outcome, not this executor.
		if err := pending.container.Fail(ctx, context.Canceled); err != nil {
			return err
		}
	} else {
		acknowledged, err := pending.container.FailActivated(ctx, pending.cause)
		if !acknowledged {
			return errors.Join(errors.New("selected execution failure disposition remains unacknowledged"), err)
		}
		pending.container.diagnostics.add(err)
	}
	p.pendingExecutionFailure = nil
	return nil
}
