package runforkpersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// The caller holds the source lifecycle lock.
func admitSelectedForkSource(sourceRunID string, source runlifecycle.Snapshot) error {
	if source.RunID != sourceRunID {
		return fmt.Errorf("selected-contract source lifecycle identity disagrees with requested source")
	}
	if err := source.Validate(); err != nil {
		return fmt.Errorf("selected-contract source lifecycle: %w", err)
	}
	if !runForkSelectedContractBranchSourceStatusSupported(string(source.State)) {
		return fmt.Errorf("selected-contract fork source state %s is unsupported", source.State)
	}
	return nil
}
