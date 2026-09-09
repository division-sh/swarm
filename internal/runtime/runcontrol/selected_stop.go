package runcontrol

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// SelectedStopRequest carries the exact durable binding and current process
// possession. It grants no execution or replay permission.
type SelectedStopRequest struct {
	Transition TransitionRequest
	Binding    runfork.RunForkSelectedContractBinding
	Process    startupownership.Authority
}

func (r SelectedStopRequest) Validate() error {
	for _, id := range []string{r.Transition.RunID, r.Binding.BindingID, r.Binding.ForkRunID, r.Binding.SourceRunID, r.Binding.ForkEventID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return fmt.Errorf("selected stop requires canonical nonzero identities")
		}
	}
	if r.Transition.RunID != r.Binding.ForkRunID || r.Binding.ForkRunID == r.Binding.SourceRunID ||
		r.Binding.Owner != runfork.RunForkSelectedContractBindingOwner || r.Binding.CreatedAt.IsZero() {
		return fmt.Errorf("selected stop requires its exact fork binding")
	}
	switch r.Binding.ContractSelection.Mode {
	case runfork.RunForkContractSelectionModeSelectedContracts:
		if r.Binding.ContractSelection.BundleHash != "" {
			return fmt.Errorf("selected-contract selection cannot carry a replacement hash")
		}
	case runfork.RunForkContractSelectionModeBundleHash:
		if err := bundleidentity.ValidateCanonicalHash(r.Binding.ContractSelection.BundleHash); err != nil {
			return err
		}
	default:
		return fmt.Errorf("selected stop requires a canonical selection")
	}
	if err := r.Process.Validate(); err != nil {
		return err
	}
	if r.Process.State != startupownership.StateActive {
		return fmt.Errorf("selected stop requires active process possession")
	}
	return nil
}
