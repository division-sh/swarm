package runfork

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/google/uuid"
)

// SelectedControl names controls that would require deferred selected execution.
// Stop has a separate terminal-disposition operation and is not in this set.
type SelectedControl string

const (
	ControlRunPause           SelectedControl = "run.pause"
	ControlRunContinue        SelectedControl = "run.continue"
	ControlAgentRestart       SelectedControl = "agent.restart"
	ControlAgentDirective     SelectedControl = "agent.send_directive"
	ControlAgentReplay        SelectedControl = "agent.replay"
	ControlEventReplay        SelectedControl = "event.replay"
	ControlEventPublish       SelectedControl = "event.publish"
	ControlMailboxDecide      SelectedControl = "mailbox.decide"
	ControlMailboxDefer       SelectedControl = "mailbox.defer"
	ControlMailboxBeginInput  SelectedControl = "mailbox.begin_input"
	ControlMailboxCancelInput SelectedControl = "mailbox.cancel_input"
)

func (c SelectedControl) Validate() error {
	switch c {
	case ControlRunPause, ControlRunContinue, ControlAgentRestart, ControlAgentDirective,
		ControlAgentReplay, ControlEventReplay, ControlEventPublish, ControlMailboxDecide,
		ControlMailboxDefer, ControlMailboxBeginInput, ControlMailboxCancelInput:
		return nil
	default:
		return fmt.Errorf("unknown selected-fork execution control %q", c)
	}
}

type SelectedForkControlUnsupported struct {
	Operation SelectedControl
	RunID     string
	BindingID string
}

func (e *SelectedForkControlUnsupported) Error() string {
	return fmt.Sprintf("%s is unsupported for selected fork %s (binding %s); selected deferred execution is not admitted", e.Operation, e.RunID, e.BindingID)
}

// RequireNormalControlForBinding consumes an exact durable classification. Its
// caller must load that classification successfully; absence is not a fallback.
func RequireNormalControlForBinding(runID string, operation SelectedControl, binding RunForkSelectedContractBinding, exists bool) error {
	if err := operation.Validate(); err != nil {
		return err
	}
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return fmt.Errorf("selected control requires exact run identity")
	}
	if !exists {
		return nil
	}
	if err := binding.ValidateExecutionBinding(runID); err != nil {
		return err
	}
	id, err = uuid.Parse(binding.BindingID)
	if err != nil || id == uuid.Nil || id.String() != binding.BindingID {
		return fmt.Errorf("selected control binding has invalid identity")
	}
	return &SelectedForkControlUnsupported{Operation: operation, RunID: runID, BindingID: binding.BindingID}
}

func (binding RunForkSelectedContractBinding) ValidateExecutionBinding(forkRunID string) error {
	if strings.TrimSpace(binding.Owner) != RunForkSelectedContractBindingOwner {
		return fmt.Errorf("selected-contract execution admission requires %s binding; got %q", RunForkSelectedContractBindingOwner, binding.Owner)
	}
	if strings.TrimSpace(binding.ForkRunID) != forkRunID {
		return fmt.Errorf("selected-contract execution admission binding fork run_id mismatch: got %q want %q", binding.ForkRunID, forkRunID)
	}
	if strings.TrimSpace(binding.SourceRunID) == "" {
		return fmt.Errorf("selected-contract execution admission binding missing source run_id")
	}
	if _, err := uuid.Parse(strings.TrimSpace(binding.SourceRunID)); err != nil {
		return fmt.Errorf("selected-contract execution admission binding source run_id must be a UUID: %w", err)
	}
	if err := binding.ForkPoint.Validate(); err != nil {
		return fmt.Errorf("selected-contract execution admission binding fork point: %w", err)
	}
	if binding.ForkEventID != binding.ForkPoint.EventID {
		return fmt.Errorf("selected-contract execution admission binding event differs from fork point")
	}
	return binding.ContractSelection.ValidateExecutionSelection("binding")
}

func (selection RunForkContractSelection) ValidateExecutionSelection(label string) error {
	switch strings.TrimSpace(selection.Mode) {
	case RunForkContractSelectionModeSelectedContracts:
		if strings.TrimSpace(selection.BundleHash) != "" {
			return fmt.Errorf("selected-contract execution admission %s selected_contracts mode cannot carry bundle_hash", label)
		}
	case RunForkContractSelectionModeBundleHash:
		if strings.TrimSpace(selection.BundleHash) == "" {
			return fmt.Errorf("selected-contract execution admission %s requires bundle_hash", label)
		}
		if err := runtimecontracts.ValidateBundleHash(selection.BundleHash); err != nil {
			return fmt.Errorf("selected-contract execution admission %s bundle_hash invalid: %w", label, err)
		}
	default:
		return fmt.Errorf("selected-contract execution admission %s requires mode selected_contracts or bundle_hash; got %q", label, selection.Mode)
	}
	return nil
}
