package runfork

import (
	"fmt"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// SelectedForkRecoveryInspection contains a prospective, read-only plan. It is
// never an acknowledged recovery result, an execution grant, or currentness proof.
type SelectedForkRecoveryInspection struct {
	RunState       runlifecycle.State
	ExecutionState string
	Plan           SelectedForkRecoveryResult
}

type SelectedForkRecoveryEntry struct {
	Binding    RunForkSelectedContractBinding
	BundleHash string
}

type SelectedForkRecoveryDisposition string

const (
	SelectedForkRecoveryTerminal    SelectedForkRecoveryDisposition = "terminal_preserved"
	SelectedForkRecoveryStaged      SelectedForkRecoveryDisposition = "staged_admission_required"
	SelectedForkRecoveryControlOnly SelectedForkRecoveryDisposition = "control_only"
	SelectedForkRecoveryCurrent     SelectedForkRecoveryDisposition = "current_process_owned"
	SelectedForkRecoveryFailed      SelectedForkRecoveryDisposition = "interrupted_failed"
	SelectedForkRecoveryResume      SelectedForkRecoveryDisposition = "resume_selected_fork"
	SelectedForkRecoveryActivate    SelectedForkRecoveryDisposition = "activate_selected_fork"
)

// SelectedForkContinuation contains current child attachment evidence, not a
// second materialization plan. Operation retains the immutable activation result
// separately from the child's current lifecycle status.
type SelectedForkContinuation struct {
	ForkRunStatus   string
	Pins            []durabledata.Pin
	AgentTopologies []RunForkSelectedContractAgentTopology
}

type SelectedForkRecoveryResult struct {
	RunID        string
	ExecutionID  string
	Disposition  SelectedForkRecoveryDisposition
	Effects      effects.RecoverySummary
	Operation    *ForkOperationRecord
	Continuation *SelectedForkContinuation
	// Pending cancellations are settlement work, never executable replay.
	PendingCancellations []effects.TurnExecutionResult
	CanceledTurns        []effects.CanceledTurnCommit
}

// ActivatedResult returns the original acknowledgment, never a projection of
// the recovered child's current status. It confers no execution authority.
func (r SelectedForkRecoveryResult) ActivatedResult() (ForkOperationResult, bool, error) {
	if r.Operation == nil {
		return ForkOperationResult{}, false, nil
	}
	if err := r.Operation.Validate(); err != nil {
		return ForkOperationResult{}, false, err
	}
	if r.Operation.ForkRunID != r.RunID {
		return ForkOperationResult{}, false, fmt.Errorf("selected recovery operation belongs to another child")
	}
	if r.Operation.Status != ForkOperationActivated {
		return ForkOperationResult{}, false, nil
	}
	result := *r.Operation.Result
	result.DataPins = append([]durabledata.Pin(nil), result.DataPins...)
	return result, true, nil
}
