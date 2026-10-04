package runfork

import (
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
	SelectedForkRecoveryTerminal           SelectedForkRecoveryDisposition = "terminal_preserved"
	SelectedForkRecoveryStaged             SelectedForkRecoveryDisposition = "staged_admission_required"
	SelectedForkRecoveryControlOnly        SelectedForkRecoveryDisposition = "control_only"
	SelectedForkRecoveryCurrent            SelectedForkRecoveryDisposition = "current_process_owned"
	SelectedForkRecoveryFailed             SelectedForkRecoveryDisposition = "interrupted_failed"
	SelectedForkRecoveryResumeFiniteFeed   SelectedForkRecoveryDisposition = "resume_finite_feed"
	SelectedForkRecoveryActivateFiniteFeed SelectedForkRecoveryDisposition = "activate_finite_feed"
)

// SelectedForkFiniteFeedResume carries the permanent request-to-child binding.
// Runtime recovery must build a fresh execution generation, never reuse the
// predecessor execution ID as authority.
type SelectedForkFiniteFeedResume struct {
	Operation       ForkOperationRequest
	ForkRunStatus   string
	Pins            []durabledata.Pin
	AgentTopologies []RunForkSelectedContractAgentTopology
}

type SelectedForkRecoveryResult struct {
	RunID       string
	ExecutionID string
	Disposition SelectedForkRecoveryDisposition
	Effects     effects.RecoverySummary
	Resume      *SelectedForkFiniteFeedResume
}
