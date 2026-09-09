package runfork

import "github.com/division-sh/swarm/internal/runtime/effects"

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
)

type SelectedForkRecoveryResult struct {
	RunID       string
	ExecutionID string
	Disposition SelectedForkRecoveryDisposition
	Effects     effects.RecoverySummary
}
