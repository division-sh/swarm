package runlifecycle

type StandingBindingBlockReason string

const (
	StandingBindingCredentialsAbsent StandingBindingBlockReason = "credentials_absent"
	StandingBindingRecoveryRequired  StandingBindingBlockReason = "recovery_required"
	StandingBindingSessionRequired   StandingBindingBlockReason = "session_admission_required"
)
