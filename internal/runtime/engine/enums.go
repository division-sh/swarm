package engine

type OutcomeStatus uint8

const (
	OutcomeUnknown OutcomeStatus = iota
	OutcomeCompleted
	OutcomeBlocked
	OutcomeDiscarded
	OutcomeRejected
	OutcomeKilled
	OutcomeEscalated
	OutcomeWaiting
	OutcomeFannedOut
)

// IsHandledOutcome is the engine's fail-closed authority for whether an
// execution admitted its event. New statuses remain unhandled until they are
// classified here.
func IsHandledOutcome(status OutcomeStatus) bool {
	switch status {
	case OutcomeCompleted, OutcomeBlocked, OutcomeKilled, OutcomeEscalated, OutcomeWaiting, OutcomeFannedOut:
		return true
	case OutcomeUnknown, OutcomeDiscarded, OutcomeRejected:
		return false
	default:
		return false
	}
}

func (s OutcomeStatus) String() string {
	switch s {
	case OutcomeCompleted:
		return "success"
	case OutcomeBlocked:
		return "reject"
	case OutcomeDiscarded:
		return "discard"
	case OutcomeRejected:
		return "reject"
	case OutcomeKilled:
		return "kill"
	case OutcomeEscalated:
		return "escalate"
	case OutcomeWaiting:
		return "waiting"
	case OutcomeFannedOut:
		return "fanned_out"
	default:
		return ""
	}
}

type FailureDisposition uint8

const (
	FailureDispositionNone FailureDisposition = iota
	FailureDispositionTerminal
	FailureDispositionRetry
	FailureDispositionDeadLetter
)

func (f FailureDisposition) String() string {
	switch f {
	case FailureDispositionTerminal:
		return "terminal"
	case FailureDispositionRetry:
		return "retry"
	case FailureDispositionDeadLetter:
		return "dead_letter"
	default:
		return ""
	}
}
