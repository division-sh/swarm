package engine

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

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

type FailureClass uint8

const (
	FailureNone FailureClass = iota
	FailureLogic
	FailureTransient
	FailureDeadLetter
)

func (f FailureClass) String() string {
	switch f {
	case FailureLogic:
		return "logic"
	case FailureTransient:
		return "transient"
	case FailureDeadLetter:
		return "dead_letter"
	default:
		return ""
	}
}

type TimerOperation uint8

const (
	TimerUnknown TimerOperation = iota
	TimerStart
	TimerCancel
)

func (t TimerOperation) String() string {
	switch t {
	case TimerStart:
		return "start"
	case TimerCancel:
		return "cancel"
	default:
		return ""
	}
}

type GuardFailureAction uint8

const (
	GuardFailureUnknown GuardFailureAction = iota
	GuardFailureReject
	GuardFailureDiscard
	GuardFailureKill
	GuardFailureEscalate
)

func (a GuardFailureAction) String() string {
	switch a {
	case GuardFailureReject:
		return "reject"
	case GuardFailureDiscard:
		return "discard"
	case GuardFailureKill:
		return "kill"
	case GuardFailureEscalate:
		return "escalate"
	default:
		return ""
	}
}

type GuardFailure struct {
	Action    GuardFailureAction
	EventType string
}

func ParseGuardFailure(action string) (GuardFailure, error) {
	spec, err := runtimecontracts.ParseGuardFailureSpec(action)
	if err != nil {
		return GuardFailure{}, err
	}
	return GuardFailureFromSpec(spec)
}

func GuardFailureFromSpec(spec runtimecontracts.GuardFailureSpec) (GuardFailure, error) {
	switch spec.Action {
	case runtimecontracts.GuardFailureActionReject:
		return GuardFailure{Action: GuardFailureReject}, nil
	case runtimecontracts.GuardFailureActionDiscard:
		return GuardFailure{Action: GuardFailureDiscard}, nil
	case runtimecontracts.GuardFailureActionKill:
		return GuardFailure{Action: GuardFailureKill}, nil
	case runtimecontracts.GuardFailureActionEscalate:
		eventType := strings.TrimSpace(spec.Escalation.Event)
		if eventType == "" {
			return GuardFailure{}, fmt.Errorf("guard on_fail escalate requires event type")
		}
		return GuardFailure{Action: GuardFailureEscalate, EventType: eventType}, nil
	default:
		return GuardFailure{}, fmt.Errorf("unsupported guard on_fail action %q", spec.Action)
	}
}
