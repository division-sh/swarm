package engine

import (
	"fmt"
	"strings"
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
		return "completed"
	case OutcomeBlocked:
		return "blocked"
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
	GuardFailureBlocked
	GuardFailureDiscard
	GuardFailureKill
	GuardFailureEscalate
)

func (a GuardFailureAction) String() string {
	switch a {
	case GuardFailureReject:
		return "reject"
	case GuardFailureBlocked:
		return "blocked"
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

func parseGuardFailure(action string) (GuardFailure, error) {
	normalized := strings.TrimSpace(strings.ToLower(action))
	switch normalized {
	case "", "reject":
		return GuardFailure{Action: GuardFailureReject}, nil
	case "block", "blocked":
		return GuardFailure{Action: GuardFailureBlocked}, nil
	case "discard":
		return GuardFailure{Action: GuardFailureDiscard}, nil
	case "kill":
		return GuardFailure{Action: GuardFailureKill}, nil
	}
	if strings.HasPrefix(normalized, "escalate:") {
		eventType := strings.TrimSpace(strings.TrimPrefix(normalized, "escalate:"))
		if eventType == "" {
			return GuardFailure{}, fmt.Errorf("guard on_fail escalate requires event type")
		}
		return GuardFailure{Action: GuardFailureEscalate, EventType: eventType}, nil
	}
	return GuardFailure{}, fmt.Errorf("unsupported guard on_fail action %q", action)
}
