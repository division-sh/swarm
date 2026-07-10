package deliverylifecycle

import (
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type State string

const (
	StateQueued    State = "queued"
	StateLaunching State = "launching"
	StateActive    State = "active"
	StateRetrying  State = "retrying"
	StateDelivered State = "delivered"
	StateExhausted State = "exhausted"
)

type Transition struct {
	EventID         string
	AgentID         string
	EntityID        string
	State           State
	PreviousState   State
	Reason          string
	TerminalOutcome string
	Failure         *runtimefailures.Envelope
	RetryCount      int
}

func StateFromDelivery(status, activeSessionID string) (State, bool) {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "pending":
		return StateQueued, true
	case "in_progress":
		if strings.TrimSpace(activeSessionID) != "" {
			return StateActive, true
		}
		return StateLaunching, true
	case "failed":
		return StateRetrying, true
	case "delivered":
		return StateDelivered, true
	case "dead_letter":
		return StateExhausted, true
	default:
		return "", false
	}
}
