package deliverylifecycle

import "strings"

type State string

const (
	StateQueued    State = "queued"
	StateLaunching State = "launching"
	StateActive    State = "active"
	StateRetrying  State = "retrying"
	StateDelivered State = "delivered"
	StateExhausted State = "exhausted"
)

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
