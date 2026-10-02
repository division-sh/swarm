package genericschedule

import (
	"fmt"
	"time"
)

// ClockReadback projects admitted lifecycle evidence, never source declarations
// or a newly computed recurrence.
type ClockReadback struct {
	ActivationID string        `json:"activation_id"`
	Name         string        `json:"name"`
	RunID        string        `json:"run_id"`
	FlowID       string        `json:"flow_id"`
	FlowInstance string        `json:"flow_instance"`
	Emit         string        `json:"emit"`
	Cron         string        `json:"cron,omitempty"`
	Every        string        `json:"every,omitempty"`
	Status       Status        `json:"status"`
	InitialDueAt time.Time     `json:"initial_due_at"`
	NextDueAt    *time.Time    `json:"next_due_at,omitempty"`
	RetainsRun   bool          `json:"retains_run"`
	CancelCause  string        `json:"cancel_cause,omitempty"`
	CancelledAt  *time.Time    `json:"cancelled_at,omitempty"`
	Failure      *ClockFailure `json:"failure,omitempty"`
}

type ClockFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func ProjectClockReadback(activation Activation, runActive bool) (ClockReadback, error) {
	activation = activation.Canonical()
	if err := activation.Validate(); err != nil {
		return ClockReadback{}, err
	}
	if activation.Command.OwnerKind != OwnerInstance {
		return ClockReadback{}, fmt.Errorf("clock readback requires an instance-owned schedule")
	}
	command := activation.Command
	view := ClockReadback{
		ActivationID: activation.ID, Name: command.ScheduleKey, RunID: command.RunID,
		FlowID: command.OwnerID, FlowInstance: command.FlowInstance, Emit: command.EventType,
		Cron: command.Due.Cron, Status: activation.Status, InitialDueAt: activation.InitialDueAt,
		CancelCause: activation.CancelCause,
	}
	if command.Due.Kind == DueEvery {
		view.Every = command.Due.Every.String()
	}
	if activation.Status == StatusActive {
		due := activation.CurrentDueAt
		view.NextDueAt = &due
		view.RetainsRun = runActive
	}
	if !activation.CancelledAt.IsZero() {
		cancelled := activation.CancelledAt
		view.CancelledAt = &cancelled
	}
	if activation.Status == StatusFailed {
		failure := ClockFailure{Code: activation.Failure.Code, Message: activation.Failure.Message}
		view.Failure = &failure
	}
	return view, nil
}
