package timercancellation

import (
	"errors"
	"strings"
	"time"
)

type Family string

const (
	FamilyGenericSchedule Family = "generic_schedule"
	FamilyWorkflowTimer   Family = "workflow_timer"
)

// Ref is exact post-commit evidence for retiring one process-local wakeup.
type Ref struct {
	Family       Family
	ActivationID string
	RunID        string
	TaskID       string
	DueAt        time.Time
}

func (r Ref) Canonical() Ref {
	r.ActivationID = strings.TrimSpace(r.ActivationID)
	r.RunID = strings.TrimSpace(r.RunID)
	r.TaskID = strings.TrimSpace(r.TaskID)
	if !r.DueAt.IsZero() {
		r.DueAt = r.DueAt.UTC()
	}
	return r
}

func (r Ref) Validate() error {
	r = r.Canonical()
	if r.Family != FamilyGenericSchedule && r.Family != FamilyWorkflowTimer {
		return errors.New("timer cancellation family is invalid")
	}
	if r.ActivationID == "" || r.DueAt.IsZero() {
		return errors.New("timer cancellation requires activation identity and due coordinate")
	}
	if r.Family == FamilyWorkflowTimer && r.TaskID == "" {
		return errors.New("workflow timer cancellation requires task identity")
	}
	return nil
}
