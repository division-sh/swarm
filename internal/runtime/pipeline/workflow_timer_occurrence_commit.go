package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type WorkflowTimerOccurrenceCommitOutcome string

const (
	WorkflowTimerOccurrenceCommitted WorkflowTimerOccurrenceCommitOutcome = "committed"
	WorkflowTimerOccurrenceTerminal  WorkflowTimerOccurrenceCommitOutcome = "terminal"
)

type WorkflowTimerOccurrenceCommand struct {
	Activation  WorkflowTimerActivation
	Occurrence  timeridentity.WorkflowTimerOccurrenceRef
	FiredAt     time.Time
	Publication runtimeengine.DurablePublicationPlan
}

func (c WorkflowTimerOccurrenceCommand) Validate() error {
	c.Activation = c.Activation.normalized()
	c.Occurrence = c.Occurrence.Normalize()
	c.FiredAt = canonicalWorkflowTimerTime(c.FiredAt)
	if err := c.Activation.validate(); err != nil {
		return err
	}
	if c.Activation.Status != workflowTimerStatusActive || c.Occurrence.Activation != c.Activation.Ref ||
		!c.Occurrence.DueAt.Equal(c.Activation.FireAt) {
		return fmt.Errorf("workflow timer occurrence command requires the exact active coordinate")
	}
	if c.FiredAt.IsZero() || c.FiredAt.Before(c.Occurrence.DueAt) {
		return fmt.Errorf("workflow timer occurrence command requires fired_at at or after due_at")
	}
	if c.Publication == nil {
		return fmt.Errorf("workflow timer occurrence command requires a publication plan")
	}
	if err := c.Publication.ValidateDurablePublicationPlan(); err != nil {
		return err
	}
	wantEventID := timeridentity.WorkflowTimerOccurrenceEventID(c.Occurrence)
	if strings.TrimSpace(c.Publication.DurablePublicationEventID()) != wantEventID {
		return fmt.Errorf("workflow timer occurrence publication identity mismatch")
	}
	return nil
}

type CommittedWorkflowTimerOccurrence struct {
	Outcome     WorkflowTimerOccurrenceCommitOutcome
	Next        WorkflowTimerActivation
	Publication runtimeengine.CommittedDurablePublication
}

func (r CommittedWorkflowTimerOccurrence) Validate() error {
	switch r.Outcome {
	case WorkflowTimerOccurrenceTerminal:
		if r.Publication != nil || r.Next.Ref.Valid() {
			return fmt.Errorf("terminal workflow timer occurrence cannot carry commit evidence")
		}
		return nil
	case WorkflowTimerOccurrenceCommitted:
		if err := r.Next.validate(); err != nil {
			return err
		}
		if r.Next.Status != workflowTimerStatusFired && r.Next.Status != workflowTimerStatusActive {
			return fmt.Errorf("committed workflow timer occurrence has invalid successor status")
		}
		if r.Publication == nil {
			return fmt.Errorf("committed workflow timer occurrence requires publication evidence")
		}
		return r.Publication.ValidateCommittedDurablePublication()
	default:
		return fmt.Errorf("workflow timer occurrence has invalid outcome %q", r.Outcome)
	}
}

type WorkflowTimerOccurrenceOwner interface {
	CommitWorkflowTimerOccurrence(context.Context, WorkflowTimerOccurrenceCommand) (CommittedWorkflowTimerOccurrence, error)
}
