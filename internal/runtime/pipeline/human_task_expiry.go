package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type HumanTaskExpiryCommand struct {
	ObservedAt   time.Time
	Limit        int
	Publications []runtimeengine.DurablePublicationPlan
}

func (c HumanTaskExpiryCommand) Validate() error {
	if c.ObservedAt.IsZero() {
		return fmt.Errorf("human-task expiry requires an authoritative observation time")
	}
	if c.Limit <= 0 || c.Limit > 200 {
		return fmt.Errorf("human-task expiry limit must be between 1 and 200")
	}
	if len(c.Publications) > c.Limit {
		return fmt.Errorf("human-task expiry publications exceed the declared limit")
	}
	for index, publication := range c.Publications {
		if publication == nil {
			return fmt.Errorf("human-task expiry publication %d is required", index)
		}
		if err := publication.ValidateDurablePublicationPlan(); err != nil {
			return fmt.Errorf("human-task expiry publication %d: %w", index, err)
		}
	}
	return nil
}

type CommittedHumanTaskExpiry struct {
	Publications []runtimeengine.CommittedDurablePublication
}

func (r CommittedHumanTaskExpiry) Validate() error {
	for index, publication := range r.Publications {
		if publication == nil {
			return fmt.Errorf("committed human-task expiry publication %d is required", index)
		}
		if err := publication.ValidateCommittedDurablePublication(); err != nil {
			return fmt.Errorf("committed human-task expiry publication %d: %w", index, err)
		}
	}
	return nil
}

type HumanTaskExpiry interface {
	ListDueHumanTaskExpiryEvents(context.Context, time.Time, int) ([]events.Event, error)
	CommitHumanTaskExpirations(context.Context, HumanTaskExpiryCommand) (CommittedHumanTaskExpiry, error)
}
