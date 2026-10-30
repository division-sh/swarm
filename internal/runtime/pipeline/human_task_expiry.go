package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
	Acknowledged bool
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

func humanTaskExpiryFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	if typed, ok := runtimefailures.As(err); ok && (typed.Failure.Class != runtimefailures.ClassInternalFailure || typed.Failure.Detail.Code != "unclassified_runtime_error") {
		return err
	}
	var types, states []string
	truncated := false
	var walk func(error, int)
	walk = func(cause error, depth int) {
		if cause == nil {
			return
		}
		if len(types) == 32 || depth > 8 {
			truncated = true
			return
		}
		types = append(types, fmt.Sprintf("%T", cause))
		if sqlCause, ok := cause.(interface{ SQLState() string }); ok {
			state := sqlCause.SQLState()
			if len(state) == 5 && strings.IndexFunc(state, func(char rune) bool {
				return !(char >= '0' && char <= '9' || char >= 'A' && char <= 'Z')
			}) == -1 {
				states = append(states, state)
			}
		}
		if joined, ok := cause.(interface{ Unwrap() []error }); ok {
			for _, branch := range joined.Unwrap() {
				walk(branch, depth+1)
				if truncated {
					break
				}
			}
		} else {
			walk(errors.Unwrap(cause), depth+1)
		}
	}
	walk(err, 0)
	code := "human_task_expiry_failed"
	if stage == "plan_count" {
		code = "human_task_expiry_plan_count_mismatch"
	}
	return runtimefailures.Wrap(runtimefailures.ClassInternalFailure, code, runtimeWorkflowID, "expire_human_task_cards",
		map[string]any{"stage": stage, "cause_types": types, "sqlstates": states, "truncated": truncated}, err)
}
